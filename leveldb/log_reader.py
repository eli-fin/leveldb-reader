'''
Module for parsing all entries in a log (.log) file
see partial format description here:            https://github.com/google/leveldb/blob/master/doc/log_format.md
see code implementation in *.log files here:    https://github.com/google/leveldb/tree/main/db
'''

import collections
import io


from . import crc32c
from . import util


RECORD_TYPE_FULL    = 0x1
RECORD_TYPE_FIRST   = 0x2
RECORD_TYPE_MIDDLE  = 0x3
RECORD_TYPE_LAST    = 0x4


def __get_record(block_stream):
    ''' Get a record from a block stream, return (data, record_type) '''
    
    # read
    expectec_checksum_masked = int.from_bytes(util.read_safe(block_stream, 4), 'little')
    expectec_checksum = util.unmask_leveldb_crc32c(expectec_checksum_masked)
    length = int.from_bytes(util.read_safe(block_stream, 2), 'little')
    record_type = util.read_safe(block_stream, 1)[0]
    data = util.read_safe(block_stream, length)
    
    # validate crc
    calculated_crc = crc32c.crc(bytes([record_type]))
    calculated_crc = crc32c.crc_update(calculated_crc, data)
    assert expectec_checksum == calculated_crc, 'block checksum mismatch'
    
    # return data and type
    return data, record_type
    

def __get_record_entries(record):
    ''' Get all entries from record stream (a dict for new entries, and a set of deleted keys) '''
    
    kTypeDeletion   = 0x0
    kTypeValue      = 0x1
    
    sequence = int.from_bytes(util.read_safe(record, 8), 'little')
    count = int.from_bytes(util.read_safe(record, 4), 'little')
    
    entries = {}
    deleted_keys = set()
    while util.bytes_left(record):
        record_type = util.read_safe(record, 1)[0]
        if record_type == kTypeValue:
            key_len = util.read_varint32(record)
            key = util.read_safe(record, key_len)
            val_len = util.read_varint32(record)
            val = util.read_safe(record, val_len)
            entries[key] = val
        elif record_type == kTypeDeletion:
            key_len = util.read_varint32(record)
            key = util.read_safe(record, key_len)
            deleted_keys.add(key)
        else:
            assert False, 'unknown record entry type'
    
    assert len(entries)+len(deleted_keys) == count, 'incorrect record entry count'
    return entries, deleted_keys


def __records_left_in_block(stream):
    ''' Block should have more than 6 bytes if it has more records '''
    return util.bytes_left(stream) > 6


def __get_next_block(stream):
    ''' Get block from stream (as a BytesIO stream), if remaining '''
    buf = stream.read(1024*32) # 32kb
    if buf:
        return io.BytesIO(buf)
    else:
        return None


def get_log_records(path):
    '''
    Get all records for given log file path
    This returns a full record (even if it's fragmented over a few blocks)
    '''
    
    records = []
    
    with open(path, 'rb') as log:
        # iterate over blocks
        while True:
            block = __get_next_block(log)
            if not block:
                break
            
            # iterate over block records
            while __records_left_in_block(block):
                full_record, record_type = __get_record(block)
                
                # if this isn't full, read until last
                if record_type != RECORD_TYPE_FULL:
                    assert record_type == RECORD_TYPE_FIRST, "if record isn't full, first is expected here"
                    
                    while True:
                        # advance to next block, if necessary
                        if not __records_left_in_block(block):
                            block = __get_next_block(log)
                            assert block is not None, 'expecting another block'
                        
                        record, record_type = __get_record(block)
                        full_record += record
                        if record_type == RECORD_TYPE_MIDDLE:
                            continue
                        elif record_type == RECORD_TYPE_LAST:
                            break
                        else:
                            assert False, 'unknown record type'
                
                records.append(full_record)
    
    return records


def get_log_entries(path):
    ''' Get all entries for given log path (a dict for new entries, and a set of deleted keys) '''
    
    entries = {}
    deleted_keys = set()
    
    records = get_log_records(path)
    for record in records:
        record_entries, record_deleted_keys = __get_record_entries(io.BytesIO(record))
        entries.update(record_entries)
        deleted_keys.update(record_deleted_keys)
    
    return entries, deleted_keys


class Manifest:
    '''
    The DB manifest file with all info about db files, deleted files, etc.
    Initialize from file path
    '''
    
    NewFile = collections.namedtuple('new_file', ('level', 'number', 'size', 'smallest', 'largest'))
    DeletedFile = collections.namedtuple('deleted_file', ('level', 'number'))
    
    kComparator     = 1
    kLogNumber      = 2
    kNextFileNumber = 3
    kLastSequence   = 4
    kCompactPointer = 5
    kDeletedFile    = 6
    kNewFile        = 7
    # 8 was used for large value refs
    kPrevLogNumber  = 9
    
    def __init__(self, path):
        ''' Read the manifest file '''
        
        self.comparator_name = '<none>'
        self.log_number = -1
        self.next_file_number = -1
        self.last_sequence = -1
        self.compact_pointers = []
        self.deleted_files = []
        self.new_files = []
        self.prev_log_number = -1
        self.existing_files = []
        
        # manifest has the same records as log files
        records = get_log_records(path)
        for record in records:
            # see VersionEdit::DecodeFrom
            record_stream = io.BytesIO(record)
            while util.bytes_left(record_stream):
                tag = util.read_safe(record_stream, 1)[0]
                
                if tag == Manifest.kComparator:
                    cmp_len = util.read_varint32(record_stream)
                    self.comparator_name = util.read_safe(record_stream, cmp_len)
                elif tag == Manifest.kLogNumber:
                    self.log_number = util.read_varint64(record_stream)
                elif tag == Manifest.kNextFileNumber:
                    self.next_file_number = util.read_varint64(record_stream)
                elif tag == Manifest.kLastSequence:
                    self.last_sequence = util.read_varint64(record_stream)
                elif tag == Manifest.kCompactPointer:
                    level = util.read_varint32(record_stream)
                    key = self.__get_internal_key_data(record_stream)
                    self.compact_pointers.append((level, key))
                elif tag == Manifest.kDeletedFile:
                    level = util.read_varint32(record_stream)
                    f_number = util.read_varint64(record_stream)
                    self.deleted_files.append(Manifest.DeletedFile(level, f_number))
                elif tag == Manifest.kNewFile:
                    level = util.read_varint32(record_stream)
                    f_number = util.read_varint64(record_stream)
                    f_size = util.read_varint64(record_stream)
                    smallest = self.__get_internal_key_data(record_stream)
                    largest = self.__get_internal_key_data(record_stream)
                    self.new_files.append(Manifest.NewFile(level, f_number, f_size, smallest, largest))
                elif tag == Manifest.kPrevLogNumber:
                    self.prev_log_number = util.read_varint64(record_stream)
                else:
                    assert False, f'unknown tag ({tag})'
        
        self.existing_files = set(f[1] for f in self.new_files) - set(f[1] for f in self.deleted_files)
    
    def __get_internal_key_data(self, stream):
        ''' Helper function to read key data from manifest '''
        key_ley = util.read_varint32(stream)
        key_data = util.read_safe(stream, key_ley)
        user_key = key_data[:-8] # ignore internal key part
        return user_key
