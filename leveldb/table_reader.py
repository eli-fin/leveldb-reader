'''
Module for parsing all entries in a table (.ldb) file
see partial format description here:    https://github.com/google/leveldb/blob/master/doc/table_format.md
see code implementation here:           https://github.com/google/leveldb/tree/main/table
'''

import io


import snappy


from . import crc32c
from . import util


class Table:
    '''
    A table file with all entries
    Initialize from file path
    '''
    
    MAGIC = 0xDB4775248B80fB57
    
    def __init__(self, path):        
        with open(path, 'rb') as f:
            # read magic from end
            f.seek(-8, 2)
            magic = int.from_bytes(util.read_safe(f, 8), 'little')
            assert magic == Table.MAGIC, 'Not a valid table file'
            
            # read footer from end
            f.seek(-40-8, 2)
            
            index_meta_blocks = io.BytesIO(util.read_safe(f, 40))
            self.__metaindex_handle = BlockHandle(index_meta_blocks)
            self.__index_handle = BlockHandle(index_meta_blocks)
            
            self.__index_block = Block(self.__index_handle, f, False)
            self.__meta_block = Block(self.__metaindex_handle, f, True)
            
            # the index block has a key,val entry for each data block
            self.__data_blocks = []
            for key, val in self.__index_block.entries.items():
                # ignore key, not sure what it's used for
                data_block_handle = BlockHandle(io.BytesIO(val))
                data_block = Block(data_block_handle, f, False)
                self.__data_blocks.append(data_block)
            
            # extract all entries and add to list
            self.entries = {}
            self.meta_entries = {}
            self.deleted_entries = {}
            for db in self.__data_blocks:
                self.entries.update(db.entries)
                self.meta_entries.update(db.meta_entries)
                self.deleted_entries.update(db.deleted_entries)


class BlockHandle:
    '''
    A handle to a data block within a table file
    Initialize from stream
    '''
    
    def __init__(self, stream):
        self.offset = util.read_varint64(stream)
        self.size = util.read_varint64(stream)
    
    def __str__(self):
        return f'BlockHandle(offset={self.offset}, size={self.size})'
        
    __repr__ = __str__


class Block:
    '''
    A data block from a table file
    Initialize entries from stream and handle
    '''
    
    kBlockTrailerSize   = 1+4  # block type and crc32
    kNoCompression      = 0x0
    kSnappyCompression  = 0x1
    
    kTypeDeletion       = 0x0
    kTypeValue          = 0x1
    
    def __init__(self, handle, stream, is_meta):
        # read block
        stream.seek(handle.offset)
        buf = util.read_safe(stream, handle.size + Block.kBlockTrailerSize)
        
        data, block_type, masked_crc_bytes = buf[:handle.size], buf[handle.size], buf[handle.size+1:]
        
        # validate crc
        masked_crc = int.from_bytes(masked_crc_bytes, 'little')
        crc = util.unmask_leveldb_crc32c(masked_crc)
        calculated_crc = crc32c.crc(data)
        calculated_crc = crc32c.crc_update(calculated_crc, bytes([block_type]))
        assert (crc == calculated_crc), 'block checksum mismatch'
        
        # get block data
        if block_type == Block.kNoCompression:
            data = data
        elif block_type == Block.kSnappyCompression:
            data = snappy.uncompress(data)
        else:
            assert False, 'invalid block type'
        
        # calculate restart points and validate
        max_restarts = (len(data) - 4) // 4;
        restarts = int.from_bytes(data[-4:], 'little')
        assert restarts <= max_restarts, 'invalid restarts'
        restart_offset = len(data) - (1 + restarts) * 4
        
        # read all entries
        data_stream = io.BytesIO(data)
        self.entries = {}
        self.meta_entries =  {}
        self.deleted_entries = {}
        prev_key = None
        while data_stream.tell() < restart_offset:  # the offset is the end of entries
            shared = util.read_varint32(data_stream)
            non_shared = util.read_varint32(data_stream)
            val_len = util.read_varint32(data_stream)
            
            shared_key_data = b''
            if shared > 0:
                # if we have shared bytes, get them from the last key
                shared_key_data = prev_key[:shared]
            
            key = shared_key_data + data_stream.read(non_shared)
            val = data_stream.read(val_len)
            
            # last 8 bytes (int64) is some internal key, remove and ignore for now
            assert (len(key) > 8), 'key too short'
            internal_key = int.from_bytes(key[-8:], 'little')
            user_key = key[:-8]
            
            record_type = internal_key & 0xFF
            sequence = internal_key >> 8
            
            if is_meta:
                self.meta_entries[user_key] = val
            else:
                if record_type == Block.kTypeValue:
                    self.entries[user_key] = val
                elif record_type == Block.kTypeDeletion:
                    self.deleted_entries[user_key] = val
                else:
                    assert False, f'unknown block entry type {record_type}'
            
            prev_key = key
