'''
Module for parsing all entries in a Chrome IndexedDB database.
Chrome IndexedDB for each origin is backed by a single folder which contains a LevelDB database
and a blob folder for large values.

An IndexedDB has multiple DBs, each of them containing multiple Object Stores with key-value pairs
(and other data, such and indexes), all backed by a single LevelDB .

There's 1 IndexedDB per origin.

The LevelDB has an identifier, see: storage/common/database/database_identifier.cc.
Basically, it's the escaped site origin (filename forbidden chars are replaced by '_',
if the port is the default for the protocol (i.e. http port 80), its replaced by 0, followed by '@1'.
e.g. https_drive.google.com_0@1
(this is also the folder name where IndexedDB stores the LevelDB, without the '.indexeddb.leveldb' suffix)

To dump all object stores to log files:
import os, leveldb.db, indexeddb
DB_PATH = os.path.expandvars(r'%AppData%\Microsoft\Teams\IndexedDB\https_teams.microsoft.com_0.indexeddb.leveldb')
ldb = leveldb.db.DB(DB_PATH, False)
idb = indexeddb.IDB(ldb.entries, ldb.deleted_entries)
for db in idb.get_db_names():
 for store in db.stores:
  try:
   with open(f'{db.id}-{store.id}.log', 'w', encoding='utf8') as f:
    _=print(f'DB: {db.name}, store: {store.name}', file=f)
    for e in idb.get_obj_store_entries(db.id, store.id)[0]:
     _=print(e, file=f)
  except Exception as e:
   print('error: ' + store.name, e)
'''

import collections
import io
import struct


from leveldb import util


class IDBKey:
    '''
    Parse an indexeddb key from a leveldb key
    see key format here: https://github.com/chromium/chromium/blob/main/content/browser/indexed_db/docs/leveldb_coding_scheme.md
    '''
    
    def __init__(self, ldb_key):
        key_stream = io.BytesIO(ldb_key)
        
        # get length (in bytes) of each ID, encoded in the first byte
        first_byte = util.read_safe(key_stream, 1)[0]
        db_id_len           = (first_byte & 0b11100000 >> 5)+1
        obj_store_id_len    = (first_byte & 0b00011100 >> 2)+1
        index_id_len        = (first_byte & 0b00000011 >> 0)+1
        
        # get the actual IDs
        self.db_id = int.from_bytes(util.read_safe(key_stream, db_id_len), 'little')
        self.obj_store_id = int.from_bytes(util.read_safe(key_stream, obj_store_id_len), 'little')
        self.index_id = int.from_bytes(util.read_safe(key_stream, index_id_len), 'little')
        
        self.key = self.__parse_key(key_stream)
        assert util.bytes_left(key_stream) == 0, 'unexpected trailing data'
    
    @staticmethod
    def __parse_key(stream):
        ''' Parse the actual key data '''
        tag = util.read_safe(stream, 1)[0]
        
        if tag == 0: # Null
            assert False, 'unexpected null key'
        elif tag == 3: # Number - Double
            buf = util.read_safe(stream, 8)
            ret = struct.unpack('d', buf)[0]
        elif tag == 2: # Date - Double
            assert False, 'date type not implemented'
        elif tag == 1: # String - StringWithLength
            s_len = util.read_varint64(stream)
            s = util.read_safe(stream, s_len*2)
            ret = s.decode('utf-16be')
        elif tag == 6: # Binary
            bin_len = util.read_varint64(stream)
            ret = util.read_safe(stream, bin_len)
        elif tag == 4: # Array
            ret = []
            arr_len = util.read_varint64(stream)
            for _ in range(arr_len):
                ret.append(IDBKey.__parse_key(stream))
        else:
            assert False, f'unknown primitive type "{tag}"'
        
        return ret
    
    @staticmethod
    def encode_key_prefix(db_id, obj_store_id, idx_id):
        '''
        Encode length of IDs and IDs themselves in a buffer, which will serve as a
        key prefix for entries of this db/store/index
        '''
        assert db_id >= 0 and obj_store_id >= 0 and idx_id >= 0, 'invalid IDs'
        
        db_id_bytes = util.int64_to_bytes_compact(db_id)
        obj_store_id_bytes = util.int64_to_bytes_compact(obj_store_id)
        idx_id_bytes = util.int64_to_bytes_compact(idx_id)
        
        first_byte = 0
        first_byte |= (len(db_id_bytes          )-1) << 5
        first_byte |= (len(obj_store_id_bytes   )-1) << 2
        first_byte |= (len(idx_id_bytes         )-1) << 0
        return bytes([first_byte])+db_id_bytes+obj_store_id_bytes+idx_id_bytes


class IDBValue:
    '''
    Parse an indexeddb value from a leveldb value
    The value is a v8 serialized object
    For type structure, see (in the chromium repo):
        v8/src/objects/value-serializer.cc
        third_party/blink/renderer/bindings/core/v8/serialization/serialization_tag.h
    '''
    
    EndMarker = collections.namedtuple('EndMarker', 'count')
    
    def __init__(self, value):
        val_stream = io.BytesIO(value)
        db_version = util.read_varint64(val_stream) # guess we can validate this, but skipping it
        
        # check if this is a valid value
        # see third_party/blink/renderer/modules/indexeddb/idb_value_wrapping.cc
        header = val_stream.read(2)
        if len(header) != 2 or header[0] != 0xFF or header[1] < 0x11:
            assert False, "doesn't look like an IDB serialized value"
        tag = util.read_safe(val_stream, 1)[0]
        if tag == 0x01:
            # this is a reference to a blob object, stored in another folder. not implemented.
            self.value = '<blob>'
        else:
            assert tag == 0xFF, 'invalid tag'
            
            # deserialize
            v8_serialization_version = util.read_safe(val_stream, 1)[0] # doesn't look like we need to verify this
            self.value = IDBValue.v8_deserizlize(val_stream)
            assert util.bytes_left(val_stream) == False, 'unexpected trailing data'
    
    @staticmethod
    def v8_deserizlize(stream):
        ''' Read type tag from stream, then read object, recursively '''
        
        tag = util.read_safe(stream, 1)
        if tag == b'\x00': # padding, move to next
            return IDBValue.v8_deserizlize(stream)
        elif tag == b'o': # object start
            obj = {}
            while True:
                key = IDBValue.v8_deserizlize(stream)
                if type(key) == IDBValue.EndMarker:
                    assert(len(obj) == key.count), f'obj has {len(obj)} properties, but expecting {key.count}'
                    return obj
                val = IDBValue.v8_deserizlize(stream)
                obj[key] = val
        elif tag in (b'{', b'@', b'$'): # object end, sparse array end or dense array end
            count = util.read_varint64(stream)
            return IDBValue.EndMarker(count)
        elif tag == b'"': # length prefixed one-byte string
            str_len = util.read_varint32(stream)
            str_val = util.read_safe(stream, str_len)
            # not sure why, but I've seen some strings in chrome DBs which weren't in ascii range,
            # so 'replace' just in case
            str_val = str_val.decode('ascii', 'replace')
            return str_val
        elif tag == b'c': # length prefixed two-byte string
            str_len = util.read_varint64(stream)
            str_val = util.read_safe(stream, str_len)
            str_val = str_val.decode('utf-16le')
            return str_val
        elif tag == b'I': # sint32, zig zag encoded
            n = util.read_sint32(stream)
            return n
        elif tag == b'N': # double
            buf = util.read_safe(stream, 8)
            doub = struct.unpack('d', buf)[0]
            return doub
        elif tag == b'a': # sparse array
            arr = []
            arr_len1 = util.read_varint32(stream)
            while True:
                key = IDBValue.v8_deserizlize(stream)
                if type(key) == IDBValue.EndMarker:
                    assert(len(arr) == key.count), f'arr has {len(arr)} properties, but expecting {key.count}'
                    arr_len2 = util.read_varint32(stream)
                    assert(arr_len1 == arr_len2), 'array length mismatch'
                    return arr
                val = IDBValue.v8_deserizlize(stream)
                arr.append((key, val))
        elif tag == b'A': # dense array
            arr = []
            arr_len1 = util.read_varint32(stream)
            for _ in range(arr_len1):
                val = IDBValue.v8_deserizlize(stream)
                arr.append(val)
            property_count = 0
            while True:
                key = IDBValue.v8_deserizlize(stream)
                if type(key) == IDBValue.EndMarker:
                    assert(property_count == key.count), f'arr has {len(arr)} properties, but expecting {key.count}'
                    arr_len2 = util.read_varint32(stream)
                    assert(arr_len1 == arr_len2), 'array length mismatch'
                    return arr
                val = IDBValue.v8_deserizlize(stream)
                arr.append((key, val))
                property_count += 1
        elif tag in (b'_', b'0'): # undefined or null
            return None
        elif tag == b'T':
            return True
        elif tag == b'F':
            return False
        assert False, f'unknown tag: {tag}'


class IDB:
    ''' An IndexedDB instance, with all ObjectStores and their data (key-value pairs) '''
    
    DbName = collections.namedtuple('DbName', ('origin', 'name', 'id', 'stores'))
    DbStore = collections.namedtuple('DbStore', ('name', 'id'))
    Entry = collections.namedtuple('Entry', ('key', 'value'))
    
    def __init__(self, ldb_entries, ldb_deleted_entries):
        self.entries = ldb_entries
        self.deleted_entries = ldb_deleted_entries
    
    def get_db_names(self):
        ''' Get names of all indexeddb db names in the leveldb '''
        db_name_key_prefix = b'\x00\x00\x00\x00\xC9' # db name entries have this prefix
        db_name_keys = [e for e in self.entries.items() if e[0].startswith(db_name_key_prefix)]
        
        ret = []
        for k, v in db_name_keys:
            origin, db_name = self.__read_db_name_key(k[len(db_name_key_prefix):]) # read, after removing prefix
            db_id = util.read_varint64(io.BytesIO(v))
            stores = self.get_obj_stores(db_id)
            ret.append(IDB.DbName(origin, db_name, db_id, stores))
        return ret
    
    @staticmethod
    def __read_db_name_key(name_key):
        stream = io.BytesIO(name_key)
        
        origin_len = util.read_varint64(stream)
        origin = util.read_safe(stream, origin_len*2).decode('utf-16be')
        db_name_len = util.read_varint64(stream)
        db_name = util.read_safe(stream, db_name_len*2).decode('utf-16be')
        
        assert util.bytes_left(stream) == False, 'unexpected trailing data'
        return origin, db_name
    
    def get_obj_stores(self, db_id):
        ''' Get all object stores of db '''
        store_info_type = bytes([50])
        key_end = bytes([0])
        key_prefix = IDBKey.encode_key_prefix(db_id, 0, 0) + store_info_type
        store_keys = [e for e in self.entries.items() if e[0].startswith(key_prefix) and e[0].endswith(key_end)]
        
        ret = []
        for k, v in store_keys:
            stream = io.BytesIO(k)
            util.read_safe(stream, len(key_prefix)) # skip the prefix
            store_id = util.read_varint64(stream)
            store_name = v.decode('utf-16be')
            
            ret.append(IDB.DbStore(store_name, store_id))
        return ret
    
    def get_obj_store_entries(self, db_id, obj_store_id):
        ''' Get all key-value entries of the object store '''
        
        # not sure exactly of the index structures, but it seems like id 1 returns all the entries
        index_id = 1
        store_entries = []
        store_deleted_entries = []
        key_prefix = IDBKey.encode_key_prefix(db_id, obj_store_id, index_id)
        ldb_store_entries = [e for e in self.entries.items() if e[0].startswith(key_prefix)]
        ldb_store_deleted_entries = [e for e in self.deleted_entries.items() if e[0].startswith(key_prefix)]
        for k, v in ldb_store_entries:
            key = IDBKey(k).key
            val = IDBValue(v).value
            store_entries.append(IDB.Entry(key, val))
        for k, v in ldb_store_deleted_entries:
            key = IDBKey(k).key
            val = IDBValue(v).value
            store_deleted_entries.append(IDB.Entry(key, val))
        return store_entries, store_deleted_entries
