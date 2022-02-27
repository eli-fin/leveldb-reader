'''
Main file. You can run this to get some basic info about a LevelDB database or one of it's files.
'''

import os
import sys


# validate deps early
try:
    import snappy
except ImportError:
    exit('missing dependencies, run "pip install python-snappy"')


import indexeddb
from leveldb import db
from leveldb import log_reader
from leveldb import table_reader


def main(argv):
    '''
    Expect type and path (i.e. table, db), parse it and print some basic info about it.
    This also serves as an example use of each module.
    '''
    valid_types = 'db', 'idb', 'table', 'log', 'manifest'
    try:
        arg_type = argv[1]
        path = argv[2]
        assert arg_type in valid_types
    except (Exception, AssertionError):
        exit(f'Usage: {argv[0]} {"|".join(valid_types)} <path>\n'
             f'  db       - a LevelDB folder\n'
             f'  idb      - a Chrome IndexedDB folder\n'
             f'  table    - a LevelDB table file (.ldb)\n'
             f'  log      - a LevelDB log file (.log)\n'
             f'  manifest - a LevelDB manifest file\n')
    
    print(f'Opening {arg_type} entry: {path}')
    
    if arg_type == 'db':
        ldb = db.DB(path, True, True)
        print('Printing first 10 entries:')
        for k,v in list(ldb.entries.items())[:10]:
            print(f'{k}: {v}')
    
    if arg_type == 'idb':
        ldb = db.DB(path, False, True)
        idb = indexeddb.IDB(ldb.entries)
        idb_databases = idb.get_db_names()
        for idb_db in idb_databases:
            print(f'IndexedDB - DB: name={idb_db.name}, id={idb_db.id}')
            for store in idb_db.stores:
                print(f'\tStore: name={store.name}, id={store.id}')
    
    if arg_type == 'table':
        table = table_reader.Table(path)
        print('Found', len(table.entries), 'entries')
        print('Found', len(table.meta_entries), 'meta_entries')
        print('Found', len(table.deleted_entries), 'deleted entries')
        print('Printing first 10 entries:')
        for k,v in list(table.entries.items())[:10]:
            print(f'{k}: {v}')
    
    if arg_type == 'log':
        entries, deleted_entries = log_reader.get_log_entries(path)
        print(f'Found {len(entries)} entries and {len(deleted_entries)} deletions')
        print('Printing first 10 entries:')
        for k,v in list(entries.items()[:10]):
            print(f'{k}: {v}')
    
    if arg_type == 'manifest':
        m = log_reader.Manifest(path)
        print('comparator_name:', m.comparator_name)
        print('log_number:', m.log_number)
        print('next_file_number:', m.next_file_number)
        print('last_sequence:', m.last_sequence)
        #print('compact_pointers:', m.compact_pointers)
        print('deleted_files:', [f'(level={f.level}, number={f.number})' for f in m.deleted_files])
        print('new_files:', [f'(level={f.level}, number={f.number}, size={f.size})' for f in m.new_files])
        print('prev_log_number:', m.prev_log_number)
        print('existing_files:', m.existing_files)


if __name__ == '__main__':
    main(sys.argv)
