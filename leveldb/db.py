'''
Module for parsing all entries in a LevelDB database
see partial format description here:    https://github.com/google/leveldb/blob/master/doc
see code implementation here:           https://github.com/google/leveldb
'''

import os


from . import log_reader
from . import table_reader


class DB:
    '''
    A database with all entries
    Initialize from db folder path
    
    The following member can be used to access the data:
    manifest        : manifest info (see log_reader)
    entries         : map of all entries
    deleted_entries : map of deleted entries (with values, if exist, otherwise None)
    '''
    
    def __init__(self, base_path, do_log):
        self.__do_log = do_log
        self.base_path = base_path
        
        self.__log('opening DB:', base_path)
        assert os.path.isdir(base_path), f'path "{base_path}" doesn\'t exist or is not a directory'
        
        # get a list of all files, so we can see at the end which weren't used
        # except for the log/lock files, which we don't read
        self.all_files = set(os.listdir(base_path)) - {'LOCK', 'LOG', 'LOG.old'}
        
        self.manifest = self.get_manifest()
        self.entries = {}
        self.meta_entries = {}
        self.deleted_entries = {}
        
        # read all data from table files
        self.__log('found', len(self.manifest.existing_files), 'table files')
        for f_number in self.manifest.existing_files:
            self.__log('reading table file', f_number)
            table = table_reader.Table(self.get_table_file(f_number))
            self.__log(f'got {len(table.entries)} entries, {len(table.deleted_entries)} deleted_entries, {len(table.meta_entries)} meta_entries')
            self.entries.update(table.entries)
            self.meta_entries.update(table.meta_entries)
            self.deleted_entries.update(table.deleted_entries)
        
        # read data from log file
        if self.manifest.log_number == -1:
            self.__log('no log file')
            deleted_keys = []
        else:
            self.__log('reading log file', self.manifest.log_number)
            entries, deleted_keys = log_reader.get_log_entries(self.get_log_file(self.manifest.log_number))
            self.__log(f'got {len(entries)} entries, {len(deleted_keys)} deleted_keys')
            self.entries.update(entries)
        
        # make sure all deleted_entries are not in entries
        for k in self.deleted_entries:
                self.entries.pop(k, None)
        # for each deleted key, if found in entries, move it to deleted_entries
        for k in deleted_keys:
            deleted_val = self.entries.pop(k, None)
            if deleted_val:
                self.deleted_entries[k] = deleted_val
                
        self.__log(f'found total of {len(self.entries)} entries, {len(self.meta_entries)} meta_entries, {len(self.deleted_entries)} deleted_entries')
        
        if self.all_files:
            # something is probably off or corrupt
            print('warning: the following files were found and not processed', self.all_files)
    
    def __log(self, *args):
        if self.__do_log:
            print(*args)
        
    def get_file(self, file):
        ''' Get a DB file by name (relative to DB path) '''
        self.all_files.discard(file)
        f = os.path.join(self.base_path, file)
        return f
    
    def get_table_file(self, number):
        ''' Get table file by number, 123 -> "path/to/db/000123.ldb" '''
        name = f'{number:06}.ldb'
        return self.get_file(name)
    
    def get_log_file(self, number):
        ''' Get log file by number, 123 -> "path/to/db/000123.log" '''
        name = f'{number:06}.log'
        return self.get_file(name)
    
    def get_manifest(self):
        self.__log('reading CURRENT file')
        current_path = self.get_file('CURRENT')
        assert os.path.isfile(current_path), f'"CURRENT" file doesn\'t exist or is a directory. Is this a valid LevelDB folder?'
        with open(current_path, 'r') as current:
            manifest_name = current.read(20)
            # validate no bytes are left, start and end
            assert current.read(1) == '', 'invalid manifest'
            assert manifest_name[-1] == '\n', 'invalid manifest'
            assert manifest_name.startswith('MANIFEST-'),  'invalid manifest'
            
            manifest_path = self.get_file(manifest_name.strip())
        
        self.__log('reading manifest:', manifest_path)
        return log_reader.Manifest(manifest_path)
