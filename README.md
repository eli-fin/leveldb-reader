# leveldb

A [level db](https://github.com/google/leveldb/) library implemented fully in Python.
Only read has been implemented and there's no support for writing/editing.
Also, this implementation will read the entire DB content into memory and there's
no handling of comparators or more efficient lookups.

This is a minimal implementation and not very thorough. Not all features or edge cases
have been implemented or addressed, but you should be able to get all key/value pairs
from any valid DB. This code probably isn't recommended for production.
(DB files are opened in read-only mode though, so no potential for corruption).

My intention was to use this reader in order to eventually extract IndexedDB values,
so I can get the content of Microsoft Teams messages.

### Dependencies
`pip install python-snappy`

#### Other related implementations I came across:
- https://github.com/cclgroupltd/ccl_chrome_indexeddb (a re-implementation of leveldb and indexeddb)
- https://github.com/IncideDigital/rvt2/blob/master/plugins/common/RVT_skype.py (a re-implementation of indexeddb)
- https://github.com/gro-ove/actools/tree/master/AcTools.LapTimes.LevelDb (a re-implementation of indexeddb)


#### Accessing LevelDB directly
You can use python wrappers (which use the official LevelDB implementation) to access the raw values in a chrome db.
Note: you must pass a comparator with the same name used to create the db, or it will refuse to open it.
You can use a simple comparator, but since it doesn't really implement the comparison, it might lead to inaccurate iterations or missing values.
Examples 1:
```
pip install leveldb

import leveldb
cmp = lambda a,b: -1 if a < b else 1 if b < a else 0)
db = leveldb.LevelDB('db_folder',comparator=('idb_cmp1', cmp))
pairs={bytes(k):bytes(v) for k,v in db.RangeIter()}
```
Example 2:
```
pip install plyvel --only-binary :all:

import plyvel
db = plyvel.DB('db_folder', comparator_name=b'idb_cmp1', comparator=cmp)
with open('dump.log', 'w') as log:
    for k,v in db.iterator(): log.write(f'{k}: {v}\n')
```

## indexeddb.py
A module for parsing IndexedDB databases from a LevelDB database.

## ms_teams_reader.py
A module for getting Teams chats/notifications from an IndexedDB.

## main.py
You can run this to get some basic info about a LevelDB database or one of it's files.

## ms_teams_indexed_db.js
Some JS code to help browse through Teams' DB using Chrome DevTools.

## googletasks.py
A script to access Google Tasks APIs.
You can use this script to periodically get the latest updates from Teams
and create a Google Tasks list with that info.
This is useful if you don't have easy access to Teams from your phone or if you don't want to install it :)

### Usage
1. Get a client id and secret
   - Go to https://console.developers.google.com/ and create a project (or use an existing one)
   - Under "Enabled APIs & services", search for "Tasks API" and enable it for this project
   - Go to "OAuth consent screen", configure as external,
     then add the scope "https://www.googleapis.com/auth/tasks" (manually, if needed),
     finally, add yourself to test users
   - Under "Credentials", create a new "OAuth Client ID"
     (type=Web Application, Authorized redirect URIs=http://localhost:8080)
   - Now you'll get a client id and secret (which you can access later too)
2. Run the app
   - Create python virtual env (recommended): `python -m venv .venv` (first time only)
   - Activate it: `.venv\Scripts\activate.bat`
   - Install dependencies: `pip install python-snappy lxml`  (first time only)
   - Run: `python googletasks.py` (on first run you'll be asked for the client id and secret, which will be persisted in a config file for subsequent runs)

## Platforms
I run this on Windows most of the time, but the code should work just as well on any python supporting OS
(except you'll have to fix the default `ms_teams_reader.DB_PATH` and `googletasks.start_browser_background` won't work)
