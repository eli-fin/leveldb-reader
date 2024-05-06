/*
Microsoft Teams is an Electron Chrome based app.
To open its dev tools, left-click 7 times on its notification icon, then right click and you'll see that option.
The dev tools windows we're interested in is MainWindow.

The data seems to be (mostly) stored in IndexedDB object stores (a mix of teams and skype schema).

Below are some convenience methods to quickly get/search through object stores, since the devtools UI doesn't offer much.
This can be run in the console of the dev tools window.

The access token used by Teams can be found in the Local Storage: ts.<oid>.cache.token.https://api.spaces.skype.com
The oid can be found in the (encoded cookie) ringFinder.
*/

// create db, store and add a key
//
// usage:
// var db = await createDbAndStore({name: 'my_test', version: 1}, 'test_store_1')
// db.transaction(['test_store_1'], 'readwrite').objectStore('test_store_1').add('joe', 'name')
async function createDbAndStore(db_info, store_name) {
    var createdStore = false
    var db = await new Promise((resolve, reject)=>{
        var db_req = indexedDB.open(db_info.name, db_info.version)
        db_req.onupgradeneeded = (e)=> {
            e.target.result.createObjectStore(store_name)
            createdStore = true
        }
        db_req.onsuccess = (e)=> resolve(e.target.result)
        db_req.onerror = (e)=> reject(e)
    })
    console.log(`createdStore=${createdStore}`)
    return db
}


// get array of all key,value entries in specifies db and store
async function getEntireStore(db_info, obj_store) {
    var db = await new Promise((resolve, reject)=>{
        var db_req = indexedDB.open(db_info.name, db_info.version)
        db_req.onsuccess = (e)=> resolve(e.target.result)
        db_req.onerror = (e)=> reject(e)
    })
            
    var trans = db.transaction([obj_store], 'readonly')
    var store = trans.objectStore(obj_store);
    
    // get all values
    //var data = await new Promise((resolve, reject)=>{
    //    var data_req = store.getAll()
    //    data_req.onsuccess = (e)=> resolve(e.target.result)
    //    data_req.onerror = (e)=> reject(e)
    //})
    
    // get all keys and values
    var data = []
    await new Promise((resolve, reject)=>{
        var curser_req = store.openCursor()
        curser_req.onsuccess = e => {
            var cursor = event.target.result
            if (cursor) {
                data.push({key: cursor.primaryKey, value: cursor.value})
                cursor.continue()
            } else {
                resolve(data)
            }
        }
        curser_req.onerror = (e)=> reject(e)
    })
    
    db.close()
    return data
}


// get value by db, store and key
async function getKey(db_info, obj_store, key) {
    var db = await new Promise((resolve, reject)=>{
        var db_req = indexedDB.open(db_info.name, db_info.version)
        db_req.onsuccess = (e)=> resolve(e.target.result)
        db_req.onerror = (e)=> reject(e)
    })
            
    var trans = db.transaction([obj_store], 'readonly')
    var store = trans.objectStore(obj_store);
    
    var val = await new Promise((resolve, reject)=>{
        var key_req = store.get(key)
        key_req.onsuccess = (e)=> resolve(e.target.result)
        key_req.onerror = (e)=> reject(e)
    })
    
    db.close()
    return val
}


// see if an object includes a string
function objContaines(val, keyword) {
    return !!val && JSON.stringify(val).toLowerCase().includes(keyword.toLowerCase())
}


// search for keyword in all DBs and stores, and log matches
async function findInAllDBs(keyword, print_keys, print_values) {
    var dbs = await getAllDbsAndStores()
    for (var db of dbs) {
        for (var store of db.objectStoreNames) {
            var data = await getEntireStore(db, store)
            var matches = data.filter(d=>objContaines(d, keyword))
            if (matches.length > 0) {
                console.log(`found ${matches.length} matches in ${db.name} (${db.version}) ${store}`)
                if (print_keys) {
                    for (var match of matches) {
                        if (print_values) {
                            console.log(match.key, match.value)
                        } else {
                            console.log(match.key)
                        }
                    }
                }
            }
        }
    }
}


// get an array of all DBs and object stores
// [{name=name, version=version, stores=[store1, store2, ...]}, ...]
async function getAllDbsAndStores() {
    var all_db_objs = await indexedDB.databases()
    
    for (var db_obj of all_db_objs) {
        var db = await new Promise((resolve, reject)=>{
            var db_req = indexedDB.open(db_obj.name, db_obj.version)
            db_req.onsuccess = (e)=> resolve(e.target.result)
            db_req.onerror = (e)=> reject(e)
        })
        db_obj.objectStoreNames = db.objectStoreNames
        db.close()
    }
    
    return all_db_objs
}


// get messages for specific chat
async function getMessagesOfChat(my_name, other_name) {
    var all_dbs = await getAllDbsAndStores()
    
    // find db with prefix followed by 72 (2 uuid's), since there are a few with the same prefix
    var all_users_db_name_prefix = 'skypexspaces-'
    var all_users_db_info = all_dbs.filter(db=>db.name.substr(0, db.name.length-72-1) == all_users_db_name_prefix)[0]
    var all_users_db = (await getEntireStore(all_users_db_info, 'people')).filter(u=>u.value.type=='person')
    
    // get info for me and others
    var my_info = all_users_db.filter(u=>u.value.displayName == my_name)[0]
    var other_info = all_users_db.filter(u=>u.value.displayName == other_name)[0]
    
    // find replies db
    var replies_db_info = all_dbs.filter(db=>db.name.includes('replychain-manager'))[0]
    var replies_db = await getEntireStore(replies_db_info, 'replychains')
    
    // get list of our replies
    var me_and_other_replies = replies_db.filter(r=>{
        var key = r.key[0]
        return key.includes(my_info.value.objectId) && key.includes(other_info.value.objectId)
    })
    
    // sort by timestamp descending
    var me_and_other_replies = me_and_other_replies.sort((a,b)=>
        (Object.values(a.value.messageMap)[0].originalArrivalTime-
        Object.values(b.value.messageMap)[0].originalArrivalTime)
        *-1)
    
    // map to actual content, and add arrow based on msg direction
    return me_and_other_replies.map(m=> {
        var msg = Object.values(m.value.messageMap)[0]
        if (msg.imDisplayName == my_name) {
            return '->: ' + msg.content
        } else {
            return '<-: ' + msg.content
        }
    })
}

await findInAllDBs('some text...', true, true)

(await getMessagesOfChat('Me, Myself', 'and I')).slice(0, 10)
