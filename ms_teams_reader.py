'''
Module to read the content of Microsoft Teams's database, which is a Chrome IndexedDB.
It provides methods for getting chat messages or notifications.
'''

import collections
import datetime
import io
import os
import re


# validate deps early
try:
    import snappy
    import lxml.html
except ImportError:
    exit('missing dependencies, run "pip install python-snappy lxml"')


from leveldb import db
import indexeddb


NUM_OF_NOTIFICATIONS = 15
NUM_OF_CHATS = 10
NUM_OF_MEETINGS = 10
NUM_OF_CHAT_MESSAGES = 10
MSGS_TO_SKIP = ('Event/Call',
                'ThreadActivity/AddMember',
                'ThreadActivity/MemberJoined',
                'ThreadActivity/MemberLeft',
                'RichText/Media_CallTranscript',
                'RichText/Media_CallRecording',
                'ThreadActivity/TopicUpdate')
LAST_UPDATES_FILE = 'teams_updates.log'

# This is where the teams application stores it's IndexedDB for this user
# If you're using teams in your chrome browser, the DB will be here (this module should work just as well with both)
# r'%AppData%\..\Local\Google\Chrome\User Data\Default\IndexedDB\https_teams.microsoft.com_0.indexeddb.leveldb'
DB_PATH = os.path.expandvars(r'%AppData%\Microsoft\Teams\IndexedDB\https_teams.microsoft.com_0.indexeddb.leveldb')

# define some types
ReplyInfo = collections.namedtuple('ReplyInfo', ('user', 'arrival_time', 'content'))
ChatMessage = collections.namedtuple('ChatMessage', ('key', 'arrival_time', 'user', 'content'))
Notification = collections.namedtuple('Notification', ('key', 'arrival_time', 'user', 'activity_type', 'topic', 'preview'))
MeetingMessage = collections.namedtuple('MeetingMessage', ('key', 'arrival_time', 'user', 'content'))


def format_timestamp(t):
    ''' format a teams timestamp value '''
    return datetime.datetime.fromtimestamp(t/1000).strftime('%d/%m %H:%M')


def html_to_text(html):
    ''' get text only from teams rich text/html messages '''
    if html.strip() == '':
        return ''
    return lxml.html.fromstring(html).text_content()


def normalize_notification_preview(msg):
    ''' remove empty lines and prefix each line with ">" '''
    lines = (line.strip() for line in msg.split('\n') if line.strip())
    return '\n'.join(f'> {line}' for line in lines)


def get_chat_name(chat, my_id, all_people_store):
    if '_' in chat.key:
        # chat key is in the format: "19:<id1>_<id2>@unq.gbl.spaces" (order depends on who started the conversation)
        return get_chat_participant_name(chat, my_id, all_people_store)
    else:
        # group chat key doesn't contain a '_'
        return get_chat_group_name(chat, my_id, all_people_store)


def get_chat_participant_name(chat, my_id, all_people_store):
    ''' get name of chat participant (name of other party) '''
    id1, id2 = re.match('\d+:(.+)_(.+)@.+', chat.key).groups()
    
    if my_id == id1:
        part_id = id2
    elif my_id == id2:
        part_id = id1
    else:
        assert False, 'invalid chat key'
    
    people_key = f'8:orgid:{part_id}'
    info = next((p for p in all_people_store if p.key == people_key), None)
    bot = False
    if info == None:
        # maybe it's a bot, they have a different prefix
        bot_key = f'28:{part_id}'
        info = next((p for p in all_people_store if p.key == bot_key), None)
        if info == None:
            assert False, f"can't find name of 'part_id'"
        bot = True
    
    name = info.value['displayName']
    return ('BOT:' if bot else '') + name


def get_chat_group_name(chat, my_id, all_people_store):
    ''' get name of chat (comma delimited list of names, except mine) '''
    members = chat.value['members']
    member_ids = []
    for m in members:
        # there seems to be a difference between teams versions
        if isinstance(m, tuple):
            member_ids.append(m[1]['id'])
        elif isinstance(m, dict):
            member_ids.append(m['id'])
    
    member_names = []
    for m in member_ids:
        if my_id not in m:
            m_info = next(p for p in all_people_store if p.key == m)
            m_name = m_info.value['displayName']
            member_names.append(m_name)
    return ', '.join(member_names)


def filter_duplicate_entries(entries):
    ''' filter duplicate db entries based on same key, while preserving order '''
    
    # Update:
    # at some times I was getting multiple chats/messages with the same key, probably due to some bug
    # in the leveldb or indexeddb implementation, around the area of deleted keys.
    # after some refactoring, this doesn't seem to be required anymore.
    
    ret = []
    seen = set()
    # reverse, caue if there's a duplicate entry, it seems the last one is more up to date
    # so we want to keep that
    for e in reversed(entries):
        key = str(e.key)
        if key not in seen:
            seen.add(key)
            ret.append(e)
    return ret


def get_last_chats(all_convs, all_replies, my_id, all_people):
    '''
    for each of the last few chats, return the last few messages
    return a dict of {chat_name: [chat_messages]}
    '''
    # get last few chats
    chat_convs = [c for c in all_convs if c.value['type'] == 'Chat' and c.value['lastMessage'] != None]
    chat_convs.sort(key=lambda c: c.value['lastMessageTimeUtc'], reverse=True)
    chat_convs = chat_convs[:NUM_OF_CHATS]
    ret = {}
    for chat in chat_convs:
        name = get_chat_name(chat, my_id, all_people)
        ret[name] = []
        chat_messages = [r for r in all_replies if r.key[0] == chat.key]
        chat_messages.sort(key=(lambda n: n.value['latestDeliveryTime']))
        chat_messages = chat_messages[-NUM_OF_CHAT_MESSAGES:]
        for chat_msg in chat_messages:
            reply_info = extract_reply_info(chat_msg)
            if reply_info == None:
                continue
            ret[name].append(ChatMessage(
                key=chat_msg.key,
                arrival_time=reply_info.arrival_time,
                user=reply_info.user,
                content=reply_info.content))
    return ret


def get_last_notifications(all_convs, all_replies):
    ''' return an entry for each of the last few notifications '''
    # find notification conversation id
    notification_conv = next(c for c in all_convs if c.key.endswith(':notifications'))
    # find replies
    notifications = [r for r in all_replies if r.key[0] == notification_conv.key]
    notifications.sort(key=(lambda n: n.value['latestDeliveryTime']), reverse=True)
    notifications = notifications[:NUM_OF_NOTIFICATIONS]
    ret = []
    for notification in notifications:
        msg_map = list(notification.value['messageMap'].values())[0]
        topic = msg_map['properties']['activity'].get('sourceThreadTopic', '').strip()
        user = msg_map['properties']['activity'].get('sourceUserImDisplayName', '<bot>') # looks like when it's a bot, it doesn't have this field
        activity_type = msg_map['properties']['activity']['activitySubtype']
        arrival_time = msg_map['originalArrivalTime']
        preview = msg_map['properties']['activity']['messagePreview']
        preview = normalize_notification_preview(preview)
        ret.append(Notification(key=notification.key, arrival_time=format_timestamp(arrival_time), user=user, activity_type=activity_type, topic=topic, preview=preview))
    return ret


def get_last_meeting_messages(all_convs, all_replies):
    '''
    for each of the last few meetings, return the last few messages
    return a dict of {meeting_topic: [meeting_messages]}
    '''
    # get last few meetings
    meet_convs = [c for c in all_convs if c.value['type'] == 'Meeting' and 'lastMessageTimeUtc' in c.value]
    meet_convs.sort(key=lambda c: c.value['lastMessageTimeUtc'], reverse=True)
    meet_convs = meet_convs[:NUM_OF_MEETINGS]
    ret = {}
    for meeting in meet_convs:
        topic = meeting.value['threadProperties']['topic']
        ret[topic] = []
        meeting_messages = [r for r in all_replies if r.key[0] == meeting.key]
        meeting_messages.sort(key=(lambda n: n.value['latestDeliveryTime']))
        meeting_messages = meeting_messages[-NUM_OF_CHAT_MESSAGES:]
        for meet_msg in meeting_messages:
            reply_info = extract_reply_info(meet_msg)
            if reply_info == None:
                continue
            ret[topic].append(MeetingMessage(
                key=meet_msg.key,
                arrival_time=reply_info.arrival_time,
                user=reply_info.user,
                content=reply_info.content))
    return ret


def extract_reply_info(reply_obj):
    msg = list(reply_obj.value['messageMap'].values())[0]
    if msg['messageType'] in MSGS_TO_SKIP:
        return None
    
    user = msg['imDisplayName']
    arrival_time = msg['originalArrivalTime']
    arrival_time = format_timestamp(arrival_time)
    content = msg['content']
    if 'deletetime' in msg['properties']:
        del_time = int(msg['properties']['deletetime'])
        content = f"<deleted at {format_timestamp(del_time)}>"
    elif msg['messageType'] == 'RichText/Html':
        content = html_to_text(content)
    content = content.replace('\r\n', ' ')
    
    return ReplyInfo(user, arrival_time, content)


def get_last_updates():
    # get ldb and list of idb db names
    ldb = db.DB(DB_PATH, False)
    idb = indexeddb.IDB(ldb.entries, ldb.deleted_entries)
    idb_databases = idb.get_db_names()
    
    # get conversation list
    conv_manager_db = next(db for db in idb_databases if db.name.startswith('Teams:conversation-manager:'))
    conv_store = next(c for c in conv_manager_db.stores if c.name == 'conversations')
    all_convs, _ = idb.get_obj_store_entries(conv_manager_db.id, conv_store.id)
    
    # get people info
    skypexspaces_db = next(db for db in idb_databases if db.name.startswith('skypexspaces-')
                          and len(db.name) == len('skypexspaces-00000000-0000-0000-0000-000000000000'))
    my_id = skypexspaces_db.name.split('-', 1)[1]
    people_store = next(c for c in skypexspaces_db.stores if c.name == 'people')
    all_people, _ = idb.get_obj_store_entries(skypexspaces_db.id, people_store.id)
    
    # get replychain list (this has an entry for each message in a conversation)
    reply_manager_db = next(db for db in idb_databases if db.name.startswith('Teams:replychain-manager:'))
    reply_store = next(c for c in reply_manager_db.stores if c.name == 'replychains')
    all_replies, _ = idb.get_obj_store_entries(reply_manager_db.id, reply_store.id)
    
    chat_messages = get_last_chats(all_convs, all_replies, my_id, all_people)
    notifications = get_last_notifications(all_convs, all_replies)
    meeting_messages = get_last_meeting_messages(all_convs, all_replies)
    
    return chat_messages, notifications, meeting_messages


def main():
    '''
    get the last few updates (last few messages from last few chats/meetings and last few notifications)
    and print them to a file
    '''
    chat_messages, notifications, meeting_messages = get_last_updates()
    with open(LAST_UPDATES_FILE, 'w', encoding='utf8') as f:
        for chat_name in chat_messages:
            print(f'Chat with {chat_name}', file=f)
            print(f'----------------', file=f)
            for msg in chat_messages[chat_name]:
                print(f'{msg.arrival_time} - {msg.user}: {msg.content}', file=f)
                #print(msg.key, file=f)
            print(file=f)
        
        print(f'Last {len(notifications)} notifications', file=f)
        print(f'---------------------------------------', file=f)
        for notification in notifications:
            print(f'{notification.arrival_time} - {notification.user} ({notification.activity_type}){" on " + notification.topic if notification.topic else ""}:', file=f)
            if notification.preview:
                print(notification.preview, file=f)
            #print(notification.key, file=f)
            print(file=f)
        
        for topic in meeting_messages:
            print(f'Meeting {topic}', file=f)
            print(f'---------------------', file=f)
            for msg in meeting_messages[topic]:
                print(f'{msg.arrival_time} - {msg.user}: {msg.content}', file=f)
                #print(msg.key, file=f)
            print(file=f)


if __name__ == '__main__':
    main()
