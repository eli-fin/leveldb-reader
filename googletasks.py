'''
Module for using Google Tasks API.

On first run, the module will authenticate and persist a refresh token.
On subsequent runs, it will periodically get the latest updates from the Teams DB
and create a Tasks list with that info.
'''

CONFIG_FILE_NAME    = 'config.json'
UPDATE_INTERVAL_SEC = 600
AUTH_SCOPE          = 'https://www.googleapis.com/auth/tasks'
TASKS_LIST_NAME     = 'My Teams Data'


import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
import io
import json
import os
import threading
import time
import traceback
import urllib.request


import ms_teams_reader


class GoogleTokenService:
    '''
    Handle authentication, getting and refreshing a token.
    see https://developers.google.com/identity/protocols/oauth2/web-server
    '''
    
    def __init__(self, client_id, client_secret, refresh_token):
        self.expires = 0 # force refresh on instantiation
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
    
    @staticmethod
    def serve_request(address, port):
        '''
        start a server, wait for a request to arrive and return the request path.
        we can use this to get the value of the oauth code sent to the redirect url
        '''
        path = None
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                nonlocal path
                print('Code received')
                path = self.path
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'Code received. You can close your browser now.')
            
            def log_request(self, msg):
                pass # suppress logging
        
        print('Starting server...')
        while path == None:
            # this server gets stuck and doesn't respond to keyboard interrupts
            # so do it in a loop
            try:
                server = HTTPServer((address, port), Handler)
                server.timeout = 1 # seconds
                server.handle_request()
            finally:
                server.server_close()
        
        return path
    
    @staticmethod
    def start_browser_background(url, delay_sec):
        ''' start the browser in the background, so the user can authenticate while we're waiting for the redirect '''
        def worker():
            print('Starting your browser, please login and authenticate')
            time.sleep(delay_sec)
            os.system('start "" "' + url + '"')
        t = threading.Thread(target=worker)
        t.start()
    
    @staticmethod
    def authenticate(redirect_uri, client_id, client_secret, scope):
        ''' function to authenticate the first time (and get a refresh token, which can then be used) '''
        print('No refresh token found, re-authenticating')
        redirect_uri_encoded = urllib.request.quote(redirect_uri)
        
        # get auth code
        # (we want access_type=offline so we can get a refresh_token in the /token request,
        #  but it only returns a refresh_token if the /auth request specified prompt=consent)
        auth_url = \
            f'https://accounts.google.com/o/oauth2/v2/auth?' \
            f'client_id={client_id}&response_type=code&access_type=offline&prompt=consent&'\
            f'redirect_uri={redirect_uri_encoded}&scope={scope}'

        GoogleTokenService.start_browser_background(auth_url, 2)
        host, port = urllib.request.urlsplit(redirect_uri).netloc.split(':')
        port = int(port)
        redirect_request_path = GoogleTokenService.serve_request(host, port)
    
        # get the code from the path, which looks like:
        # /?code=<code>&scope=<scope>
        query = urllib.request.urlparse(redirect_request_path).query
        code = urllib.parse.parse_qs(query)['code'][0]
        
        # get access token
        token_request = urllib.request.Request(
            'https://oauth2.googleapis.com/token',
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            data =
                f'code={code}&client_id={client_id}&client_secret={client_secret}&' \
                f'redirect_uri={redirect_uri_encoded}&grant_type=authorization_code'.encode())
        
        with urllib.request.urlopen(token_request) as response:
            response_obj = json.load(response)
        # get the refresh_token (we ignore the token, as we'll re-get it every time we start)
        refresh_token = response_obj['refresh_token']
        return refresh_token
    
    def get_token(self):
        ''' get the token, refresh if expired '''
        if time.time() > self.expires:
            token_refresh_request = urllib.request.Request(
                'https://oauth2.googleapis.com/token',
                headers={'Content-Type': 'application/x-www-form-urlencoded'},
                data =
                    f'client_id={self.client_id}&client_secret={self.client_secret}&' \
                    f'refresh_token={self.refresh_token}&grant_type=refresh_token'.encode())
            
            with urllib.request.urlopen(token_refresh_request) as response:
                response_obj = json.load(response)
            
            self.token = response_obj['access_token']
            self.expires = time.time() + response_obj['expires_in'] - 30 # save expiration time, with a 30 sec buffer
        return self.token


class Config:
    ''' Handle config, read from file and persist updates '''
    
    def init(self):
        if not os.path.isfile(CONFIG_FILE_NAME):
            raise FileNotFoundError()
        else:
            print('Loading config')
            with open(CONFIG_FILE_NAME) as f:
                values = json.load(f)
            
            try:
                self.client_id      = values['client_id']
                self.client_secret  = values['client_secret']
                self.refresh_token  = values['refresh_token']
            except KeyError as e:
                raise Exception(f'Invalid config, missing key "{e.args[0]}"')
    
    def create_config(self):
        print('No config file. Please authenticate.')
        
        values = {}
        values['client_id']     = input('Please enter the client_id: ').strip()
        values['client_secret'] = input('Please enter the client_secret: ').strip()
        redirect_uri            = input('Please enter the redirect_uri: ').strip()
        
        values['refresh_token'] = GoogleTokenService.authenticate(redirect_uri, values['client_id'], values['client_secret'], AUTH_SCOPE)
        with open(CONFIG_FILE_NAME, 'w') as f:
            json.dump(values, f, indent=2)


def google_tasks_api(token, resource, method='GET', data=None):
    ''' Make a Google tasks API call and return result object '''
    BASE_URL = 'https://tasks.googleapis.com/tasks/v1/'
    req_obj = urllib.request.Request(
        f'{BASE_URL}{resource}',
        method=method,
        headers={'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token},
        data = json.dumps(data).encode() if data else None)
    with urllib.request.urlopen(req_obj) as req:
        content_length = req.headers['Content-Length']
        resp_obj = None if content_length == '0' else json.load(req) 
    return resp_obj


def get_cleared_list(token, name):
    ''' get a list by this name, with no items in it (if it exists, it's deleted and re-created) '''
    # get all lists, and delete if it exists
    all_lists = google_tasks_api(token, 'users/@me/lists')
    teams_lists = [item for item in all_lists['items'] if item['title'] == name]
    for lst in teams_lists:
        google_tasks_api(token, 'users/@me/lists/' + lst['id'], 'DELETE')
    
    new_list = google_tasks_api(token, 'users/@me/lists', 'POST', {'title': name})
    return new_list


def add_task(token, list_id, title, notes):
    ''' add a task to given list '''
    google_tasks_api(
        token,
        f'lists/{list_id}/tasks',
        'POST',
        {'title': title,
         'notes': notes})


def main():
    print('Starting')
    config = Config()
    try:
        config.init()
    except FileNotFoundError:
        # config doesn't exist, create config and exit
        config.create_config()
        exit('Config created, please restart app')
    token_service = GoogleTokenService(config.client_id, config.client_secret, config.refresh_token)
    while True:
        chat_messages, notifications, meeting_messages = ms_teams_reader.get_last_updates()
        token = token_service.get_token()
        tasks = []
        
        new_list = get_cleared_list(token, TASKS_LIST_NAME)
        
        update_time = datetime.datetime.fromtimestamp(time.time()).strftime('%d/%m %H:%M')
        tasks.append(('Update time: ' + update_time, ''))
        
        for chat in chat_messages:
            task_title = f'Chat: {chat}'
            task_notes = '\n\n'.join(f'{msg.arrival_time} - {msg.user}: {msg.content}' for msg in chat_messages[chat])
            tasks.append((task_title, task_notes))

        task_title = 'Notifications'
        task_notes = ''
        for notification in notifications:
            task_notes += f'{notification.arrival_time} - {notification.user} ({notification.activity_type}){" on " + notification.topic if notification.topic else ""}:\n'
            if notification.preview:
                task_notes += notification.preview + '\n'
            task_notes += '\n'
        tasks.append((task_title, task_notes))

        for topic in meeting_messages:
            task_title = f'Meeting: {topic}'
            task_notes = '\n\n'.join(f'{msg.arrival_time} - {msg.user}: {msg.content}' for msg in meeting_messages[topic])
            tasks.append((task_title, task_notes))
        
        for task in reversed(tasks): # task are added to the top, so reverse
            add_task(token, new_list['id'], task[0], task[1])
        
        print('Updated at: ' + update_time)
        time.sleep(UPDATE_INTERVAL_SEC)


if __name__ == '__main__':
    prev_error_type = None
    prev_error_code = 0
    
    while True:
        try:
            main()
        except Exception as e:
            traceback.print_exception(e)
            if hasattr(e, 'read'):
                print(e.read().decode())
            
            # on repeating error, break if same http error code, or same non-http error
            if type(e) == prev_error_type:
                if isinstance(e, urllib.request.HTTPError):
                    if prev_error_code == e.code:
                        break
                else:
                    break
            
            prev_error_type = type(e)
            if isinstance(e, urllib.request.HTTPError):
                prev_error_code = e.code
