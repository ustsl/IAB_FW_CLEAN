#!/usr/bin/env python
# coding: utf-8

# In[1]:
import json
import requests
from pprint import pprint
from yaml import load 
from datetime import datetime, timedelta

class Token_analytics:
            
    def upd_token (self):
        
    
        
        #Функция обновления токена
        config = json.load( open('analytics.dat') )
        client_id = config['client_id']
        client_secret = config['client_secret']
        refresh_token = config['refresh_token']



        def update_token(client_id, client_secret, refresh_token):
            """Обновление токена для запросов к API. Возвращает токен"""    
            url_token = 'https://accounts.google.com/o/oauth2/token'
            params = { 'client_id' : client_id, 'client_secret' : client_secret, 
                        'refresh_token' : refresh_token, 'grant_type' : 'refresh_token' }
            r = requests.post( url_token, data = params )  
            print('Токен выдан до {}'.format(datetime.today() + timedelta( hours = 1 )))
            return r.json()['access_token']

        token = update_token(client_id, client_secret, refresh_token) #вызов обновления токена
                
        self.token = token


