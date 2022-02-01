"""
Корневая библиотека получения данных из гугл. Упрощена.
"""
import time
from scripts.datareceiver.google_analytics.google_analytics_autorizer \
import Token_analytics
import httplib2
import argparse
from apiclient import discovery
from apiclient.discovery import build
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage
from oauth2client import file     
import json
import requests
from pprint import pprint
from yaml import load    
import os

#Пример запроса
###########################################
#client = 'angio_line'
#gparams = {'metrics': 'ga:adClicks, ga:goalCompletionsAll, ga:adCost',
#                        'dimensions': 'ga:adwordsCampaignID, ga:adGroup, ga:adMatchedQuery' }
#date = ['2020-01-01', '2020-01-02']
#
#test = {'client': client, 'googleParams': gparams, 'date': date }
#
#
#gu = Googleadwords_upload (test)
#############################################

class Googleadwords_upload:
    
    """
    Передаем список параметров
    """
    
    def __init__(self, params): 
        print ('Googleadwords_upload вызван. Собираю отчет')
        
        self.googleParams = params['googleParams']
        self.client = params['client']
        self.startDate = params['date'][1]
        self.endDate = params['date'][0]
        
        print ('Запускаем сбор данных из Google Analytics')
        print ('Даты отчета ->')
        print (self.startDate, self.endDate)
        print ('---')
        self.bigdatas = []
        self.workfunc()
        
        
    def workfunc (self):

        #библиотека подключения в аналитиксу     
        ta = Token_analytics()
        ta.upd_token()
        token = ta.token   
        ###

        try:
            f = open(os.path.join('clients/'+self.client, 'config.yaml') , 'r')
            configuration = load(f)
        except (OSError, IOError) as e:
            print ('Отсутствует названный клиент, или его файл конфигурации')


        #оформляем запросы
        url = 'https://analyticsreporting.googleapis.com/v4/reports:batchGet'
        profile_id = str(configuration['profile_id'])
        sheet_id = str(configuration['sheet_id'])
        api_name = 'analytics'
        api_version = 'v3'
        storage = file.Storage(api_name + '.dat')
        credentials = storage.get()
        http = credentials.authorize(http=httplib2.Http())
        service = build(api_name, api_version, http=http)
        
        

        #функция сбора данных
        def googlepars (start, maxres):        
            data = service.data().ga().get(
                ids='ga:' + profile_id,
                start_date = self.startDate,
                end_date = self.endDate,
                metrics = self.googleParams['metrics'],
                dimensions = self.googleParams['dimensions'],
                start_index= str(start),
                max_results=str(maxres))\
            .execute()
            return data
        ###
        start = 1
        maxres = 500
        data = googlepars (start, maxres)       
        users = 0
        
        while 'rows' in data:
            time.sleep(1)
            print('Выгружаю {} пользователей с offset = {}, \
            добавлено пользователей - {}'\
                  .format(maxres, start, users))   
            x = 0
            for dat in data['rows']:
                list_datas = []
                for m_and_d in data['rows'][x]:
                    list_datas.append(m_and_d)
                self.bigdatas += [list_datas]
                x += 1 
            x = 0
            print ('Цикл пройден. В листе - {}'.format(len(self.bigdatas)))
            start += maxres
            data = googlepars(start, maxres)

            if 'rows' in data:
                print ('Продолжаем')
            else:
                print ('Данные выгружены')
                break
            users += len(data['rows'])
        print ('Завершено')