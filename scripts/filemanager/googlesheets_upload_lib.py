#Для загрузок
import json
import requests
from pprint import pprint
from yaml import load

#Стартовые библиотеки для выгрузки/загрузки отчета
import httplib2
import argparse
from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

#Работа с папками
import os

class Googlesheets:
    def __init__(self, connectsheets, dat_sheets, client_name):
        
        self.connectsheets = connectsheets
        self.dat_sheets = dat_sheets
        self.client_name = client_name       
        
        # Разрешения на просмотр и редактирование
        SCOPES = 'https://www.googleapis.com/auth/spreadsheets'
        # Название файла с идентификаторами
        CLIENT_SECRET_FILE = 'client_secret_grants.json'
        # Название приложения для нашего онлайн-отчета (можно использовать любое)
        APPLICATION_NAME = 'Google Sheets API Report'
        # название файла, 
        #в который после процедуры авторизации будут записаны токены 
        #(сейчас его еще не должно быть в папке)        
        # для Google Analytics этот файл назывался 'analytics.dat', 
        #но в разработчики Google Sheets API выбрали другое название
        # чтобы вам при необходимости было проще ориентироваться в документации Google API, 
        #мы сохранили такую же систему обозначений
        
        credential_path = 'sheets.googleapis.com-report.json'

        #Процесс авторизации
        store = Storage(credential_path)
        credentials = store.get()
        parser = \
        argparse.ArgumentParser\
        (formatter_class=argparse.RawDescriptionHelpFormatter, parents=[tools.argparser])
        flags = parser.parse_args([])
        if not credentials or credentials.invalid:
            flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
            flow.user_agent = APPLICATION_NAME
            if flags:
                credentials = tools.run_flow(flow, store, flags)
            print('Storing credentials to ' + credential_path)
    
        #Формирование запроса
        http = credentials.authorize(httplib2.Http())
        discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?version=v4')
        service = discovery.build('sheets', 'v4', http = http, discoveryServiceUrl = discoveryUrl)
        f = open(os.path.join('clients/'+self.client_name, 'config.yaml') , 'r')
        #f = open('config.yaml', 'r')
        sheet_config = load(f)

        # вызов идентификатора файла
        spreadsheetId = str(sheet_config['sheet_id'])

        def tabupload():
            spreadsheetId  = str(sheet_config['sheet_id']) #обращаемся к айдишнику документа
            result = \
            service.spreadsheets().values().get(spreadsheetId = \
                                                spreadsheetId, range = range_name).execute() 
            values = result.get('values', [])  #формируем запрос из заданных выше значений
            #отправляем запрос
            service.spreadsheets().values().update( spreadsheetId = spreadsheetId, 
                                               range = range_name, 
                                               valueInputOption = 'USER_ENTERED', 
                                               body = body ).execute()
            print ('Выгрузка завершена успешно')   

        body = {'values': dat_sheets} #обрамляем данные в словарь
        range_name = connectsheets  #задаем диапазон - лист и где именно работаем на листе
        try:
            tabupload()
        except:
            print ('Выгрузка данных не произведена. Проверьте наличие листа, или размерность диапазона')

   
class ListConventer:
    
    """
    Конвертация датасета пандас в листы данных. Есть вариант с заголовками и без заголовков.
    Вариант с заголовками - width_headers
    Вариант без заголовков - None, или любое другое значение
    
    """
    
    def __init__ (self, data, header_preset):
        self.datsheets = []

        if 'pandas' in str(type(data)):        
            adsrate_dict = data.to_dict('records')
            y = 0
            if header_preset == 'width_headers':
                self.datsheets.append(list(data))

            for dat in adsrate_dict:
                listsheet = []
                for x in list(data):
                    listsheet.append(adsrate_dict[y][x])
                self.datsheets.append(listsheet)
                y += 1
        else:
            'Не обнаружен датасет Pandas. Проверьте переменные'