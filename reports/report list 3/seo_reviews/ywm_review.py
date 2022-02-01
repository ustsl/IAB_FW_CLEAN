import requests
from yaml import load
import pandas as pd
import os
import sys
from datetime import datetime #для пометки даты скана
from scripts.keymasterlib.keymaster import * #Подключаем библиотеку KeyMaster
#Подключение гугл таблиц
from scripts.filemanager.googlesheets_upload_lib import Googlesheets  
from scripts.filemanager.googlesheets_upload_lib import ListConventer

"""
Библиотека для работы с популярными поисковыми запросами в Яндекс Вебмастере.
"""

##############################
#Принцип написания библиотеки:
#Сверху вниз от частного к большему:
#Работа с API на самом верху - далее, заворачиваем в абстрации:
#API <- Список сайтов для проверки <- Сведение данных по запросам в единый JSON <-
#Создание примитивной панды <- #Подключение кейдикта и проведение вычислений.
###################################################################################


"""
Сборка в IAB:
15.06.2020
"""    

class YWM_popular_queries:
    """
    Класс отдает JSON с популярными поисковыми запросами. Работа по одному сайту в системе.
    """
    def __init__ (self, url):   

        f = open(os.path.join('settings', 'config.yaml') , 'r')
        config = load(f)
        token = config['ywmtoken']
        headers = {'Authorization': 'OAuth ' + token}

        service = 'https://api.webmaster.yandex.net/v4/user/'
        user = '1130000023832579/hosts/'
        host = 'https:{}:443/'.format(url)
        params = 'search-queries/popular/?order_by=TOTAL_CLICKS&query_indicator=TOTAL_CLICKS&query_indicator=TOTAL_SHOWS&query_indicator=AVG_CLICK_POSITION&query_indicator=AVG_SHOW_POSITION'

        apiurl = service+user+host+params
        r = requests.get(apiurl, headers=headers)
        self.data = r.json() 
        if len(self.data) == 4:
            print ('Its ok')
        if len(self.data) == 3:
            print (yw.data['host_id'] + ' | ' + yw.data['error_message'])

class GetSiteListforYWM:
    """
    Класс получает наименование клиента и исходя из этих данных работает со списком сайтов
    из конфига. 
    """
    def __init__ (self, client):
        f = open(os.path.join('clients/'+client, 'config.yaml') , 'r')
        config = load(f)
        self.urls = config['sites']     
      
class YWM_JSON_combination:
    """
    Класс по циклу проходит по доменам, собирая все запросы в единый файл запросов.
    Дальше с данными QUERIES можно работать уже в самой панде.
    """
    def __init__ (self, client):
        self.queries = []
        gs = GetSiteListforYWM(client)
        for url in gs.urls:
            yw = YWM_popular_queries(url)
            if len (yw.data) == 4:
                self.queries += yw.data['queries']
                       
class Create_YWM_pd:
    
    """
    Создаем панду на основе списка запросов
    """    
    
    def __init__ (self, client):
        yw = YWM_JSON_combination (client)
        
        headers = ['query', 
                   'shows', 
                   'clicks']

        query_list = []

        for string in yw.queries:

            query = string['query_text']
            shows = int(string['indicators']['TOTAL_SHOWS'] * 10) / 10   
            clicks = int(string['indicators']['TOTAL_CLICKS'] * 10) / 10   
            query_list += [[query, shows, clicks]]

        self.df = pd.DataFrame.from_records(query_list, columns = headers)
 

class YWM_Rewiew_for_Google_Sheets:
    
    """
    Класс генерит отчет на основании данных Яндекс Вебмастера
    и отправляет его в Гугл Таблицы
    """
    
    def __init__ (self, client):
        
        yw = Create_YWM_pd (client)
        data = yw.df
        fttd = FromTableToDictionary(client)
        basekeydict = fttd.basekeydict
        dirty_keys = list(data['query'])
        kc = Keyword_Clustering(basekeydict, dirty_keys)  

        #Создаем колонке имя с указанием даты последнего обновления
        now = datetime.now()
        actual_date = now.strftime("%d-%m-%Y")
        column_name = 'key_type ' + actual_date
        data[column_name] = kc.clasters
        data = data.groupby(column_name).sum().\
        sort_values('clicks', ascending=False).reset_index()
        data['ctr'] = data.apply\
        (lambda x:int(x['clicks'] / x['shows'] * 10000) / 100, axis=1)
        #Выгрузка отчета
        lc = ListConventer(data.head(30),
                           'width_headers')
        lc.conventer()
        Googlesheets('data ywm!a1:z31', lc.datsheets, client) 

