import pandas as pd
from reports.data_mixin import *
import os
#Игнорирование предупреждений
import warnings
warnings.filterwarnings("ignore")
#Импорт графиков
from scripts.plot_scripts.basegraphs import *
from scripts.keymasterlib.keymaster import *
#Подключение гугл таблиц
from scripts.filemanager.googlesheets_upload_lib import Googlesheets  
from scripts.filemanager.googlesheets_upload_lib import ListConventer

class GetData:
    
    def __init__ (self, client):
        self.client = client
        self.work()
        
    def work (self):       
        
        client = self.client        
        """
        Подкласс получения данных
        """
        class GetData (GetMixinGenerateBigData):
            client = self.client   
            dataset_name = 'google_ads_keys'
            deep = 1
        gt = GetData()
        data = gt.data                
        self.data = data.fillna('Нет данных')        

"""
Обслуживащий класс получения данных
"""

class FirstStep:
    def __init__ (self, client):
        
        gd = GetData (client)      
        ad_data = gd.data     
        fttd = FromTableToDictionary(client)
        basekeydict = fttd.basekeydict
        dirty_keys = list(ad_data['ga:adMatchedQuery'])
        kc = Keyword_Clustering(basekeydict, dirty_keys)  
        ad_data['key_type'] = kc.clasters
        
        #Базовые вычисления
        data_queries = ad_data.groupby('key_type').sum().\
                sort_values('ga:adClicks', ascending=False).reset_index()

        data_queries['ctr'] = data_queries.apply\
        (lambda x: int(x['conversions'] / x['ga:adClicks'] * 10000) / 100 if\
         x['ga:adClicks'] !=0 else 0, axis=1)

        data_queries['cpa'] = data_queries.apply\
        (lambda x: int(x['ga:adCost'] / x['conversions'] * 100) / 100 if\
         x['conversions'] !=0 else 0, axis=1)

        data_queries['ga:adCost'] = data_queries.apply(lambda x:int(x['ga:adCost']), axis=1)

        
        fd = data_queries[data_queries['ga:adClicks'] > 20]
        fd = fd[['key_type', 'ga:adClicks', 'ga:adCost', 'cpa', 'ctr', 'conversions']]
        
        self.fd = fd

        #Неразмеченный список запросов с эдвордса добавляем в соответствующую папку
        undef = ad_data[ad_data['key_type'] == 'не определено']
        undef.to_excel(os.path.join('data/'+client, 
                                         'гугл эдс запросы словаря.xlsx'), 
                                          sheet_name='er')
        fd.to_excel(os.path.join('data/'+client, 
                                         'месячная таблица по группам ключей в эдвордсе.xlsx'), 
                                          sheet_name='er')
        
"""
Класс отрисовки графиков. Загружаем данные и создаем визуализацию
"""
        
class Ads_keyreport:
    
    def __init__ (self, client):
        self.client = client
        fs = FirstStep (client)
        self.df = fs.fd.head(20)
        self.work() #вызываем класс ниже
                
    def work (self):   
        
        df = self.df   
        df = df[df['key_type'] != 'не определено']
        client = self.client
        self.data = df
        
        #Выгрузка отчета
        lc = ListConventer(df.head(20),
                            'width_headers')
          
        maxlonglist = 21
        maxwidthlist = 6
        width = [''] * maxwidthlist
        maxlist = [width] * maxlonglist    
        Googlesheets('data adwords!a1:f21', maxlist, client)  
        Googlesheets('data adwords!a1:f21', lc.datsheets, client)         