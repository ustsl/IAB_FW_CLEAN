from scripts.plot_scripts.basegraphs import *
import pandas as pd
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
from scripts.estimatelib.universal_estimate import Universal_Estimate

#Допфункции
from functions.basic_func import *
#Подключение оболочки отчетов
from reports.data_mixin import *


class DataPrepare:
    
    def __init__ (self, client):   
        self.client = client

        """
        Готовим даннные
        """        
        class GetData (GetMixinGenerateBigData):
            client = self.client   
            dataset_name = 'yandex_seo_keys'
            deep = 12
        gt = GetData()
        dataset = gt.data
        self.dataset = dataset
        #Убираем пустые данные, склеиваем урлы, перемножаем средние параметры
        def not_nan_func(row):
            if row['<attribution>SearchPhrase'] != row['<attribution>SearchPhrase']:
                return 'нет данных'
            return row['<attribution>SearchPhrase']
        dataset['<attribution>SearchPhrase'] = dataset.apply(not_nan_func, axis=1)
        dataset = dataset.fillna(0)    
        dataset['sumduration'] = dataset.apply(lambda x: x['visits'] * x['avgVisitDurationSeconds'], 
                                              axis=1)
        dataset['sumdepth'] = dataset.apply(lambda x: x['visits'] * x['pageDepth'], 
                                              axis=1)
        dataset['date'] = dataset.apply(lambda x: x['startOfMonth'][:-3], axis=1)
        
        self.df = dataset[['date', '<attribution>SearchPhrase', 
                           'visits', 'conversions', 
                           'sumduration', 'sumdepth']]
        """
        Окончание подготовки данных
        """        

        
class Bull_Nativ_Report:
    def __init__ (self, client):

        dp = DataPrepare(client)
        data = dp.df
        fttd = FromTableToDictionary(client)
        basekeydict = fttd.basekeydict
        dirty_keys = list(data['<attribution>SearchPhrase'])
        kc = Keyword_Clustering(basekeydict, dirty_keys)  
        data['key_type'] = kc.clasters

        def nativ_bull_func (row):
            if 'натив' in row['key_type'].lower():
                return 'Брендовые'
            return 'Независимые'
        
        
        
        data['claster_type'] = data.apply(nativ_bull_func, axis=1)
        self.data = data
        data = data[['date', 'claster_type', 'visits', 'conversions', 'sumdepth', 'sumduration']]
        
        

        data = data.groupby(['date', 'claster_type']).\
        sum().sort_values('date', ascending=False).reset_index()

        data = expand_func(data)

        lc = ListConventer(data, 'width_headers')
        maxlonglist = 900
        maxwidthlist = 7
        width = [''] * maxwidthlist
        maxlist = [width] * maxlonglist
        Googlesheets('data yandex!a1:z1000', maxlist, client)  
        Googlesheets('data yandex!a1:z1000', lc.datsheets, client)       