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
        self.work()
        
    def work (self):        
        client = self.client        
        """
        Подкласс получения данных
        """
        class GetData (GetMixinGenerateBigData):
            client = self.client   
            dataset_name = 'yandex_seo_keys'
            deep = 1
        gt = GetData()
        data = gt.data
        dataset = data.fillna('Нет данных')        
        self.dataset = dataset
        


#Класс по нативной кластеризации поисковых запросов

class Hard_YandexOrganic_KeyClaster:
    def __init__ (self, client):        
      
        dp = DataPrepare (client)
        data = dp.dataset    
    
        fttd = FromTableToDictionary(client)
        basekeydict = fttd.basekeydict
        dirty_keys = list(data['<attribution>SearchPhrase'])
        kc = Keyword_Clustering(basekeydict, dirty_keys)  
        data['key_type'] = kc.clasters
        
        undef = data[data['key_type'] == 'не определено']
        undef.to_excel(os.path.join('data/'+client, 
                                    'не размеченный список запросов поиск.xlsx'), 
                       sheet_name='er')


        data['sumduration'] = data.apply \
        (lambda x: x['avgVisitDurationSeconds'] * x['visits'], axis=1)
        data['sumdepth'] = data.apply \
        (lambda x: x['pageDepth'] * x['visits'], axis=1)
        dataset = data.groupby('key_type').sum()\
        .sort_values('visits', ascending=False).reset_index()
        
        dataset = expand_func(dataset)    
        dataset = dataset[['key_type', 'visits', 'depth', 'duration', 'conversions', 'ctr']] 
        
        dataset = dataset[dataset['key_type'] != 'не определено']        
        
        params = [['ctr', 80, 45], ['duration', 55, 30], ['depth', 20, 10]]
        ue = Universal_Estimate (dataset, params)
        dataset = ue.data
        
        #Выгрузка отчета
        lc = ListConventer(dataset.head(20),
                            'width_headers')
        Googlesheets('data organic!i18:z100', lc.datsheets, client)        
  