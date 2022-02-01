import os
import pandas as pd
#объект получения данных из папок датасетов с до-генерацией файлов
from scripts.dataset_generator.data_generator import Generate_Bigdata_File
#объект предобработки сырых данных из апи метрики
from scripts.datareceiver.yandex_metrika.data_upload import Data_upload

"""
Миксин для генерации хранимых отчетов с выставленной глубиной.
"""

class GetMixinGenerateBigData:
    
    client = None #Имя клиента (нужно для определения рабочей папки с задасетами)
    dataset_name = None #Имя датасета (имя подставляется к пути нужной папки)
    deep = 12 #Глубина сборки датасета по месячным итерациям - как много нужно захватить месяцев
    
    def __init__ (self): 
        gbf = Generate_Bigdata_File (self.client, self.dataset_name, self.deep)
        self.data = gbf.data
        
        
"""
Пример использования:

class DataPrepare:    
    def __init__ (self, client):
        self.client = client
        self.work()
        
    def work (self):

        class GetData (GetMixinGenerateBigData):
            client = self.client   
            dataset_name = 'yandex_direct_campaign_report'   
            deep = 12
        gt = GetData()
        self.data = gt.data
"""       

class GetMixinDataUpload:    
    # Миксин библиотеки ДатаАплоад Яндекс Директа с предрасчетом sum
    dimensions = None
    metrics = None
    client = None
    date = None
    preset = None
    
    def __init__ (self):
        
        print ('Старт сборки')
        
        if self.dimensions == self.dimensions:
            preset = [self.dimensions, self.metrics]       
        else:
            preset = [self.preset]
        
        params = {'client': self.client, 
                'date': self.date, 
                'preset': preset, 
                'filters': None}
        
        #Запрос
        du = Data_upload (params)
        dataset = du.dataset
        if 'pageDepth' in dataset.columns and 'avgVisitDurationSeconds' in dataset.columns:

            dataset['sumduration']\
            = dataset.apply(lambda x: x['visits'] * x['avgVisitDurationSeconds'], 
                                                      axis=1)
            dataset['sumdepth']\
            = dataset.apply(lambda x: x['visits'] * x['pageDepth'], 
                                                      axis=1)
        dataset = dataset.fillna('Нет данных')
        self.data = dataset
       