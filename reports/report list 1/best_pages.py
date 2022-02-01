import pandas as pd
import os
import warnings
warnings.filterwarnings("ignore")
from scripts.plot_scripts.basegraphs import *
from scripts.estimatelib.universal_estimate import Universal_Estimate
#Подключение гугл таблиц
from scripts.filemanager.googlesheets_upload_lib import Googlesheets  
from scripts.filemanager.googlesheets_upload_lib import ListConventer
#Допфункции
from functions.basic_func import *
#Подключение оболочки отчетов
from reports.data_mixin import *

class DataPrepare:
    
    def __init__ (self, client):
        self.client = client
        self.work()
        
    def work (self):        
        #Подкласс получения данных
        class GetData (GetMixinGenerateBigData):
            client = self.client   
            dataset_name = 'best_pages_stat'
            deep = 1
        gt = GetData()
        data = gt.data     
        #Предобработка полученного
        data = data.fillna('Нет данных')       
        data['sumduration'] = \
        data.apply(lambda x: x['avgVisitDurationSeconds'] * x['visits'], axis=1)
        data['sumdepth'] = data.apply(lambda x: x['pageDepth'] * x['visits'], axis=1)
        self.dataset = data


class Best_Pages_Report:
    
    def __init__ (self, client):
        dp = DataPrepare(client)
        data = dp.dataset
        data['startURL'] = data.apply(url_clean_func, axis=1)
        dataset = data.groupby(['startURL', 'bounce']).sum().\
        sort_values('visits', ascending=False).reset_index()
        clean = dataset[dataset.bounce == 'Не отказ']   
        clean['ctr'] = clean.apply(ctr_inner_func,  axis=1)
        clean['clean_ctr'] = clean.apply(clean_ctr_inner_func,  axis=1)
        clean['duration'] = clean.apply(duration_func, axis=1)
        clean = clean[clean.visits > 50]\
        .sort_values('visits', ascending=False)\
        .reset_index()[['startURL', 'visits', 'conversions', 
                        'clean_conversions', 'ctr', 'clean_ctr', 'duration']]
        bad = dataset[dataset.bounce == 'Отказ'][['startURL', 'visits']]
        bad.rename(columns={'visits': 'bounces'}, inplace=True)
        clean = clean.merge(bad, how = 'left', on = 'startURL')
        clean = clean.fillna(0)
        clean['badpercent'] = clean.apply\
        (lambda x: int(100 / (x['visits'] + x['bounces']) * x['bounces']), axis=1)
        clean['goodpercent'] = clean.apply(lambda x: 100 - x['badpercent'], axis=1)   
        
        #Делаем оценку качества
        params = [['goodpercent', 60, 40], ['clean_ctr', 90, 70], 
                  ['ctr', 70, 50], ['duration', 50, 30]]
        ue = Universal_Estimate (clean, params)
        clean = ue.data
        
        #Готовим данные к выгрузке
        self.data = clean[['startURL', 'visits', 'conversions', 'clean_conversions',
                           'clean_ctr', 'ctr', 'duration', 'goodpercent', 'estimate']]      

        #Выгрузка отчета
        lc = ListConventer(self.data.head(30),
                           'width_headers')
        Googlesheets('best pages!a1:z100', lc.datsheets, client)          