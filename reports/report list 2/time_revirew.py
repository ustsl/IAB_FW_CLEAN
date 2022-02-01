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
from functions.basic_func import ctr_inner_func
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
            dataset_name = 'ads_timer'
            deep = 1
        gt = GetData()
        dataset = gt.data
        dataset = dataset.fillna('Нет данных')            
        remove_list = []
        for x in list(dataset):
            if 'goal' in x:
                del dataset[x]                
        self.dataset = dataset      
        
class Time_Estimate:
    
    def __init__ (self, client):
        self.client = client
        dp = DataPrepare (client)
        dataset = dp.dataset
        #Очистка данных от неразмеченных переходов
        dataset = dataset[(dataset['UTMSource'] != 'Нет данных') &\
                (dataset['UTMCampaign'] != 'Нет данных') &\
                (dataset['UTMCampaign'] != '{campaign_id}')] 
        self.dataset = dataset 
        if len(dataset) > 0:
            self.go()
        else:
            print ('Нет данных для формирования отчета')        
        
        
        
    def go (self):
        
        dataset = self.dataset
        client = self.client
        #Стартовая группировка
        week = dataset.groupby('dayOfWeek').sum().\
                sort_values('visits', ascending=False).reset_index()
        hour = dataset.groupby('hour').sum().\
                sort_values('visits', ascending=False).reset_index()
        
        def clean_hour(row):
            return int(row['hour'].split(":")[0])
        
        hour['hour'] = hour.apply(clean_hour, axis=1)
        hour = hour.sort_values('hour', ascending=True)        
        
        self.week = week
        self.hour = hour

        def ctr_func (data):
            data['ctr'] = data.apply(ctr_inner_func, axis=1)
            return data

        #Общие срезы
        week = ctr_func (week)
        hour = ctr_func (hour)      

        reference = dataset.conversions.sum() / dataset.visits.sum() * 100
        dataset['ads'] = dataset.apply(lambda x: x['UTMSource'] + ' ' + x['UTMCampaign'], axis=1)
        if dataset.visits.max() > 150:
            dataset = dataset[dataset['visits'] > 50]
        elif dataset.visits.max() > 50:
            dataset = dataset[dataset['visits'] > 20]
        else:
            dataset = dataset[dataset['visits'] > 1]            
        
        df = ctr_func (dataset)
        print (reference)        
        

        
        def estimate_func (row):
            if row['ctr'] < reference / 1.5:
                return 'low'
            elif row['ctr'] > reference * 1.5:
                return 'hight'
            elif row['ctr'] < reference:
                return 'low_medium'
            else:
                return 'good'

        df['estimate'] = df.apply(estimate_func, axis=1)
        df.to_excel(os.path.join('data/'+client,
                                 'оценка по таймингу и дням недели.xlsx'),
                    sheet_name='er')    

        #Выгрузка отчета
        lc = ListConventer(hour,
                           'width_headers')
        Googlesheets('time!a1:f50', lc.datsheets, self.client) 


        #Выгрузка отчета
        lc = ListConventer(week,
                           'width_headers')
        Googlesheets('time!h1:z50', lc.datsheets, self.client) 
