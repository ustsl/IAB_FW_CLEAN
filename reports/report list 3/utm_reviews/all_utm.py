import pandas as pd
import os
#Игнорирование предупреждений
import warnings
warnings.filterwarnings("ignore")
#Импорт графиков
from scripts.plot_scripts.basegraphs import *
from scripts.estimatelib.universal_estimate import Universal_Estimate
#Подключение гугл таблиц
from scripts.filemanager.googlesheets_upload_lib import Googlesheets  
from scripts.filemanager.googlesheets_upload_lib import ListConventer
#Допфункции
from functions.basic_func import *
#Подключение оболочки отчетов
from reports.data_mixin import *

class ALL_UTM:
    def __init__ (self, client):
        self.client = client
        #Подготовка данных
        class GetData (GetMixinGenerateBigData):
            client = self.client   
            deep = 1
            dataset_name = 'general_utm_analytics'    
        gt = GetData()
        data = gt.data       
        data = data.fillna('Нет данных')
        self.data = data
        
        if len(data) > 0:
            self.go()
        else:
            print ('УТМ-меток для генерации отчета не найдено')
        
    def go(self):
        
        data = self.data
        
        def not_no_data_func (utm_data):
            if utm_data != 'Нет данных':
                return utm_data
            else:
                return ''

        def identify_func (row):
            return not_no_data_func(str(row['UTMSource'])) + ' '+\
            not_no_data_func(str(row['UTMMedium'])) + ' '+\
            not_no_data_func(str(row['UTMCampaign']))
         
        data['ads'] = data.apply(identify_func, axis=1)
        data = data.groupby('ads').sum().\
                        sort_values('visits', ascending=False).reset_index()
        self.data = data
        
        data['sumbounce'] = \
        data.apply (lambda x: x['visits']*x['bounceRate'], axis=1)

        data['sumdepth'] = \
        data.apply (lambda x: x['visits']*x['pageDepth'], axis=1)

        data['sumduration'] = \
        data.apply (lambda x: x['visits']*x['avgVisitDurationSeconds'], axis=1)

        data = expand_func(data)

        params = [['ctr', 80, 45], ['duration', 55, 30], ['depth', 20, 10], 
                  ['conversions', 70, 50], ['clean_conversions', 80, 60]]
        ue = Universal_Estimate (data, params)
        data = ue.data

        data = data[['ads', 'visits', 'conversions', 'clean_conversions', 'ctr', 'estimate']]
        #Выгрузка отчета
        lc = ListConventer(data.head(20),
                           'width_headers')
        Googlesheets('data all utm!a1:z100', lc.datsheets, self.client)  