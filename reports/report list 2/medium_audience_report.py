#объект получения данных
import os
from yaml import load
import pandas as pd
#Импорт графиков
from scripts.plot_scripts.basegraphs import *
#объект получения данных
from scripts.report_scripts.data_mixin import GetMixinGenerateBigData
#Подключение системы оценок
from scripts.estimatelib.universal_estimate import Universal_Estimate
#Подключение гугл таблиц
from scripts.filemanager.googlesheets_upload_lib import Googlesheets  
from scripts.filemanager.googlesheets_upload_lib import ListConventer




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
            dataset_name = 'audience_dataset'
            deep = 3
        gt = GetData()
        dataset = gt.data
        dataset['sumduration'] = \
        dataset.apply(lambda x: x['visits'] * x['avgVisitDurationSeconds'], 
                                                  axis=1)
        dataset['sumdepth'] = \
        dataset.apply(lambda x: x['visits'] * x['pageDepth'], axis=1)
        
        dataset['sumbounce'] =\
        dataset.apply(lambda x: x['visits'] * x['bounceRate'], axis=1)       

        self.dataset = dataset.fillna('Нет данных')        


class AudienceReport:
    def __init__ (self, client):

        def interest_func(row):
            string = \
            row['interest2d1'] + ' ' + \
            row['interest2d2'] + ' ' + \
            row['interest2d3']
            string = string.replace('Нет данных', '')
            string = string.replace('  ', '').strip()
            if len(string) > 3:
                return string
            return 'Нет данных'

        def expand_func (data):
            data['duration'] = data.apply(lambda x: int(x['sumduration']/ x['visits'] * 100) / 100,
                                                  axis=1)
            data['depth'] = data.apply(lambda x: int(x['sumdepth']/ x['visits'] * 100) / 100, 
                                                       axis=1)

            data['bounce'] = data.apply(lambda x: int(x['sumbounce'] / x['visits'] * 100) / 100, 
                                                       axis=1)   
            data['visits'] = data.apply(lambda x:int(x['visits']), axis=1)

            data['goodpercent'] = data.apply(lambda x: int(100 - x['bounce']), axis=1)

            data['ctr'] = data.apply(lambda x: int(x['conversions'] / x['visits'] * 10000) / 100, axis=1)

            return data[[data.columns[0], 'visits', 'duration', 
                         'depth', 'ctr', 'goodpercent', 'conversions']]




        dp = DataPrepare(client)
        data = dp.dataset

        #Подготовка данных по интересам
        data['interest'] = data.apply(interest_func, axis=1)
        interest_data = data.groupby('interest').sum().\
        sort_values('visits', ascending=False).reset_index()

        #Подготовка данных по гендеру
        data['age_gender'] = \
        data.apply(lambda x: x['gender'] + ' ' + x['ageInterval'], axis=1)
        gender_data = data.groupby('age_gender').sum().\
        sort_values('visits', ascending=False).reset_index()


        #Параметры для оценочной системы
        params = [['ctr', 80, 45], 
                  ['duration', 55, 30], 
                  ['depth', 20, 10], 
                  ['goodpercent', 70, 50]]


        #Параметры очистки листа
        maxlonglist = 900
        maxwidthlist = 9
        width = [''] * maxwidthlist
        maxlist = [width] * maxlonglist

        #Оцениваем по интересам
        idata = expand_func(interest_data.head(40))
        ue = Universal_Estimate (idata, params)


        #Выгрузка данных
        lc = ListConventer(ue.data, 'width_headers')
        lc.conventer()
        Googlesheets('data audience!a1:z900', maxlist, client)
        Googlesheets('data audience!a1:z900', lc.datsheets, client)    


        #Оцениваем по гендеру
        gdata = expand_func(gender_data.head(40))
        ue = Universal_Estimate (gdata, params)

        #Выгрузка отчета
        lc = ListConventer(ue.data, 'width_headers')
        lc.conventer()        
        Googlesheets('data audience!k1:z900', maxlist, client)
        Googlesheets('data audience!k1:z900', lc.datsheets, client)    