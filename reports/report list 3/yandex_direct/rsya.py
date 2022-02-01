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
        
        #Подкласс получения данных

        class GetData (GetMixinGenerateBigData):    
            client = self.client
            dataset_name = 'yandex_direct_audience_and_keys'
            deep = 1
            
        gt = GetData()
        dataset = gt.data
        dataset = dataset.fillna('Нет данных')            
        dataset['sumduration'] = dataset.apply(sumdur_func,  axis=1)
        dataset['sumdepth'] = dataset.apply(sumdepth_func, axis=1)
        self.dataset = dataset
        
        
class Data_direct_RSYA:
    
    def __init__ (self, client):
        
        dp = DataPrepare (client)
        data = dp.dataset
        self.client = client
        self.data = data
        try:
            self.go()
        except:
            print ('Ошибка на отчете РСЯ. Клиент - {}'.format(client))
            
    def go (self):
        
        data = self.data
        client = self.client
        
        rsdata = data [ data['<attribution>DirectClickOrder'].str.contains('РСЯ', 
                                                                           case=False) ]

        rsdata['ads'] = rsdata.apply(lambda x: x['<attribution>DirectBannerGroup'] +\
                                    ' | ' + x['<attribution>DirectClickBanner'], axis=1)

        def night_func (row):
            if 'ночь' in row['<attribution>DirectClickOrder'].lower():
                return row['ads'] + ' | ' + 'ночь'
            else:
                return row['ads'] + ' | ' + 'день'

        rsdata['ads'] = rsdata.apply(night_func, axis=1)

        #Расчет по объявлениям

        df = rsdata.groupby('ads').sum().\
        sort_values('visits', ascending=False).reset_index()

        df = expand_func (df)

        df = df[df['visits'] > 10]
        params = [['clean_conversions', 90, 60],['ctr', 80, 45], 
                  ['duration', 55, 30], ['depth', 20, 10]]
        ue = Universal_Estimate (df, params)
        df = ue.data

        df = df[['ads', 'visits', 'conversions', 'ctr', 'clean_conversions', 'estimate']]\
        .to_excel\
        (os.path.join('data/'+client, 'директ месяц рся объявления.xlsx'), sheet_name='er')

        #Расчет по условиям показа

        df = rsdata.groupby('<attribution>DirectPhraseOrCond').sum().\
        sort_values('visits', ascending=False).reset_index()
        df = expand_func (df)
        df = df[df['visits'] > 10]
        params = [['clean_conversions', 90, 60],['ctr', 80, 45], 
                  ['duration', 55, 30], ['depth', 20, 10]]
        ue = Universal_Estimate (df, params)
        df = ue.data

        df[['<attribution>DirectPhraseOrCond', 'visits', 
            'conversions', 'ctr', 'clean_conversions', 'estimate']]\
        .to_excel\
        (os.path.join('data/'+client, 
                      'директ месяц рся условия показа.xlsx'), 
         sheet_name='er')