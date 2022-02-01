import pandas as pd
import os
#Игнорирование предупреждений
import warnings
warnings.filterwarnings("ignore")
#Импорт графиков
from scripts.plot_scripts.basegraphs import *

#Подключение гугл таблиц
from scripts.filemanager.googlesheets_upload_lib import Googlesheets  
from scripts.filemanager.googlesheets_upload_lib import ListConventer

#Допфункции
from functions.basic_func import *
#Подключение оболочки отчетов
from reports.data_mixin import *


class CityStatReport:
    def __init__ (self, client):
        self.client = client

        #Подготовка данных
        class GetData (GetMixinGenerateBigData):
            client = self.client
            deep = 3
            dataset_name = 'city_stat'

        gt = GetData()
        data = gt.data
        data = data.fillna('Нет данных')
        data = \
        data.groupby('regionCity').sum().sort_values('visits', ascending=False).reset_index()
        self.data = data
        #Фильтр лишних значений
        df = \
        data[(data['conversions'] != 0) &\
             (data['visits'] > data.visits.median()) &\
             (data['regionCity'] != 'Нет данных')]
        df = df.head(12)
        df = df[['regionCity', 'visits', 'conversions']]
        df['visits'] = df.apply(lambda x: int(x['visits']), axis=1)
        df['conversions'] = df.apply(lambda x: int(x['conversions']), axis=1)
        df['ctr'] = df.apply(ctr_inner_func, axis=1)
        
        #Выгрузка актуалки
        lc = ListConventer(df,'width_headers')     
        maxlonglist = 100
        maxwidthlist = 7
        width = [''] * maxwidthlist
        maxlist = [width] * maxlonglist    
        Googlesheets('data geo!a1:z100', maxlist, client)  
        Googlesheets('data geo!a1:z100', lc.datsheets, client)    
        
        SquarifyGraph(client, df, 'ctr',
              'Квартальный CTR по городам',
              'strategy_second_city_month_stat_ctr.png', 'city_stat')