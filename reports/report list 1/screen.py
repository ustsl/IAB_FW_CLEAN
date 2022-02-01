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

class DataPrepare:
    
    def __init__ (self, client):   
        self.client = client
        """
        Готовим даннные
        """        
        class GetData (GetMixinGenerateBigData):
            client = self.client   
            dataset_name = 'screen'
            deep = 12
        gt = GetData()
        dataset = gt.data
        #Убираем пустые данные, склеиваем урлы, перемножаем средние параметры
        dataset = dataset.fillna('Нет данных')            
        dataset['sumduration'] = dataset.apply(lambda x: x['visits'] * x['avgVisitDurationSeconds'], 
                                              axis=1)
        dataset['sumdepth'] = dataset.apply(lambda x: x['visits'] * x['pageDepth'], 
                                              axis=1)
        dataset['date'] = dataset.apply(lambda x: x['startOfMonth'][:-3], axis=1)        
        self.df = dataset

        """
        Окончание подготовки данных
        """        
        
class Screen_Report:
    
    def __init__ (self, client):
        
        dp = DataPrepare (client)
        data = dp.df
        data = data[['date', 'deviceCategory', 
             'operatingSystemRoot', 'physicalScreenResolution', 'browser',
            'visits', 'conversions', 'sumduration', 'sumdepth']]        
        header = ['date', 'deviceCategory']
        device = data.groupby(header).sum().reset_index()
        device = expand_func(device)

        #Выгрузка отчета
        lc = ListConventer(device,'width_headers')
        maxlonglist = 900
        maxwidthlist = 7
        width = [''] * maxwidthlist
        maxlist = [width] * maxlonglist
        Googlesheets('screen!a1:z999', maxlist, client)
        Googlesheets('screen!a1:z100', lc.datsheets, client)       


        def screen_split_func (row):
            try:
                return int((row['physicalScreenResolution'].split('x')[0]))
            except:
                0

        data['screen'] = data.apply(screen_split_func, axis=1)

        def clasters_func (row):
            if row['deviceCategory'] == 'ПК':
                if row['screen'] > 2000:
                    return 'ТОП ПК с высоким разрешением'
                elif row['screen'] > 1900:
                    return 'FULL HD ПК'
                else:
                    return 'ПК с низким разрешением'

            elif row['deviceCategory'] == 'Смартфоны' and row['operatingSystemRoot'] == 'iOS':
                if row['screen'] > 1000:
                    return 'iPhone'
                else:
                    return 'Старый iPhone'

            elif row['deviceCategory'] == 'Смартфоны' and \
            row['operatingSystemRoot'] == 'Google Android':
                if row['screen'] > 1000:
                    return 'Android'
                else:
                    return 'Старый Android'     

            return 'Планшеты и прочее'

        data['clasters'] = data.apply(clasters_func, axis=1)        
        header = ['date', 'clasters']
        clasters = data.groupby(header).sum().reset_index()
        del(clasters['screen'])
        clasters= expand_func(clasters)
        #Выгрузка отчета
        lc = ListConventer(clasters, 'width_headers')
        maxlonglist = 900
        maxwidthlist = 7
        width = [''] * maxwidthlist
        maxlist = [width] * maxlonglist
        Googlesheets('screen!i1:z999', maxlist, client)
        Googlesheets('screen!i1:z100', lc.datsheets, client)   