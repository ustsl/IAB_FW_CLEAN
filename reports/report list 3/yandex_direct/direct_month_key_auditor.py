import pandas as pd
import os
#Игнорирование предупреждений
import warnings
warnings.filterwarnings("ignore")
#Импорт графиков
from scripts.plot_scripts.basegraphs import *
from scripts.keymasterlib.keymaster import *
from scripts.filemanager.filemanager import *
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
            dataset_name = 'yandex_direct_audience_and_keys'
            deep = 1
        gt = GetData()
        dataset = gt.data        
        
        dataset = dataset.fillna('Нет данных')        

            
        dataset['sumduration'] = \
        dataset.apply(sumdur_func, axis=1)
        dataset['sumdepth'] = \
        dataset.apply(sumdepth_func, axis=1)
        dataset['sumbounce'] = \
        dataset.apply(sumbounce_func, axis=1)
        
        self.dataset = dataset
        
        


class Yandex_Direct_key_auditor:
    def __init__ (self, client):    
        
        dp = DataPrepare(client)
        print ('Данные получены')
        
        def duration_func(data):
            
            data['duration']= \
            data.apply(lambda x: x['sumduration'] / x['visits'], axis=1)

            data['depth']= \
            data.apply(lambda x: x['sumdepth'] / x['visits'], axis=1)

            data['bounce']= \
            data.apply(lambda x: x['sumbounce'] / x['visits'], axis=1)

            data['goodvisits'] =\
            data.apply(lambda x: 100 - int(x['bounce']), axis=1)



            return data[[data.columns[0], 'visits', 'duration', 
                         'goodvisits', 'depth', 'conversions']]

        data = dp.dataset

        data = data.groupby('<attribution>DirectClickOrder').sum().\
                sort_values('visits', ascending=False).reset_index()
        
        data = duration_func(data)

        data['ctr'] = data.apply(ctr_inner_func, axis=1)


        try:
            params = [['ctr', 80, 45], ['duration', 55, 30], 
                      ['depth', 20, 10], ['goodvisits', 65, 35]]
            ue = Universal_Estimate (data, params)
            data = ue.data
        except:
            None     
    
    
        data.to_excel\
        (os.path.join('data/'+client,
                      'яндекс_директ в последнем месяце - кампании.xlsx'),
                    sheet_name='er')

        data = dp.dataset
        data['ads'] = data.apply\
        (lambda x: \
         x['<attribution>DirectClickOrder'] + ' | '\
         + x['<attribution>DirectBannerGroup'] +' | '\
         + x['<attribution>DirectClickBanner'],
         axis=1)
        data = data.groupby('ads').sum().\
                sort_values('visits', ascending=False).reset_index()
        data = duration_func(data)

        data['ctr'] = data.apply(ctr_inner_func, axis=1)


        try:
            params = [['ctr', 80, 45], ['duration', 55, 30], 
                      ['depth', 20, 10], ['goodvisits', 65, 35]]
            ue = Universal_Estimate (data, params)
            data = ue.data
        except:
            None     
    
        data.to_excel(os.path.join('data/'+client,
                                 'яндекс_директ в последнем месяце - группы.xlsx'),
                    sheet_name='er')

        #Отсюда начинается анализатор ключевиков, преобразующий данные в перевариваемую таблицу

        data = dp.dataset
        data['phrase'] = data['<attribution>DirectSearchPhrase']
        data = data[['ads', 'phrase', 'visits', 'conversions']]
        data['ctr'] = data.apply(ctr_inner_func, axis=1)

        data = data[data['phrase'] != 'Нет данных']

        goodlist = []

        for ph in list(data[data['ctr'] > 0]['phrase']):
            phr = ph.split(' ')
            for word in phr:
                if len(word) > 2:
                    goodlist += [word]            

        bads = data[(data['ctr'] < 1) & (data['visits'] > 10)]
        try:
            bads['split'] = bads.apply(lambda x: x['phrase'].split(' '), axis=1)
        except: 
            print ('Отчет по ключам не сработал')
            bads['split'] = []

        def bads_func(row):
            try:
                row_list = row['split']
                bads_string = []
                for word in row_list:
                    if len(word) > 2 and word not in set(goodlist):            
                        bads_string += [word]    
                return ', '.join(bads_string)
            except:
                print ('Плохие ключи не обнаружены')
                return ''

        bads['bads'] = bads.apply(bads_func, axis=1)

        bads[bads['bads'] != ''][['ads', 'bads', 'visits']]\
        .to_excel(os.path.join('data/'+client,
                               'яндекс_директ список плохих ключей с навигацией.xlsx'),
                  sheet_name='er')     
        print ('Отчет Yandex_Direct_key_auditor успешно завершен')