import os
import pandas as pd
import warnings
warnings.filterwarnings("ignore")
from scripts.plot_scripts.basegraphs import *
from scripts.filemanager.filemanager import *
from scripts.estimatelib.universal_estimate import *
#Допфункции
from functions.basic_func import *
#Подключение оболочки отчетов
from reports.data_mixin import *



class DataPrepare:
    
    def __init__ (self, client):
        self.client = client

        #Подкласс получения данных

        class GetData (GetMixinGenerateBigData):    
            client = self.client
            dataset_name = 'yandex_direct_creatives_and_places'
            deep = 1
        gt = GetData()
        dataset = gt.data
        
        #Предобработка полученного    
        #Убираем пустые данные, склеиваем урлы, перемножаем средние параметры
        
        dataset = dataset.fillna('Нет данных')         
        dataset['sumduration'] = dataset.apply(sumdur_func, axis=1)
        dataset['sumdepth'] = dataset.apply(sumdepth_func, axis=1)   
        
        self.dataset = dataset
  

class Direct_Place_Analytics:
    
    def __init__ (self, client):
        
        self.client = client
        
        #Общие вычисления        
        dp = DataPrepare (client)
        dataset = dp.dataset        
        dataset = dataset[dataset['<attribution>DirectPlatformType'] == 'Сети']
        
        if  len(dataset) == 0:
            print ('Данных нет. РСЯ на клиенте отсутствует')
            # Исключение срабатывает в том случае, если по фильтру "Сети" нет РК
        else:
            self.dataset = dataset
            self.analytics()
            print ('Отчет Direct_Place_Analytics успешно завершен') 
            
    def analytics (self):   
        
        dataset = self.dataset
        client = self.client

        dataset = dataset.groupby('<attribution>DirectPlatform').sum().\
                sort_values('visits', ascending=False).reset_index()

        dataset = expand_func(dataset)  
        params = [['depth', 35, 20], ['ctr', 85, 50], ['duration', 70, 40], ['conversions', 80, 50]]
        ue = Universal_Estimate (dataset, params)
        dataset = ue.data
        self.dataset = dataset[dataset['visits'] >= 50]
        

        if len(dataset) > 0:        
            self.badplaces()
            self.estimate_places_graph()
            
        else:
            print ('Недостаточно данных для анализа площадок')

    def badplaces (self):
        
        dataset = self.dataset
        client = self.client
        
        bad_place_list = \
        list(dataset[dataset['estimate'] < 2]\
             ['<attribution>DirectPlatform'])    
        
        if len(bad_place_list) > 0:
            string_place = ', '.join( bad_place_list ) 
            txt = TXT_saver (client, 'data/'+client)
            txt.save('худшие_площадки_показа_рекламы_в_директе_квартал.txt', string_place)
            print ('Завершено')
        else:
            print ('Плохих площадок не обнаружено')
            
        
    def estimate_places_graph(self):
        
        dataset = self.dataset
        dataset['estimate'] = dataset.apply(lambda x: str(int(x['estimate'])), axis=1)
        estimate_places = self.dataset.groupby('estimate').sum().\
                sort_values('visits', ascending=False).reset_index()

        GraphColumns (self.client, estimate_places, 'visits',
                      "Оценка объема трафика и площадок по пятибальной шкале",
                      '# Визиты',
                      'strategy_bad_good_places.png', 'yandex_direct', 'sort')
        