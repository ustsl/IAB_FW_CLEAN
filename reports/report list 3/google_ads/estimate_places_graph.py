from reports.data_mixin import *
from scripts.filemanager.filemanager import *
import pandas as pd
import os
import warnings
warnings.filterwarnings("ignore")
from scripts.plot_scripts.basegraphs import *


class GetData:
    
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
            dataset_name = 'google_ads_places'
            deep = 1
        gt = GetData()
        data = gt.data  
        self.data = data.fillna('Нет данных')
        
        
class Ads_Places_Analytic:
    def __init__ (self, client):
        
        gt = GetData (client)
        data = gt.data
        dataset = data[data['ga:sessions'] > 10]
        
        if len(dataset) > 0:
            self.start_report(dataset, client)
        
    def start_report(self, dataset, client):
        
        def quality_estimation_func (row):
            if row['ga:bounces'] > row['ga:sessions'] / 1.1 \
            or int(row['ga:avgSessionDuration']) < 25:
                return 'Плохое качество'
            return 'Хорошая площадка'
                

        dataset['estimation'] = dataset.apply(quality_estimation_func, axis=1)
        estimate_places = dataset.groupby('estimation').sum().\
                        sort_values('ga:sessions', ascending=False).reset_index()
        
        GraphColumns (client, estimate_places, 'ga:sessions',
                      "Объем показов Adwords на площадках в последнем отчетном месяце",
                      '# Визиты',
                      'strategy_google_ads_bad_good_places.png', 'google_ads', 'sort')

        bad_place_list = \
        list(dataset[dataset['estimation'] == 'Плохое качество']\
             ['ga:adPlacementDomain'])    

        string_place = ', '.join( bad_place_list ) 
        txt = TXT_saver (client, 'data/'+client)
        txt.save('худшие площадки показа рекламы в эдвордс квартал.txt', string_place)
        print ('Завершено')
       
        self.dataset= dataset