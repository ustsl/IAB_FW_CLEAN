import pandas as pd
from reports.data_mixin import *
from scripts.estimatelib.universal_estimate import Universal_Estimate
import os
from scripts.filemanager.filemanager import *
#Подключение гугл таблиц
from scripts.filemanager.googlesheets_upload_lib import Googlesheets  
from scripts.filemanager.googlesheets_upload_lib import ListConventer

class GetData:
    
    def __init__ (self, client):
        self.client = client
        self.work()
        
    def work (self):        
        """
        Подкласс получения данных
        """
        class GetData (GetMixinGenerateBigData):
            client = self.client   
            dataset_name = 'google_ads_keys'
            deep = 1
        gt = GetData()
        data = gt.data        
        self.data = data.fillna('Нет данных')
        
        
class Ads_Group_analyzer:
    def __init__ (self, client):
        self.client = client
        try:
            gt = GetData(client)
            self.data = gt.data
            self.go()
        except OSError:
            print ('Отчет не сработал. Возможно нет данных')
            
    def go (self):
        client = self.client
        data = self.data
        data['ads_claster'] = data.apply\
        (lambda x: str(x['ga:adwordsCampaignID']) + ' | ' + str(x['ga:adGroup']), axis=1)
        data = data.groupby('ads_claster').sum().\
        sort_values('ga:adClicks', ascending=False).reset_index()\
        [['ads_claster', 'ga:adClicks', 'ga:adCost', 'conversions']]

        def ctr_func (row):
            if row['ga:adClicks'] > 0:
                return int(row['conversions'] / row['ga:adClicks'] * 10000) / 100
            else:
                return 0

        data['ctr'] = data.apply(ctr_func, axis=1)
        params = [['conversions', 99, 60], ['ctr', 80, 45]]
        ue = Universal_Estimate (data, params)
        df = ue.data        
        #Выгрузка отчета в гугл таблицы
        lc = ListConventer(df.head(50), 'width_headers')
        self.df = df.head (50)
        maxlonglist = 900
        maxwidthlist = 6
        width = [''] * maxwidthlist
        maxlist = [width] * maxlonglist
        Googlesheets('ads medium report!a1:f900', maxlist, client)
        Googlesheets('ads medium report!a1:f900', lc.datsheets, client)           
        df.to_excel(os.path.join('data/'+client, 'оценка групп объявлений в гугл эдс.xlsx'), 
                    sheet_name='er')