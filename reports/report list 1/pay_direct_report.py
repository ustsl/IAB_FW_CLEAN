from scripts.report_scripts.data_mixin import *
import pandas as pd
#Подключение гугл таблиц
from scripts.filemanager.googlesheets_upload_lib import Googlesheets  
from scripts.filemanager.googlesheets_upload_lib import ListConventer

"""
Базовый отчет по Расходам в Яндекс Директ
"""

class DataPrepare:    
    def __init__ (self, client):
        self.client = client
        self.work()
        
    def work (self):
        
        """
        Подкласс получения данных
        """
        class GetData (GetMixinGenerateBigData):
            client = self.client   
            dataset_name = 'yandex_direct_campaign_report'   
            deep = 12
        gt = GetData()
        self.data = gt.data
        


class PayDirectReport:
    
    def __init__ (self, client):
        self.client = client
        dp = DataPrepare (client)
        self.data = dp.data
        self.go_report()
    def go_report(self):
        
        data = self.data
        client = self.client

        #Базовая подготовка данных

        def campaign_func (row):
            split = row['CampaignName'].split('.')
            if len(split) > 1:
                return split[1]
            return split[0]

        def type_func (row):
            return row['CampaignName'].split('.')[0]

        data['Campaign'] = data.apply(campaign_func, axis=1)
        data['Type'] = data.apply(type_func, axis=1)
        data['SumCTR'] = data.apply (lambda x: x['Ctr'] * x['Clicks'], 
                                     axis=1)
        data['SumCRate'] = data.apply (lambda x: x['ConversionRate'] * x['Clicks'], 
                                       axis=1)
        
        #Базовый отчет
        data_longtime = data.groupby('startOfMonth').sum().\
        sort_values('startOfMonth', ascending=True).reset_index()

        def expand_func(data):    

            data['Ctr'] = \
            data.apply(lambda x: int(x['SumCTR'] / x['Clicks'] * 100) / 100 \
                       if x['SumCTR'] != 0\
                       else 0, axis=1)
            data['ConversionRate'] = \
            data.apply(lambda x: int(x['SumCRate'] / x['Clicks'] * 100) / 100 \
                       if x['SumCRate'] !=0\
                       else 0, axis=1)

            return data

        data_longtime = expand_func(data_longtime)
        data_longtime = data_longtime[['startOfMonth', 'Impressions', 'Clicks', 'Ctr',
                                          'Campaign_cost', 'ConversionRate']]        
        
        #Отчет по типам трафика
        data_type = data.groupby(['startOfMonth', 'Type'])\
        .sum().sort_values('startOfMonth', ascending=True).reset_index()


        data_type = data_type\
        [ (data_type['Type']=='Поиск') | \
          (data_type['Type']=='РСЯ') | \
         (data_type['Type']=='Баннер на поиске')]

        data_type = expand_func(data_type)
        data_type = data_type[['startOfMonth', 'Type', 'Impressions', 'Clicks', 'Ctr',
                                          'Campaign_cost', 'ConversionRate']]   
        
        #Отчет по кампаниям
        data_campaign = data.groupby(['startOfMonth', 'Campaign'])\
        .sum().sort_values('startOfMonth', ascending=True).reset_index()

        data_campaign = expand_func(data_campaign)
        data_campaign = data_campaign[['startOfMonth', 'Campaign', 
                                       'Impressions', 'Clicks', 'Ctr',
                                          'Campaign_cost', 'ConversionRate']]   
        
        
        
        lc = ListConventer(data_longtime,
                           'width_headers')
        lc.conventer()

        maxlonglist = 14
        maxwidthlist = 6
        width = [''] * maxwidthlist
        maxlist = [width] * maxlonglist

        Googlesheets('data pay direct!a1:f14', maxlist, client)  
        Googlesheets('data pay direct!a1:f14', lc.datsheets, client)  



        lc = ListConventer(data_type,
                           'width_headers')
        lc.conventer()

        maxlonglist = 60
        maxwidthlist = 6
        width = [''] * maxwidthlist
        maxlist = [width] * maxlonglist

        Googlesheets('data pay direct!h1:z60', maxlist, client)  
        Googlesheets('data pay direct!h1:z60', lc.datsheets, client)  


        lc = ListConventer(data_campaign,
                           'width_headers')
        lc.conventer()

        maxlonglist = 700
        maxwidthlist = 7
        width = [''] * maxwidthlist
        maxlist = [width] * maxlonglist

        Googlesheets('data pay direct!a18:g800', maxlist, client)  
        Googlesheets('data pay direct!a18:g800', lc.datsheets, client)
