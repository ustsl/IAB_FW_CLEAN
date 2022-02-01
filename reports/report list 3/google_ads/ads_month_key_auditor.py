import pandas as pd
from reports.data_mixin import *
from scripts.keymasterlib.wordestimate import WordEstimate
import os
from scripts.filemanager.filemanager import *

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
        
class Google_Ads_key_auditor:
    def __init__ (self, client):
        self.client = client      
        gt = GetData(client)
        data = gt.data

        data['ads'] = data.apply(lambda x: str(x['ga:adwordsCampaignID']) + ' ' + \
                                x['ga:adGroup'], axis=1)
        data['phrase'] = data['ga:adMatchedQuery']
        data = data[data['ga:adClicks'] > 0]     
        self.data = data
        if len(data) > 0:
            self.go()
        else:
            print ('Нет данных для отчета')       
        
    def go(self):
        client = self.client
        data = self.data

        def ctr_func(row):
            if row['ga:adClicks'] > 0:
                return int(row['conversions'] / row['ga:adClicks'] * 10000) / 100
            else:
                return 0


        data['ctr'] = data.apply(ctr_func, axis=1)



        data = data[data['phrase'] != 'Нет данных']

        goodlist = []

        for ph in list(data[data['ctr'] > 0]['phrase']):
            phr = ph.split(' ')
            for word in phr:
                if len(word) > 2:
                    goodlist += [word]            

        bads = data[(data['ctr'] < 1) & (data['ga:adClicks'] > 10)]
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

        bads[bads['bads'] != ''][['ads', 'bads', 'ga:adClicks']]\
        .to_excel(os.path.join('data/'+client,
                               'гугл эдс список плохих ключей с навигацией.xlsx'),
                  sheet_name='er')     

        print ('Завершено')