import pandas as pd
import os
#Игнорирование предупреждений
import warnings
warnings.filterwarnings("ignore")
#Импорт графиков
from scripts.plot_scripts.basegraphs import *
from scripts.estimatelib.universal_estimate import Universal_Estimate
#Подключение гугл таблиц
from scripts.filemanager.googlesheets_upload_lib import Googlesheets  
from scripts.filemanager.googlesheets_upload_lib import ListConventer
#Допфункции
from functions.basic_func import *
#Подключение оболочки отчетов
from reports.data_mixin import *

class SMM_utm:
    def __init__ (self, client):
        self.client = client
        #Подготовка данных
        class GetData (GetMixinGenerateBigData):
            client = self.client   
            deep = 1
            dataset_name = 'general_utm_analytics'    
        gt = GetData()
        data = gt.data       
        data = data.fillna('Нет данных')
        self.data = data
        
        if len(data) > 0:
            self.go()
        else:
            print ('УТМ-меток для генерации отчета не найдено')
        
    def go(self):
        
        data = self.data
        
        def not_no_data_func (utm_data):
            if utm_data != 'Нет данных':
                return utm_data
            else:
                return ''

        def identify_func (row):
            return not_no_data_func(str(row['UTMSource'])) + ' '+\
            not_no_data_func(str(row['UTMMedium'])) + ' '+\
            not_no_data_func(str(row['UTMCampaign'])) + ' '+\
            not_no_data_func(str(row['UTMContent'])) + ' '+\
            not_no_data_func(str(row['UTMTerm']))
        
        data['ads'] = data.apply(identify_func, axis=1)
        
        self.list = data['UTMSource'].unique()
        
        #SMM-отчет     
        social_utms = ['fb', 'facebook', 'vk', 'vkontakte', 
                       'mt', 'my', 'mytarget', 'inst', 'instagram', 'ok']

        def not_social_utm_func (row):
            if row['UTMSource'] not in social_utms:
                return 0
            return row['UTMSource']

        data['UTMSource'] = data.apply(not_social_utm_func, axis=1)
        data = data[data['UTMSource'] != 0]
        if len(data) == 0:
            print ('По заявленным УТМ-меткам для SMM не найдено данных')
        else:
            self.social_media = data
            self.analytics()
            
    def analytics (self):
        social_media = self.social_media
        client = self.client
        social_media['sumbounce'] = \
        social_media.apply (lambda x: x['visits']*x['bounceRate'],
                            axis=1)

        social_media['sumdepth'] = \
        social_media.apply (lambda x: x['visits']*x['pageDepth'],
                            axis=1)

        social_media['sumduration'] = \
        social_media.apply (lambda x: x['visits']*x['avgVisitDurationSeconds'], axis=1)

        social_media = social_media[['ads', 'visits', 'sumbounce', 
                      'sumdepth','sumduration',
                      'conversions', ]]

        social_media = social_media.groupby('ads').sum().\
                        sort_values('visits', ascending=False).reset_index()

        social_media = expand_func(social_media)

        #Фильтруем слабые и ошибочные данные
        if social_media['visits'][0] > 10:
            social_media = social_media[social_media['visits'] > 10].head(15)
        
        social_media['goodpercent'] = social_media.apply\
        (lambda x: 100-x['bounce'], axis=1)
    
        try:
            params = [['ctr', 80, 45], 
                      ['duration', 55, 30], 
                      ['depth', 20, 10],
                     ['goodpercent', 60, 30]]
            ue = Universal_Estimate (social_media, params)
            social_media = ue.data
        except:
            None          

        lc = ListConventer(social_media.head(5|0),
                               'width_headers')
        Googlesheets('data utm!a1:z999', lc.datsheets, client)        