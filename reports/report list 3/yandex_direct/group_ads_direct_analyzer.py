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
        dataset['sumdepth'] = dataset.apply(sumdepth_func, axis=1)        
        self.dataset = dataset

        
class Group_Ads_Direct_Analyzer:
    def __init__ (self, client):

        dp = DataPrepare (client)
        data = dp.dataset

        def group_banner_func (row):
            if row['<attribution>DirectConditionType'] == 'Ключевая фраза':
                type_id = 'Поиск '
            else:
                type_id = 'РСЯ '
            return type_id + row['<attribution>DirectBannerGroup'] \
            + '///' + row['<attribution>DirectClickBanner']

        data['ads'] = data.apply(group_banner_func, axis=1)
        ads = data.groupby('ads').sum().\
                sort_values('visits', ascending=False).reset_index()
        ads['depth'] = ads.apply(depth_func, axis=1)
        ads['dur'] = ads.apply(duration_func, axis=1)
        ads = ads[['ads', 'visits', 'conversions', 'depth', 'dur']]
        ads[ ads['ads'].str.contains('РСЯ', case=False) ]
        ads = ads[ads['visits'] > 10]

        ads['ctr'] = ads.apply(ctr_inner_func, axis=1)

        params = [['conversions', 99, 60], ['ctr', 80, 45], ['dur', 55, 30], ['depth', 20, 10]]
        ue = Universal_Estimate (ads, params)
        df = ue.data
        df.to_excel(os.path.join('data/'+client, 
                                 'яндекс директ оценка по группам объявлениям.xlsx'), 
                    sheet_name='er')
        print ('Отчет Group_Ads_Direct_Analyzer успешно завершен')