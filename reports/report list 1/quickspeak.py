import pandas as pd
import os
#Подключение гугл таблиц
from scripts.filemanager.googlesheets_upload_lib import Googlesheets  
from scripts.filemanager.googlesheets_upload_lib import ListConventer
import warnings
warnings.filterwarnings("ignore")
from scripts.plot_scripts.basegraphs import *
from scripts.dataset_generator.classes_for_pandas import Means
from urllib import parse
#Допфункции
from functions.basic_func import ctr_inner_func
#Подключение оболочки отчетов
from reports.data_mixin import *
from scripts.filemanager.ftp_manager import *

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
            dataset_name = 'quickspeak'
            deep = 1
        gt = GetData()
        data = gt.data        
        """
        Предобработка полученного
        """          
        data = data.fillna('Нет данных')        
        self.dataset = data
        
        
class QUICKSPEAK_REPORT:
    def __init__ (self, client):
        
        dp = DataPrepare(client)
        data = dp.dataset
        sumconversions = int(dp.dataset['conversions'].sum())
        data = data[ data['startURL'].str.contains('quickspeak', case=False) ]

        def URL_urllib_func(row):
            parsed = row['startURL']
            url = parse.urlsplit(parsed)
            return url.netloc + url.path

        def IFRAME_func (row):
            if 'iframe' in row['url']:
                return 'FRAME'
            return 'LANDING'

        data['url'] = data.apply(URL_urllib_func, axis=1)
        data = data[['url', 'visits', 'conversions']]
        data = data.groupby(['url']).sum().sort_values('visits', ascending=False)\
        .reset_index()
        data['ctr'] = data.apply(ctr_inner_func, axis=1)
        data['type'] = data.apply(IFRAME_func, axis=1)
        
        uplgraph = data[data['type'] == 'LANDING'].head(10)[['url', 'ctr']]
        
        self.data = data        

        #Выгрузка отчета
        lc = ListConventer(data,
                           'width_headers')
        maxlonglist = 900
        maxwidthlist = 5
        width = [''] * maxwidthlist
        maxlist = [width] * maxlonglist

        Googlesheets('quickspeak!a1:i999', maxlist, client)
        Googlesheets('quickspeak!a1:z100', lc.datsheets, client)       

        data = data.groupby(['type']).sum().sort_values('visits', 
                                                        ascending=False).reset_index()
        data['ctr'] = data.apply(ctr_inner_func, axis=1)
        #Выгрузка отчета
        lc = ListConventer(data,
                           'width_headers')
        Googlesheets('quickspeak!g1:z100', lc.datsheets, client)    
        
        if uplgraph.ctr.max() > 0:            
        
            GraphColumns (client,
                uplgraph,
                'ctr',
                "CTR квик-взаимодействий",
                '# CTR',
                'quick_ctr.png', 'base', 'sort'
               )

            ftppath = 'data/'+client
            cf = Connect_FTP()
            ftpdf = Directory_FTP (cf.ftp)        
            ftpdf.dirs_walker(client, ftppath, 'quick_ctr.png')