from scripts.filemanager.ftp_manager import *
import pandas as pd
import os
#Подключение гугл таблиц
from scripts.filemanager.googlesheets_upload_lib import Googlesheets  
from scripts.filemanager.googlesheets_upload_lib import ListConventer
import warnings
warnings.filterwarnings("ignore")
from scripts.plot_scripts.basegraphs import *
from scripts.dataset_generator.classes_for_pandas import Means
#Подключение оболочки отчетов
from reports.data_mixin import *
from scripts.filemanager.txt_file_upload_lib import Write_TXT


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
            deep = 24
            dataset_name = 'base_long_stat'   
        gt = GetData()
        self.data = gt.data        

class TwoYear:
    def __init__ (self, client):

        dp = DataPrepare (client)
        data = dp.data[['startOfMonth', 'visits', 'conversions', 'clean_conversions']]

        #Выгрузка актуалки
        lc = ListConventer(data,'width_headers')     
        maxlonglist = 900
        maxwidthlist = 7
        width = [''] * maxwidthlist
        maxlist = [width] * maxlonglist    
        Googlesheets('2years!a1:z1000', maxlist, client)  
        Googlesheets('2years!a1:z1000', lc.datsheets, client) 
