"""
Оценка качества площадок размещения рекламы 
Для отсеивания и очистки рекламной кампании Google
"""
#для работы с файлами
from scripts.filemanager.filemanager import *
import pandas as pd
import os
#Игнорирование предупреждений
import warnings
warnings.filterwarnings("ignore")
#Импорт графиков
from scripts.plot_scripts.basegraphs import *
#Подключение гугл таблиц
from scripts.filemanager.googlesheets_upload_lib import Googlesheets  
from scripts.filemanager.googlesheets_upload_lib import ListConventer
#Допфункции
from functions.basic_func import *
#Подключение оболочки отчетов
from reports.data_mixin import *


class GetData:
    
    def __init__ (self, client):
        self.client = client
        
        """
        Подкласс получения данных
        """
        class GetData (GetMixinGenerateBigData):
            client = self.client   
            dataset_name = 'organic_poppages_dynamic'   
        gt = GetData()
        data = gt.data        
        """
        Предобработка полученного
        """                
        self.data = data.fillna('Нет данных')
        
        
class Interpolare_Organic_Review:
    def __init__ (self, client):
        
        gt = GetData (client) #Получаем данные
        
        """
        Предобработка
        """
        
        finaldata = None
        data = gt.data[gt.data['<attribution>TrafficSource'] == 'Переходы из поисковых систем']
        data = gt.data[['startOfMonth', 'startURLPath', 'visits']]
        date_list = sorted(list(data['startOfMonth'].unique()))

        for date in date_list:   
            print (date)
            cycledata = data[data['startOfMonth'] == date].groupby('startURLPath').sum().\
                                    sort_values('visits', ascending=False).reset_index()
            cycledata.columns = ['startURLPath', date]

            try:
                finaldata = finaldata.merge(cycledata, how = 'outer', on = 'startURLPath')
            except:
                print ('создаем пустой датасет')
                finaldata = cycledata
        print ('склеивание датасета завершено')

        finaldata = finaldata.fillna(0)
        
        """
        Вычисления
        """
        
        finaldata['dyn-'+date_list[0]] = finaldata.apply(lambda x: 0, axis=1)

        def dyn_func (row):
            if row[date] > row[old_date]:
                return 1
            elif row[date] == row[old_date]:
                return 0
            else:
                return -1

        old_date = date_list[0]

        for date in date_list[1:]:
            print (old_date, date)    
            finaldata['dyn-'+date] = finaldata.apply(dyn_func, axis=1)
            old_date = date    

        finallist = []
        for x in date_list:
            finallist += [[x] +[finaldata['dyn-'+x].sum()]]    

        rows_d = finallist
        headers = ['date', 'result']
        df = pd.DataFrame.from_records(rows_d, columns = headers)
        self.df = df
        
        #Выгрузка отчета
        lc = ListConventer(self.df,
                           'width_headers')
        Googlesheets('data organic!i3:z100', lc.datsheets, client)          
        
        InterPolare(client, df, 'seo')