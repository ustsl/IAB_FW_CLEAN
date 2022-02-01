from scripts.filemanager.txt_file_upload_lib import Write_TXT
from scripts.filemanager.ftp_manager import *

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

class DataPrepare:
    
    def __init__ (self, client):   
        self.client = client
        """
        Готовим даннные
        """        
        class GetData (GetMixinGenerateBigData):
            client = self.client   
            dataset_name = 'organic_medium_statistic'
            deep = 12
        gt = GetData()
        dataset = gt.data
        #Убираем пустые данные, склеиваем урлы, перемножаем средние параметры
        dataset = dataset.fillna('Нет данных')    
        dataset['sumduration'] = dataset.apply(lambda x: x['visits'] * x['avgVisitDurationSeconds'], 
                                              axis=1)
        dataset['sumdepth'] = dataset.apply(lambda x: x['visits'] * x['pageDepth'], 
                                              axis=1)
        dataset['date'] = dataset.apply(lambda x: x['startOfMonth'][:-3], axis=1)
        
        self.df = dataset[['date', '<attribution>SearchEngine', 
                           'visits', 'conversions', 
                           'sumduration', 'sumdepth']]
        
class Organic_Medium:
    
    def __init__ (self, client):

        dp = DataPrepare (client)
        df = dp.df
        """
        Блок аналитики поисковых систем.
        """
        #df_organic = df[ (df['<attribution>SearchEngine']!='Нет данных') ]
        df_organic = \
        df[(df['<attribution>SearchEngine'].str.contains('Яндекс', case=False)) | \
  (df['<attribution>SearchEngine'].str.contains('Google', case=False))]       

        header = ['date', '<attribution>SearchEngine']
        df_organic = df_organic.groupby(header).sum().reset_index()
        df_organic = expand_func (df_organic)
        df_organic =\
        df_organic[['date', '<attribution>SearchEngine', 
                    'visits', 'conversions', 'duration', 'depth']]
        
        df_organic['ctr'] = \
        df_organic.apply(ctr_inner_func, axis=1)
        #Выгрузка отчета
        lc = ListConventer(df_organic,
                           'width_headers')
        Googlesheets('data organic!a3:z200', lc.datsheets, client)          
        df = df_organic
        
        
        date_visits_max = int(df.visits.max())
        date_visits_source_max = list(df[df.visits == df.visits.max()]['<attribution>SearchEngine'])[0]
        date_ctr_max = int(df.ctr.max() * 100) / 100
        date_ctr_source_max = list(df[df.ctr == df.ctr.max()]['<attribution>SearchEngine'])[0]
        date_conversions_max = int(df.conversions.max())
        date_conversions_source_max = \
        list(df[df.conversions == df.conversions.max()]['<attribution>SearchEngine'])[0]

        listdata = \
        ['<strong>Разберем органический поиск.</strong><br><br> В прошлом месяце наибольшее количество визитов ({}) получили по группе источников {}.'\
         .format(date_visits_max, date_visits_source_max), 
         'Наилучшее соотношение визитов к конверсиям ({}%) наблюдаем по группе источников {}.'\
         .format(date_ctr_max, date_ctr_source_max),
         'Совокупный объем конверсий ({}) у рекордного источника {}.'\
         .format(date_conversions_max, date_conversions_source_max)]
        
        #Раскладываем на источники трафика
        for column in list(df_organic['<attribution>SearchEngine'].unique()):
            df_organic[column] = \
            df_organic.apply\
            (lambda x: x['visits'] if x['<attribution>SearchEngine'] == column else 0, axis=1)

        df_organic_visits = \
        df_organic[['date'] + list(df_organic['<attribution>SearchEngine'].unique())]
        df_organic_visits = df_organic_visits.groupby(['date']).sum().reset_index()

        """
        Фильтр нулевых значений
        """
        
        cols = [list(df_organic_visits)[0]]
        for col in list(df_organic_visits)[1:]:
            if df_organic_visits[col].sum() > 0:
                print (col)
                cols += [col]

        df_organic_visits = df_organic_visits[cols]

        """
        Конец фильтра нулевых значений
        """
        
        
        
        ftppath = 'data/'+client
        cf = Connect_FTP()
        ftpdf = Directory_FTP (cf.ftp)                

        filename = 'strategy_organic_traffic_accumulation.png'
        texttitle = 'Срез по поисковым системам - визиты'
        Сumulative_Graph (client, df_organic_visits, texttitle, filename, 'seo')
        ftpdf.dirs_walker(client, ftppath, filename)

        #Раскладываем на конверсии

        for column in list(df_organic['<attribution>SearchEngine'].unique()):
            df_organic[column] = \
            df_organic.apply\
            (lambda x: x['conversions'] if x['<attribution>SearchEngine'] == column else 0, axis=1)

        df_organic_visits =\
        df_organic[['date'] + list(df_organic['<attribution>SearchEngine'].unique())]
        df_organic_visits = df_organic_visits.groupby(['date']).sum().reset_index()
        

        filename = 'strategy_organic_conversions_accumulation.png'
        texttitle = 'Срез по поисковым системам - конверсии'
        Сumulative_Graph (client, df_organic_visits, texttitle, filename, 'seo')
        ftpdf.dirs_walker(client, ftppath, filename)
        
        strategy_organic_conversions_accumulation_txt = '<br>'.join(listdata)  
        Write_TXT(ftppath, 'strategy_organic_conversions_accumulation.txt', 
                  strategy_organic_conversions_accumulation_txt)        
        ftpdf.dirs_walker(client, ftppath, 'strategy_organic_conversions_accumulation.txt')



        """
        Окончание блока аналитики органического трафика
        """       