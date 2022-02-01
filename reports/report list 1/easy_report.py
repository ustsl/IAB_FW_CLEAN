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
            dataset_name = 'base_statistic'   
        gt = GetData()
        data = gt.data        
        """
        Предобработка полученного
        """          
        data['startURLDomain'] = data.fillna('Нет данных')
        data['startURLPath'] = data.fillna('Нет данных')
        data = data.fillna(0)        
        data['url'] = data.apply(lambda x: x['startURLDomain'] + x['startURLPath'], axis=1)
        data['sumduration'] = \
        data.apply(lambda x: x['avgVisitDurationSeconds'] * x['visits'], axis=1)
        data['sumdepth'] = data.apply(lambda x: x['pageDepth'] * x['visits'], axis=1)
        data['date'] = data.apply(lambda x: x['startOfMonth'][:-3], axis=1)
        #Чистка данных
        dataset = data[['date', '<attribution>TrafficSource', 'bounce', 
                        'url', 'visits', 'sumduration', 
                        'sumdepth', 'conversions', 'clean_conversions']]
        dataset.rename(columns={'<attribution>TrafficSource': 'source'}, inplace=True)
        self.dataset = dataset      
 
class EasyReport:
    
    def __init__ (self, client):
        self.ftppath = 'data/'+client
        dtpre = DataPrepare (client)
        self.dataset = dtpre.dataset
        self.client = client        
        self.conversion_func()
        self.first_base_report()
        self.second_base_report()

        
    def conversion_func (self):        
        client = self.client        
        """
        Функция очистки данных от "нет данных".
        """
        def conversion_func(row):
            if type(row['conversions']) == str:
                return 0
            else:
                return int(row['conversions'])
        self.dataset['conversions'] = self.dataset.apply(conversion_func, axis=1)          
        
        
    def first_base_report (self):
        client = self.client 
        ftppath = self.ftppath
        #ОТЧЕТ 1        
        df = self.dataset.groupby(['date', 'bounce']).sum().reset_index() 
        
        #Вызываем функцию развертывания sum-значений
        me = Means (df)
        df = me.df      
        self.df = df
        
        def msg_estimate (est_data):
            bounce = est_data[est_data['bounce'] == 'Отказ']
            notbounce = est_data[est_data['bounce'] != 'Отказ']
            summarized_data = est_data.groupby(['date']).sum().reset_index()  
            summarized_data['ctr'] = summarized_data.apply\
            (lambda x: int(x['conversions'] / x['visits'] * 10000) / 100, axis=1)
    
            msg = [] #контейнер сообщения в рамках отчета
            estimate_number = 0 #объем позитивных факторов
            
            #Анализ отказных визитов
            last = int(list(bounce.visits)[-2])
            act = int(list(bounce.visits)[-1])
            if act > last:
                msg += ['Объем отказных визитов вырос относительно прошлого месяца с {} до {}.'\
                       .format (last, act)]      
            else:
                msg += ['Объем отказных визитов снизился относительно прошлого месяца с {} до {}.'\
                       .format (last, act)]  
                estimate_number += 1
                
            #Анализ эффективных визитов
            last = int(list(notbounce.visits)[-2])
            act = int(list(notbounce.visits)[-1])            
            if act > last:
                msg += ['Объем эффективных визитов {} вырос относительно прошлого месяца {}.'\
                       .format(act, last)]
                estimate_number += 1
            else:
                msg += ['Объем эффективных визитов {} снизился относительно прошлого месяца {}.'\
                        .format(act, last)]     
            #Анализ конверсий
            last = int(list(summarized_data.conversions)[-2])
            act = int(list(summarized_data.conversions)[-1])
            
            if act > last:
                msg += ['Выросли по количеству конверсионных действий.']
                estimate_number += 1
            else:
                msg += ['Количество конверсий относительно прошлого месяца упало.']   
            msg += ['Было - {}, стало - {}.'.format(last, act)]
            
            #Анализ CTR
            last = int(list(summarized_data.ctr)[-2])
            act = int(list(summarized_data.ctr)[-1])
            if act > last:
                msg += \
                ['Рост CTR свидетельствует о повышении качества трафика.']
                estimate_number += 1
            else:
                msg += \
                ['Упал CTR. В сравнении с прошлым периодом эффективность трафика снизилась.'] 
            msg += ['Изменение CTR в цифрах. Прошлый месяц - {}%, текущий - {}%'.format (last, act)]
                
            if estimate_number > 1:
                msg += \
                ['Совокупный результат месяца говорит о позитивной динамике.'] 
                
            return msg        
        
        self.txt_1 = msg_estimate (df)
        
        strategy_easy_report_txt = '<br>'.join(self.txt_1)
        Write_TXT(ftppath, 'strategy_easy_report.txt', strategy_easy_report_txt)
        
        #Выгрузка данных в гугл таблицы       
        data = df[df['bounce'] == 'Не отказ'].reset_index()
        del(data['bounce'])
        del(data['index'])
        data['badvisits'] = list(df[df['bounce'] == 'Отказ']['visits'])        
        
        #Выгрузка отчета
        lc = ListConventer(data,
                           'width_headers')
        maxlonglist = 14
        maxwidthlist = 7
        width = [''] * maxwidthlist
        maxlist = [width] * maxlonglist
        Googlesheets('data base!a3:z16', maxlist, client) 
        Googlesheets('data base!a3:z16', lc.datsheets, client)  

        #Выгрузка данных в графики
        badline = df[ (df['bounce']=='Отказ')]
        goodline = df[ (df['bounce']!='Отказ')]
        
        #Данные для графика
        goodline = goodline.reset_index()
        del(goodline['index'])
        badline = badline.reset_index()
        del(badline['index'])        
        GoodBadLine (client, goodline, badline, 'strategy_easy_report.png', 'base')            
        cf = Connect_FTP()
        ftpdf = Directory_FTP (cf.ftp)
        ftpdf.dirs_walker(client, ftppath, 'strategy_easy_report.png')
        ftpdf.dirs_walker(client, ftppath, 'strategy_easy_report.txt')



    def second_base_report (self):  
        client = self.client 
        dataset = self.dataset
        ftppath = self.ftppath 
        
        def source_func(row):
            if 'реклам' in str(row['source']).lower():
                return 'ads'
            elif 'соц' in str(row['source']).lower():
                return 'smm'
            elif 'поиск' in str(row['source']).lower():
                return 'seo'
            else:
                return 'другое'

        dataset['source'] = dataset.apply(source_func, axis=1)
        df = dataset[dataset['source'] != 'другое']
        df = df.groupby(['date', 'source']).sum().reset_index()  
        me = Means (df)
        upload = me.df

        #Выгрузка актуалки
        lc = ListConventer(upload,'width_headers')     
        maxlonglist = 900
        maxwidthlist = 7
        width = [''] * maxwidthlist
        maxlist = [width] * maxlonglist    
        Googlesheets('data base!a18:z1000', maxlist, client)  
        Googlesheets('data base!a18:z1000', lc.datsheets, client) 

        #Подготовка данных для графика   
        df['smm'] = df.apply(lambda x: x['visits'] if x['source'] == 'smm' else 0, axis=1)
        df['seo'] = df.apply(lambda x: x['visits'] if x['source'] == 'seo' else 0, axis=1)
        df['ads'] = df.apply(lambda x: x['visits'] if x['source'] == 'ads' else 0, axis=1)
        graph_upl = df.groupby(['date']).sum().reset_index()
        graph_upl = graph_upl[['date', 'smm', 'seo', 'ads']]        
        Сumulative_Graph (client, graph_upl, 'Диаграмма накопления трафика',
                          'strategy_traffic_accumulation.png', 'base')     
        cf = Connect_FTP()
        ftpdf = Directory_FTP (cf.ftp)
        ftpdf.dirs_walker(client, ftppath, 'strategy_traffic_accumulation.png') 
        self.df = df

        #Подготовка данных для графика   
        df['smm'] = df.apply(lambda x: x['conversions'] if x['source'] == 'smm' else 0, 
                             axis=1)
        df['seo'] = df.apply(lambda x: x['conversions'] if x['source'] == 'seo' else 0, 
                             axis=1)
        df['ads'] = df.apply(lambda x: x['conversions'] if x['source'] == 'ads' else 0, 
                             axis=1)
        graph_upl = df.groupby(['date']).sum().reset_index()
        graph_upl = graph_upl[['date', 'smm', 'seo', 'ads']]       
        df.date_max = df.apply\
        (lambda x: int(x['date'].replace('-','')), axis=1)
        df = df[df.date_max == df.date_max.max()]
        date_visits_max = int(df.visits.max())
        date_visits_source_max = list(df[df.visits == df.visits.max()].source)[0]
        date_ctr_max = int(df.ctr.max() * 100) / 100
        date_ctr_source_max = list(df[df.ctr == df.ctr.max()].source)[0]
        date_conversions_max = int(df.conversions.max())
        date_conversions_source_max = list(df[df.conversions == df.conversions.max()].source)[0]
        listdata = ['В прошлом месяце наибольшее количество визитов ({}) получили по группе источников {}.'\
        .format(date_visits_max, date_visits_source_max), 
                    'Наилучшее соотношение визитов к конверсиям ({}%) наблюдаем по группе источников {}.'\
        .format(date_ctr_max, date_ctr_source_max),
                    'Совокупный объем конверсий ({}) у рекордного источника {}.'\
        .format(date_conversions_max, date_conversions_source_max)]
        strategy_ctr_accumulation_txt = '<br>'.join(listdata)     
        Write_TXT(ftppath, 'strategy_ctr_accumulation.txt', strategy_ctr_accumulation_txt) 
        Сumulative_Graph (client, graph_upl, 'Диаграмма накопления конверсий',
                          'strategy_ctr_accumulation.png', 'base')  
        cf = Connect_FTP()
        ftpdf = Directory_FTP (cf.ftp)        
        ftpdf.dirs_walker(client, ftppath, 'strategy_ctr_accumulation.png')
        ftpdf.dirs_walker(client, ftppath, 'strategy_ctr_accumulation.txt')
        
        
        