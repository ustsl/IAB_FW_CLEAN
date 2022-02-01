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
from scripts.estimatelib.universal_estimate import Universal_Estimate
#Допфункции
from functions.basic_func import *
#Подключение оболочки отчетов
from reports.data_mixin import *
from scripts.filemanager.ftp_manager import *
from scripts.filemanager.txt_file_upload_lib import Write_TXT



class TXT_module:
    
    """
    На вход принимаем датасет. 
    На основе данных датасета расписываем результаты.
    """
    
    def __init__ (self, df):   
        
        #Готовим 2 датасета
        date_group = df.groupby('date').sum().reset_index()
        date_group['date_max'] = date_group.apply\
        (lambda x: int(x['date'].replace('-','')), axis=1)
        date_group['ctr'] = date_group.apply(ctr_inner_func, axis=1)
        date_group['clean_ctr'] = date_group.apply(clean_ctr_inner_func, axis=1)
        params = [['depth', 35, 20], ['clean_ctr', 95, 80], 
                  ['ctr', 85, 50], ['duration', 70, 40]]
        ue = Universal_Estimate (date_group, params)
        date_group = ue.data

        #Пишем необходимые цифры для формирования отчета
        last_ctr = list(date_group.iloc[-2:].ctr)[0]
        act_ctr = list(date_group.iloc[-2:].ctr)[1]
        last_visits = int(list(date_group.iloc[-2:].visits)[0])
        act_visits = int(list(date_group.iloc[-2:].visits)[1])
        last_estimate = list(date_group.iloc[-2:].estimate)[0]
        act_estimate = list(date_group.iloc[-2:].estimate)[1]
        df.date_max = df.apply\
        (lambda x: int(x['date'].replace('-','')), axis=1)
        df = df[df.date_max >= df.date_max.max()]
        date_visits_max = int(df.visits.max())
        date_visits_source_max = list(df[df.visits == df.visits.max()].source)[0]
        date_ctr_max = int(df.ctr.max() * 100) / 100
        date_ctr_source_max = list(df[df.ctr == df.ctr.max()].source)[0]
        date_ctr_source_visits_max = list(df[df.source == date_ctr_source_max].visits)[0]
        date_conversions_max = int(df.conversions.max())
        date_conversions_source_max = list(df[df.conversions == df.conversions.max()].source)[0]

        list_data = []

        list_data += \
        ['<strong>Разберем рекламные каналы.</strong>']
        list_data += \
        ['В прошлом месяце наибольшее количество визитов ({}) получили по группе источников {}.'\
         .format(date_visits_max, date_visits_source_max)]
        list_data += \
        ['Наилучшее соотношение визитов к конверсиям ({}%) наблюдаем по группе источников {}.'\
        .format(date_ctr_max, date_ctr_source_max)]
        list_data += \
        ['При объеме трафика по данному источнику - {}'\
         .format(date_ctr_source_visits_max)]
        list_data += \
        ['Совокупный объем конверсий ({}) у рекордного источника {}.'\
         .format(date_conversions_max, date_conversions_source_max)]


        #Производим сравнения
        if last_ctr > act_ctr:
            list_data +=\
            ['По отношению к предыдущему периоду упал общий CTR рекламных кампаний. Было - {}%, стало - {}%.'\
            .format (last_ctr, act_ctr)]
        if last_ctr < act_ctr:
            list_data +=\
            ['По отношению к предыдущему периоду вырос общий CTR рекламных кампаний. Было - {}%, стало - {}%.'\
            .format (last_ctr, act_ctr)]

        #Производим сравнения
        if last_visits > act_visits:
            list_data +=\
            ['На проекте сократился трафик с рекламных кампаний. Было - {}, стало - {}.'\
            .format (last_visits, act_visits)]
        if last_visits < act_visits:
            list_data +=\
            ['Вырос совокупный объем трафика с рекламных кампаний. Было - {}, стало - {}.'\
            .format (last_visits, act_visits)]

        list_data +=\
        ['В рамках комплексной оценки качества рекламных кампаний произошли следующие изменения']
        list_data +=\
        ['Было - {}, стало - {}.'\
         .format (last_estimate, act_estimate)]

        #Производим сравнения
        if last_estimate > act_estimate:
            list_data +=\
            ['Стоит обратить пристальное внимание на поведенческие факторы рекламных кампаний.']

        if last_estimate < act_estimate:
            list_data +=\
            ['Рост замечен в том числе по косвенным поведенческим показателям.']


        self.result = '<br>'.join(list_data)  


class DataPrepare:
    
    def __init__ (self, client): 
        self.client = client
        """
        Подкласс получения данных
        """
        
        class GetData (GetMixinGenerateBigData):
            client = self.client   
            dataset_name = 'ads_medium_statistic'
            deep = 12
        gt = GetData()
        self.data = gt.data
        dataset = self.data.fillna(0)
        dataset = dataset[dataset['<attribution>AdvEngine'] != 0]
        
        """
        Готовим даннные
        """        
        
        
        dataset['sumduration'] = \
        dataset.apply(lambda x: x['visits'] * x['avgVisitDurationSeconds'],
                      axis=1)
        dataset['sumdepth'] = \
        dataset.apply(lambda x: x['visits'] * x['pageDepth'],
                      axis=1)
        dataset['date'] = dataset.apply(lambda x: x['startOfMonth'][:-3], axis=1)

        dataset['source'] = dataset.apply(lambda x: x['<attribution>AdvEngine'], axis=1)
        
        #Добавляем конверсионные столбики
        goals = []
        for x in list(dataset.columns):
            if 'conversion' in x:
                goals += [x]
        
        self.df = dataset[['date', 'source', 'visits', 'sumduration', 'sumdepth'] + goals]
        
        """ 
        Окончание подготовки данных
        """        
        
        
class AdsMediumReport:
    
    def __init__ (self, client):
        
        dprep = DataPrepare (client)
        df_ads = dprep.df    

        #Отчет по рекламным каналам трафика      
        header = ['date', 'source']
        df_ads = df_ads.groupby(header).sum().reset_index()
        df_ads = expand_func (df_ads)
        df_ads = df_ads[df_ads.visits >= 1].reset_index()
        df_ads_filter = df_ads.visits.mean()
        df_ads['ctr'] = df_ads.apply(ctr_inner_func, axis=1)
        df_ads['clean_ctr'] = df_ads.apply(clean_ctr_inner_func, axis=1)
        del(df_ads['index'])
        df_ads = df_ads[['date', 'source', 'visits', 'conversions', 
                        'clean_conversions', 'ctr', 'clean_ctr', 
                        'duration', 'depth']]
        
        params = [['depth', 35, 20], ['clean_ctr', 95, 80], 
                  ['ctr', 85, 50], ['duration', 70, 40]]
        ue = Universal_Estimate (df_ads, params)
        self.df = ue.data
        
        #Выгрузка отчета
        lc = ListConventer(self.df,'width_headers')        
        maxlonglist = 900
        maxwidthlist = 10
        width = [''] * maxwidthlist
        maxlist = [width] * maxlonglist    
        Googlesheets('data ads!a1:z999', maxlist, client)        
        Googlesheets('data ads!a1:z100', lc.datsheets, client)      

        #Раскладываем на источники трафика
        for column in list(df_ads['source'].unique()):

            df_ads[column] = \
            df_ads.apply(lambda x: x['visits'] if x['source'] == column else 0, axis=1)

        df_ads_visits = df_ads[['date'] + list(df_ads['source'].unique())]
        df_ads_visits = df_ads_visits.groupby(['date']).sum().reset_index()
        for fltr in df_ads_visits.columns[1:]:
            if df_ads_visits[fltr].sum() < df_ads_filter:
                del(df_ads_visits[fltr])
                
        #Визуальная часть
        col = df_ads_visits.columns    
        
        ftppath = 'data/'+client
        cf = Connect_FTP()
        ftpdf = Directory_FTP (cf.ftp)        



        #Накопительный график
        filename = 'strategy_ads_traffic_accumulation.png'
        texttitle = 'Срез по ключевым рекламным каналам - визиты'
        Сumulative_Graph (client, df_ads_visits, texttitle, filename, 'base')     
        ftpdf.dirs_walker(client, ftppath, filename)
        
        #Раскладываем на конверсии
        for column in col[1:]:    
            df_ads[column] = \
            df_ads.apply(lambda x: x['conversions'] if x['source'] == column else 0, axis=1)

        df_ads_visits = df_ads[col]
        df_ads_visits = df_ads_visits.groupby(['date']).sum().reset_index() 
        filename = 'strategy_ads_conversion_accumulation.png'
        texttitle = 'Срез по ключевым рекламным каналам - конверсии'
        Сumulative_Graph (client, df_ads_visits, texttitle, filename, 'base')
        ftpdf.dirs_walker(client, ftppath, filename)
        
        df = df_ads
        
        try:
            sac = TXT_module(df)        
            strategy_ads_conversion_accumulation_txt = sac.result    
            Write_TXT(ftppath, 'strategy_ads_conversion_accumulation.txt', 
                      strategy_ads_conversion_accumulation_txt)        
            ftpdf.dirs_walker(client, ftppath, 'strategy_ads_conversion_accumulation.txt')
        except:
            print ('Текстовая часть не выгружена')
        self.data = df_ads

#Вычисляем CTR по рекламным каналам. 
#Отдельно, т.к. другой график и другой набор вычислений

class CTR_ads_report:
    def __init__ (self, client):
        
        #Получение данных 
        dp = DataPrepare(client) 
        data = dp.df
        
        #Вычисление CTR
        data = data.groupby(['date', 'source']).sum().reset_index()
        data['ctr'] = data.apply(ctr_inner_func, axis=1)
        data = data[['date', 'source', 'ctr']]
        
        #Транспонирование таблицы
        for column in list(data['source'].unique()):
            data[column] = \
            data.apply(lambda x: x['ctr'] if x['source'] == column else 0, axis=1)
            
        data = data[['date'] + list(data['source'].unique())]
        data = data.groupby(['date']).sum().reset_index()
        
        #Фильтруем непонятную рекламу
        for fltr in list(data.columns[1:]):
            if 'Другая' in fltr or data[fltr].mean() < 0.001 :
                del(data[fltr])
            
        self.data = data

        #Отрисовываем
        Line_Graph (client, data,
                    'Изменение CTR по рекламным каналам трафика',
                    'strategy_ctr_year_stat.png', 'base')     
        
        

        