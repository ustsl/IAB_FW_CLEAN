"""
Класс получения данных.
Задаем параметры, запрашивая недостающее в файле конфиг
Отдаем запрос в метрику по АПИ
Получаем JSON и преобразуем его в Пандас Датасет
"""

import os
from yaml import load
import pandas as pd
from scripts.datareceiver.yandex_metrika.preset_metrika_api import *
from scripts.dataset_generator.pd_func import yametrica_dictlistfunc
from functions.uploadConfig import config_func

# Выгрузка данных в панду

class YaMePanda:    
    
    def __init__ (self, params_width_goals):  
        #Запись пресетов
        metrics = params_width_goals['preset'][1]
        dimensions = params_width_goals['preset'][0]
        #выгрузка отчета     
        client = params_width_goals['client']
        upload_config = config_func(client)
        ym = Metrika_preset(params_width_goals, upload_config[0], upload_config[1])  
        rows = yametrica_dictlistfunc (ym.data)
        df = pd.DataFrame.from_records(rows, columns = dimensions + metrics)
        headers = []
        for x in list(df.columns):
            if ':' in x:
                head = x.split(':')[2]
            else:
                head = x
            headers += [head]
        df.rename(columns=dict(zip(list(df.columns), headers)), inplace=True)
        self.dataset = df


# Добавление целей в отчет
class Data_upload:    
    def __init__ (self, params):  
        
        self.dataset = None
        self.reports(params)

    def reports (self, params): 

        f = open(os.path.join('clients/'+params['client'], 'config.yaml') , 'r')
        config = load(f)
        goals = config['yandex_goals']   

        #Получаем список чистых целей
        if 'yandex_clean_goals' in config:
            clean_goals = config['yandex_clean_goals']        

        stop_goal = False
        #параметры конверсионного отчета  
        for metric in params['preset'][1]:
            if 'goal' in metric.lower():
                stop_goal = True
                
        if stop_goal == False:
            for onegoal in goals:
                params['preset'][1].append (onegoal)
        
        print (params['preset'][1], '- метрики после добавления к ним целей')

        yame = YaMePanda (params)
        dataset = yame.dataset     
        
        columnlist = []       
        for column in dataset.columns:
            if 'goal' in column.lower():
                columnlist += [column]     
        print ('Столбцы целей - {}'.format(str(columnlist)))
        
        def summator (row):
            
            try:
                result = 0
                for x in columnlist:
                    result += row[x]
                return result
            except:
                return 0
            
        dataset['conversions'] = dataset.apply(summator, axis=1)
        
        #Добавление чистых конверсий   
        if 'yandex_clean_goals' in config:            
            clean_goals = config['yandex_clean_goals']  
            columnlist = []
            
            for x in clean_goals:
                if x[5:] in list(dataset.columns):
                    columnlist += [x[5:]]

            dataset['clean_conversions'] = dataset.apply(summator, axis=1)
        print (dataset.columns)

        self.dataset = dataset
        params = None