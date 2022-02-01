import time
import json
import requests
from pprint import pprint

"""
Пример запроса:

dimensions = ['ym:s:startOfMonth', 'ym:s:firstVisitDate']
metrics = ['ym:s:visits', 'ym:s:pageDepth', 'ym:s:avgVisitDurationSeconds']
date = ['2021-10-01', '2021-12-05']
preset = [dimensions, metrics]
params = {'date': date, 'preset': preset, 'filters': None}
token = 'AQAEA7qiBEgDAAWCkiOJYRbq4UB3up80xTmWRuc'
counter = '3858688'
"""

class Metrika_preset:
    
    def __init__ (self, params, token, counter):        
        
        #Хранилище данных        
        self.data = [] 
        
        self.startDate = params['date'][1]
        self.endDate = params['date'][0]

        self.dimensions = params['preset'][0]
        self.metrics = params['preset'][1]
        self.counter = counter
        self.preset = params['preset'] 
        
        self.API_URL = 'https://api-metrika.yandex.ru/stat/v1/data'
        self.headers = {'Authorization': 'OAuth ' + token}    
            
        self.upload()
        
              
    def request_method(self, offset, n_rows):
            
        #Функция запроса      
        params = {
        'date1': self.startDate,
        'date2': self.endDate,
        'id': self.counter,
        'offset': offset,
        'limit': n_rows,
        'accuracy': 'full',
        'dimensions': self.dimensions,
        'metrics': self.metrics,
        }     
        
        r = requests.get(self.API_URL, params=params, headers=self.headers) 
        self.json = r.json()
            

    def upload (self):
        
        n_rows = 500
        offset = 1  
        timer = 1 
        cycle = True
        
        while cycle == True:
            
            #Получаем данные в цикле
            self.request_method(offset, n_rows)
            dataset = self.json
            
            if 'data' in dataset.keys():
                self.data += dataset['data']  
            else:
                print (dataset)
                self.data = 'error'
                break
                
                
            if 'errors' in dataset.keys():            
                print (dataset) 
                self.data = 'error'
                break

            timer += 1

            if timer / 10 == int (timer / 10):       
                print ('Завершен проход данных {}, следующая строка - {}'\
                       .format(timer, offset)) 
                
            time.sleep(1)
        
            offset += n_rows 
            
            if len (dataset['data']) == 0:
                cycle = False
                break

        print ('Успешно завершено. Длина отчета - {}'.format(len(self.data)))