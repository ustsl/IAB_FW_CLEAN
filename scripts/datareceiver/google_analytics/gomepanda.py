import pandas as pd
import os
from yaml import load
from scripts.datareceiver.google_analytics.preset_analytics_api import Googleadwords_upload




class GoMePanda:
    
    def __init__ (self, date, client, googleparams):
        
        f = open(os.path.join('clients/'+ client, 'config.yaml') , 'r')
        config = load(f)
        #принимаем в переменную гугл
        goals = config['google_goals']   
        goals =  ', '.join(goals)        
        googleparams['metrics'] = googleparams['metrics'] + ', ' + goals

        #Параметры запроса       
        self.googleparams = googleparams        
        print (googleparams)
        self.params = {'googleParams': googleparams, 'client': client, 'date':date}
    
    def work (self):    
        
        headers = \
        self.googleparams['dimensions'].split(', ') + self.googleparams['metrics'].split(', ')
        
        mess = \
        Googleadwords_upload (self.params)
        self.bigdatas = mess.bigdatas
        
        rows_d = self.bigdatas[1:]
        print ('sdfsdfsdfsfsdfsdfsdf')
        data = pd.DataFrame.from_records(rows_d, columns = headers)
        
        for x in list(data.columns):
            if x in self.googleparams['metrics'].split(', '):
                data[x] = data[x].astype(float)
                print (x, ' переведен в числовой тип из строчного')
                        
        columnlist = []          
        for column in data.columns:
            if 'goal' in column.lower():
                columnlist += [column]     
        print ('Столбцы целей - {}'.format(str(columnlist)))

        def summator (row):
            result = 0
            try:
                for x in columnlist:
                    result += row[x]
            except:
                print \
                ('Вероятно, нечего считать. В google_analytics/gomepanda была ошибка ')
            return result

        data['conversions'] = data.apply(summator, axis=1)                
                

        self.data = data
        print ('Данные собраны в pandas-контейнер data')