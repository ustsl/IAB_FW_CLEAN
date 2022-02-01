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
        dataset['sumduration'] = dataset.apply(sumdur_func,  axis=1)
        dataset['sumdepth'] = dataset.apply(sumdepth_func, axis=1)
        self.dataset = dataset
           
class HardDirectReview:
    
    """
    Класс отчета по когортам слов в Яндекс Директе.
    """
    
    def __init__ (self, client):     
        
        dp = DataPrepare(client)        
        data = dp.dataset
        data_query = data[data['<attribution>DirectConditionType'] == 'Ключевая фраза']

        def corrected (row):
            if row['<attribution>DirectSearchPhrase'] == 'Нет данных':
                return row['<attribution>DirectPhraseOrCond']
            else:
                return row['<attribution>DirectSearchPhrase']
        data_query['<attribution>DirectSearchPhrase'] = data_query.apply(corrected, axis=1)

        fttd = FromTableToDictionary(client)
        basekeydict = fttd.basekeydict
        dirty_keys = list(data_query['<attribution>DirectSearchPhrase'])
        kc = Keyword_Clustering(basekeydict, dirty_keys)  
        data_query['key_type'] = kc.clasters

        #основные данные
        df = data_query.groupby('key_type').sum().\
        sort_values('visits', ascending=False).reset_index()
        
        df = expand_func (df)

        df = df[['key_type', 'visits', 'duration', 'depth', 'conversions', 'ctr' ]]
        if df['visits'][0] > 70:
            mv = 50
        else:
            mv = 10
        df = df[df['visits'] > mv]
      
        #сохраняем полученное
        df.to_excel(os.path.join('data/'+client, 
                                 'квартальная таблица по группам ключей в директе.xlsx'), 
                                  sheet_name='er')


        not_key_type = data_query[data_query['key_type'] == 'не определено']\
        ['<attribution>DirectSearchPhrase']

        not_key_type.to_excel(os.path.join('data/'+client, 
                                 'яндекс директ запросы словаря.xlsx'), 
                                  sheet_name='er')
        df = df[df['key_type'] != 'не определено']
        
        try:
            params = [['ctr', 80, 45], ['duration', 55, 30], ['depth', 20, 10]]
            ue = Universal_Estimate (df, params)
            df = ue.data
        except:
            None      
            
        GraphColumns (client, df.head(10), 'visits',
                      "Визиты с директа по кластерам в последнем отчетном месяце",
                      '# Визиты',
                      'direct_claster_visits.png', 'yandex_direct', 'sort')
        
        GraphColumns (client, df.head(10), 'ctr',
                      "CTR с директа по кластерам в последнем отчетном месяце",
                      '# CTR',
                      'direct_claster_ctr.png', 'yandex_direct', 'sort')        
        
        #Выгрузка отчета
        lc = ListConventer(df.head(20),
                           'width_headers')
        Googlesheets('data direct!a3:z100', lc.datsheets, client)      
    
        conditiontypedata = data.groupby('<attribution>DirectConditionType').sum().\
        sort_values('visits', ascending=False).reset_index()

        conditiontypedata = expand_func(conditiontypedata)        
    
        params = [['ctr', 80, 45], ['duration', 55, 30], ['depth', 20, 10]]
        ue = Universal_Estimate (conditiontypedata, params)
        conditiontypedata = ue.data
        conditiontypedata = conditiontypedata[['<attribution>DirectConditionType', 'visits',
                                               'conversions', 'ctr', 'estimate']]    
        #Выгрузка отчета
        lc = ListConventer(conditiontypedata.head(20),
                           'width_headers')
        
        Googlesheets('data direct!a25:z100', lc.datsheets, client)    
        
        def mark_func(row):
            if 'условие' in row['<attribution>DirectPhraseOrCond'].lower():
                return 1
            else:
                return 0
        self.data = data
        data['mark'] = data.apply (mark_func, axis=1)          
        markdata = data[data['mark'] == 1]
        
        try:
            markdata = markdata.groupby('<attribution>DirectPhraseOrCond').sum().\
            sort_values('visits', ascending=False).reset_index()            
            markdata = expand_func(markdata)               
            markdata = markdata[['<attribution>DirectPhraseOrCond', 'visits', 
                                                  'duration', 'depth', 'conversions', 'ctr']]
            #markdata = markdata[markdata['visits'] > 20]

            #Выгрузка актуалки
            lc = ListConventer(markdata.head(20),'width_headers')     
            maxlonglist = 55
            maxwidthlist = 7
            width = [''] * maxwidthlist
            maxlist = [width] * maxlonglist    
            Googlesheets('data direct!a45:z100', maxlist, client)  
            Googlesheets('data direct!a45:z100', lc.datsheets, client) 

        except:
            print ('Выгрузка по условиям не сработала')

        print ('Отчет HardDirectReview успешно завершен')