import os
import pandas as pd

"""
Быстрый кластеризатор по ключам.
Алгоритм мгновенной предобработки данных

СИНТАКСИС ИСПОЛЬЗОВАНИЯ:

client = 'angio_line' #ЗАВОДИМ КЛИЕНТА
fttd = FromTableToDictionary(client) #ВЫЗЫВАЕМ С КЛИЕНТОМ ГЕНЕРАЦИЮ СЛОВАРЯ
basekeydict = fttd.basekeydict #
kc = Keyword_Clustering (basekeydict, test)
cluster_column = kc.finalkeylist
"""

class FromTableToDictionary:
    
    #Класс получает таблицу и переводит ее в словарь
    #В качестве терминов словаря - лист со значениями

    def __init__ (self, client):
        keydict = pd.read_excel(os.path.join('clients/'+client, 'keydict.xlsx'))
        basekeydict = {}
        for term in keydict.columns:
            termlist = []
            for key in list(keydict[term]):
                if key == key:
                    termlist += [str(key)]
            
            basekeydict[term] = termlist
        self.basekeydict = basekeydict
        print ('Done. Словарь доступен под именем basekeydict')

        
from itertools import groupby

class Keyword_Clustering:
    
    #Класс принимает словарь данных и группу ключевых слов
    #На выходе отдает группу кластеров
    def __init__ (self, basekeydict, basekeylist):
        self.basekeydict = basekeydict
        self.clasters = []
        for query in basekeylist:
            self.key_claster_func(query)
            self.clasters += [self.key_type]
        print ('Done. Столбец в переменной clasters')

    def key_claster_func(self, query):
        #базовый алгоритм на основе сборки по ключам словаря
        key_type_list = []
        for dictkey in list(self.basekeydict.keys()):
            for term in self.basekeydict[dictkey]:
                if term in str(query).lower():
                    key_type_list.append(dictkey.title()) 
                    continue
        #безопасная группировка
        key_type_list = [el for el, _ in groupby(key_type_list)]
        #если нужных значений не обнаружено - присваем статус "не определено"
        if len(key_type_list) == 0:
            key_type_list.append('не определено') 
        #превращаем полученный список значений в строку
        self.key_type = str(' '.join(key_type_list))
        