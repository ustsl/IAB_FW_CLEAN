import pandas as pd
import os
#Игнорирование предупреждений
import warnings
warnings.filterwarnings("ignore")
#Импорт графиков
from scripts.plot_scripts.basegraphs import *
#Допфункции
from functions.basic_func import *
#Подключение оболочки отчетов
from reports.data_mixin import *

class CityStatBigReport:
    def __init__ (self, client):
        self.client = client

        #Подготовка данных
        class GetData (GetMixinGenerateBigData):
            client = self.client
            deep = 12
            dataset_name = 'city_stat'

        gt = GetData()
        data = gt.data
        self.data = data.fillna('Нет данных')
        self.rev()
        
    def rev(self):
        
        client = self.client
        data = self.data
        data['date'] = data.apply(lambda x: x.startOfMonth[:-3], axis=1 )
        cities = ['Лесной', 'Камышлов', 'Серов', 'Асбест', 'Рефтинский',
                  'Аремовский', 'Богданович', 'Алапаевск', 'Нижний Тагил', 'Заречный',
                  'Каменск-Уральский', 'Реж', 'Полевской', 'Белоярский', 'Первоуральск', 
                  'Верхняя Пышма', 'Березовский', 'Сухой лог', 'Новоуральск', 'Снежинск']

        for city in cities:
            if city in data.regionCity.unique():
                df = data[data.regionCity == city].sort_values(by='date', ascending=True) 
                x = df['date']
                y1 = df['conversions']
                y2 = df['visits']
                name = 'Динамика визитов и конверсий по городу '+city 
                namey1 = 'Конверсии'
                namey2 = 'Визиты'
                namegraph = 'strategy_динамика_визитов_и_конверсий_по_городу_'+city
                TwoLines(client, x, y1, y2, name, namey1, namey2, namegraph, 'city_stat')      