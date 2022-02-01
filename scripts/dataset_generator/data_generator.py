"""
Задача блока:

- выявить датасеты для генерации,
- отправить данные в библиотеки парсинга,
- получить датасеты обратно,
- сохранить их по одному.
"""

import os
import pandas as pd
#Библиотека обработки запросов для API их распределения по каналам
from scripts.datareceiver.datareceiver import DataReceiver
#На основе дат генерит нужные имена файлов
from scripts.datemaster.datefilemaster import DateFileNow
#Менеджер файлов
from scripts.filemanager.filemanager import FileManagerForDatasets
#Для организации пауз между запросами
import time

class DatasetGenerator:
    
    def __init__ (self, client, dataset_name, deep):
        
        self.client = client
        self.dataset_name = dataset_name
        self.deep = deep
        self.generate_files()
    
    def generate_files (self):
        
        client = self.client
        dataset_name = self.dataset_name
        deep = self.deep
        
        #Забираем файл конфигурации и смотрим что есть в папке  
        #Фактически - передаем имя клиента и имя датасета
        #На основании этих данных получаем список файлов в папке
        
        fm = FileManagerForDatasets (client, dataset_name)
        fm.filelist()
        fm.openconfig()
        
        #Записываем файл конфига
        
        config = fm.config
        print ('Количество файлов в папке - {}'.format(len(fm.files)))

        #Смотрим - чего недостает. Генерим под недостающее: имена файлов, даты запросов  
        
        dfn = DateFileNow (deep)
        actualfiles = dfn.actualfiles
        data_names_for_parsing = [] 
        
        #Сюда складываем файлы, которых не хватает для формирования датасета
        
        for filelist in actualfiles:    
            if filelist[0] not in fm.files:
                data_names_for_parsing += [filelist]
        timer = 0    

        for parser in data_names_for_parsing:
            print ('Собираю данные под дату {}'.format(parser[1]))
            try:
                dr = DataReceiver (parser[1], client, config)    
                fm.savefile(dr.dataset, parser[0])
                print ('Файл сохранен - {}'.format(str(parser[0])))
                time.sleep(1)
                timer += 1
            except ValueError:
                print ('Преждевременный выход из цикла')
                break
                
            
        print ('Количество дозагруженных в датасет файлов - {}'.format(timer))
        self.actualfiles = actualfiles
    
    
    
class Generate_Bigdata_File:
    
    """
    Создает большой датасет на основе заявленных данных.
    """
    
    def __init__ (self, client, dataset_name, deep):        
        #Отправляем запрос - ДатасетГенератор
        dg = DatasetGenerator (client, dataset_name, deep)
        print ('Создаем датасет в Generate_Bigdata_File')
        #Получаем список актуальных названий файлов из папки
        actualfiles = dg.actualfiles
        #Вызываем менеджер файлов. Передаем имя клиента и имя датасета
        #И получаем путь к датасету, с который пойдет работа
        fm = FileManagerForDatasets (client, dataset_name)
        path = fm.path
        #Запускаем цикл по списку ActualFiles
        #Открываем в Пандас каждый из датасетов и контакенируем в единый файл
        bigdata = None
        for filelist in actualfiles:
            file = filelist[0]  
            try:
                data = pd.read_csv( os.path.join(path, file), sep=';', encoding="utf-8")
                bigdata = pd.concat([bigdata,data], ignore_index=True) 
            except:
                break
        #Класс возвращает сконтакнированный файл, на котором уже можно вести вычисления
        self.data = bigdata
        #fm.saveonefile(bigdata)        
      