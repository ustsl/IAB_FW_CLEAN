#Класс управления блоком Datareceiver. Все запросы шлем через него.

"""
Класс не вызывается напрямую. Работает через класс DatasetGenerator,
расположенный по адресу scripts.dataset_generator.

Задача класса:
1. считать отправляемый конфигурационный файл отчета и определить тип отчета
2. в зависимости от типа отчета запустить соответствующие методы для сбора данных по API.

По системному соглашению все запросы к данным кроме YWM и Калибри (под вопросом)
следует отправлять через этого класс.

ПРИМЕЧАНИЕ
Класс работает только с помесячными выборками данных
"""

from scripts.datemaster.datemaster import *

class DataReceiver:

    """
    Класс принимает характеристики запроса по датасету.
        И на основании данных запускает сбор информации.
    На выходе отдает dataset
    """

    def __init__ (self, month, client, config):
        print (month)
        ld = LastDayOfMonth(month)
        
        self.date = ld.dates
        self.client = client
        self.config = config
        
        parse_type = config['parse_type']
        if parse_type == 'yandex_metrika':
            self.yandex_metrika_uploader()
        if parse_type == 'google_analytics':
            self.google_analytics_uploader()
        if parse_type == 'yandex_direct':
            self.yandex_direct_uploader()

    def yandex_metrika_uploader(self):
        #Получение данных посредством АПИ Яндекс Метрики
        from scripts.datareceiver.yandex_metrika.data_upload import Data_upload
        config = self.config
        dimensions = config['dimensions']
        metrics = config['metrics']
        preset = [dimensions, metrics]
        filters = config['filters']

        params = {'client': self.client,
                'date': self.date,
                'preset': preset,
                'filters': filters}
        #Запрос
        du = Data_upload (params)
        self.dataset = du.dataset

    def google_analytics_uploader(self):
        #Получение данных посредством АПИ Google Analytics
        from scripts.datareceiver.google_analytics.gomepanda import GoMePanda
        config = self.config
        gparams = config['gparams']
        get = GoMePanda (self.date, self.client, gparams)
        get.work()
        self.dataset = get.data

    def yandex_direct_uploader(self):
        dates = self.date
        from scripts.datareceiver.yandex_direct.direct_uploader import Direct_Data_Upload
        print ('Запускаю scripts.datareceiver.yandex_direct.direct_uploader Direct Data Upload')
        dd = Direct_Data_Upload (self.client, dates)
        self.dataset = dd.data
