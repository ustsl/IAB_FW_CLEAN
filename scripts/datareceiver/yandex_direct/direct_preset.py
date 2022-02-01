import os
from yaml import load
import requests
from requests.exceptions import ConnectionError
from time import sleep
import json
import pandas as pd
import numpy as np
import pandas as pd
import sys

class Direct_Token:
    
    def __init__ (self, client):
        self.direct_token = 0
        
        try:
            f = open(os.path.join('clients/'+client, 'config.yaml') , 'r')
            config = load(f)
            self.direct_token = config['direct_personal_token']
            f.close()     
   
        except KeyError:
            print ('Не найден токен директа в конфигурационном файле')
            
        except FileNotFoundError:
            print ('Не найден файл. Проверьте наличие клиента в датасетах')
            
            
            
            
class DirectCampaignStat:
    def __init__ (self, client, dates):
        self.dates = dates
        dt = Direct_Token(client)
        if dt.direct_token != 0:
            print ('Все ок. Начинаю отчет')
            self.token = dt.direct_token
            self.start_report()
        else:
            print ('Исправьте ошибку')
            
    def start_report (self):
        token = self.token
        dates = self.dates
        
        # Метод для корректной обработки строк 
        # в кодировке UTF-8 как в Python 3, так и в Python 2
        
        if sys.version_info < (3,):
            def u(x):
                try:
                    return x.encode("utf8")
                except UnicodeDecodeError:
                    return x
        else:
            def u(x):
                if type(x) == type(b''):
                    return x.decode('utf8')
                else:
                    return x


        ReportsURL = 'https://api.direct.yandex.com/json/v5/reports'

        headers = {
                   # OAuth-токен. Использование слова Bearer обязательно
                   "Authorization": "Bearer " + token,
                   # Логин клиента рекламного агентства
                   #"Client-Login": 'uem-66',
                   # Язык ответных сообщений
                   "Accept-Language": "ru",
                   # Режим формирования отчета
                   "processingMode": "auto"}

        # Создание тела запроса
        body = {
                "params": {
                    "SelectionCriteria": {
                          "DateFrom": dates[1],
                    "DateTo": dates[0]

                    },
                    "FieldNames": [
                        "CampaignName",
                        "Impressions",
                        "Clicks",
                        "Ctr",
                        "Cost",
                        "AvgCpc",
                        "BounceRate",
                        "AvgPageviews",
                        "ConversionRate",
                        "CostPerConversion",
                        "Conversions"
                    ],
                    "ReportName": u("Report4"),
                    "ReportType": "CAMPAIGN_PERFORMANCE_REPORT",
                    "DateRangeType":"CUSTOM_DATE",
                    "Format": "TSV",
                    "IncludeVAT": "NO",
                    "IncludeDiscount": "NO"
                }
            }

        # Кодирование тела запроса в JSON
        body = json.dumps(body, indent=4)
        req = requests.post(ReportsURL, body, headers=headers)
        req.encoding = 'utf-8'  # Принудительная обработка ответа в кодировке UTF-8
        file = open("cashe.csv", "w")
        file.write(req.text)
        file.close()
        if 'Незавершенная' in req.text:
            print ('Незавершенная регистрация приложения.')        
        
        self.data = pd.read_csv("cashe.csv",header=1, sep='	', encoding="1251").head(-1)


