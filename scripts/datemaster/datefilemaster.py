from datetime import datetime
from dateutil.relativedelta import relativedelta

class DateFileNow: 
    
    """
    Универсальный класс для генерации имен файлов 
        и даты для запросов в системы получения 
        сырой даты по API.
    """
    
    def __init__ (self, deep):
        #Прибавляем к глубине единицу, чтобы получить в цикле нужный размер выборки
        self.deep = deep + 1 
        self.filelist()
            
    def filelist (self):
        
        self.actualfiles = []
        
        for x in range (1, self.deep):
            cycle_date = datetime.today()+ relativedelta(months=-x)
            filedate = cycle_date.strftime('%Y-%m')    
            parsedate = cycle_date.strftime('%m.%Y') 
            self.actualfiles += [[str(filedate)+'.csv', parsedate]]