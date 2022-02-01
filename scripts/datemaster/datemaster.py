import datetime
from datetime import datetime, timedelta
import dateutil.relativedelta
import calendar

class DateMaster:    
    """
    Выходные данные неизменно лежат в start, date.
    Вызываем либу, вызываем метод, получаем данные.
    04.02.2020
    """    
    def __init__ (self):
        dtnow = datetime.now() #текущий день        
        self.last_day_of_last_month = dtnow - timedelta (days = dtnow.day)
        # ^Последний день прошлого месяца^ 
        self.first_day_of_cur_month = dtnow - timedelta (days = dtnow.day-1)
        # ^Первый день текущего месяца^
        self.actual_date = dtnow - timedelta( days = 1) #Вчерашний день
        self.start = self.actual_date.strftime('%Y-%m-%d')

    def yesterday (self):        
        self.end = self.actual_date.strftime('%Y-%m-%d')
              
    def currentmonth(self):
        self.end = self.first_day_of_cur_month.strftime('%Y-%m-%d')      
          
    def lastmonth (self):
        self.start = self.last_day_of_last_month.strftime('%Y-%m-%d')
        self.end = self.start[:-3] + '-01'
        self.start_new_month = self.first_day_of_cur_month.strftime('%Y-%m-%d')
        
    def oneweek (self):
        three_weeks = self.actual_date - timedelta ( days = 6)
        self.end = three_weeks.strftime('%Y-%m-%d')
        
    def threeweeks (self):
        three_weeks = self.actual_date - timedelta ( days = 20)
        self.end = three_weeks.strftime('%Y-%m-%d')
        
    def year(self):
        self.lastmonth()
        lastyear = \
        self.last_day_of_last_month - dateutil.relativedelta.relativedelta(months=11)
        lastyear = lastyear.strftime('%Y-%m')
        self.start = lastyear + '-01'        
        
    def quarter(self):
        self.lastmonth()
        quarter = \
        self.last_day_of_last_month - dateutil.relativedelta.relativedelta(months=2)
        quarter = quarter.strftime('%Y-%m')
        self.start = quarter + '-01'
        
        
class MonthList:
    def __init__ (self, month):
        self.startdate_strptime = datetime.strptime(month, '%m.%Y' )
        self.refactoring()
    def refactoring (self):
        self.datalist = []
        for x in range (0, 13):
            md = self.startdate_strptime - dateutil.relativedelta.relativedelta(months=x)
            str_md = md.strftime( '%m.%Y' )     
            self.datalist += [str_md]


class LastDayOfMonth:
    """
    Класс собирает даты в нужном формате
    """
    def __init__ (self, indate):
        strdates = indate.split('.')
        if strdates[0][0] == '0':            
            month = int(strdates[0][1])   
        else:
            month = int(strdates[0])
        year = int(strdates[1])
        
        lastdateofmonth = str(list(calendar.monthrange(year,month))[1])
        
        if len(str(month)) == 1:
            month = '0'+str(month)
        template = str(year) + '-' + str(month) + '-'
        
        print (template)
        self.dates = [template+lastdateofmonth, template+'01']         

        
        
class DaysListGenerator:
    """
    Принимает лист со стартовой и начальными датами
    Отдает datelist
    """
    def __init__ (self, dates):
        actual = datetime.strptime(dates[0], '%Y-%m-%d' )
        self.dates = []

        while actual != datetime.strptime(dates[1], '%Y-%m-%d') + timedelta ( days = 1):
            self.dates += [actual.strftime('%d.%m.%Y')]
            actual = actual  + timedelta ( days = 1)
            #Защита
            if len(dates) > 366:
                break
       
        