#Передаем датасет и параметры
#Вычисляем оценку по пятибальной шкале
#Отдаем датасет с новым столбцом

class Universal_Estimate:
    
    #Универсальный класс, создающий взвешенную оценку по заданным признакам.    
    def __init__ (self, data, params):

        #params = [['duration', '30', '15'], ...]
        
        def estimate_func (row):
            
            result = 0
            maximum = 0
            
            for param in params:
                maximum += param[1]
                if row[param[0]] > data[param[0]].median():
                    result += param[1]
                elif row[param[0]] > data[param[0]].median() / 1.5:
                    result += param[2]

            return int(50 / maximum * result) / 10
        
        data['estimate'] = data.apply(estimate_func, axis=1)
        print ('UNIVERSAL ESTIMATE. Данные в контейнере "data"')
        self.data = data