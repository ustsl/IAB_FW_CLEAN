import os
from yaml import load

class FileManagerForDatasets:
    
    """
    Задача класса:
    1. поиск нужной папки
    2. отправление списка файлов в нужной папке
    3. сохранение присланных файлов в нужную папку
    """
    
    def __init__ (self, client, dataset):
        
        self.pathconfig = 'clients/'+client+'/datasets/'+dataset
        self.path = self.pathconfig +'/data'     
        
    def filelist (self):
        
        try:        
            self.files = os.listdir(self.path)            
        except OSError:
            print ('Папка не найдена')
            self.files = 'Указан несуществующий путь к данным'
    
    def savefile (self, dataset, filename):
        
        dataset.to_csv(os.path.join(self.path,
                                    filename), encoding='utf-8',
                       index=False, sep=";")
        print ('Файл с именем {}, сохранен в папку {}'.format(filename, self.path))
        
    def saveonefile (self, bigdata):
        
        bigdata.to_csv(os.path.join(self.pathconfig,
                                    'dataset.csv'), encoding='utf-8',
                       index=False, sep=";")
        print ('Текущий рабочий датасет сохранен в папку {}'.format(self.pathconfig)) 
        
    def openconfig (self):
        
        """
        Метод работы с конфигурационным файлом. 
        Открываем конфигурационный файл и отдаем словарь во внешку        
        """
        
        try:        
            with open(os.path.join(self.pathconfig, 'revconfig.yaml' ), 'r') as f:
                self.config = load(f)
        except OSError:
            print ('Нет доступа к файлу, или данным файла')
            
            
            
class TXT_saver:
    def __init__ (self, client, path):
        self.path = path
        
    def save (self, filename, file):
        my_file = open(os.path.join(self.path,
                                    filename), 'w')
        my_file.write(file)
        my_file.close()
        
        
def cleaner_func():
    data = 'data/'
    dir_list = os.listdir(data)

    for dir_item in dir_list:
        path = data+dir_item
        files = os.listdir(path)
        for file in files:
            os.remove(os.path.join(path, file))