from ftplib import FTP  
from yaml import load
import os

class Connect_FTP:
    def __init__ (self):        
        #Конфиги
        f = open(os.path.join('settings', 'config.yaml') , 'r')
        config = load(f)
        ftp_url = config['ftp_url']
        ftp_login = config['ftp_login']
        ftp_pass = config['ftp_pass']
        f.close()    
        #Коннект
        try:
            ftp = FTP(ftp_url)  
            ftp.login(ftp_login,ftp_pass)         
            self.ftp = ftp   
            print ('Соединение установлено')
        except:
            print ('Нет соединения с сервером')              

class Upload:
    def __init__ (self, ftp, client, path, file_name):
        ftp.cwd('/data/'+client)
        ftp.storbinary('STOR ' + file_name, 
                       open(os.path.join(path, file_name), 'rb'))   
        
        
class Delete_Files:
    def __init__ (self):

        cf = Connect_FTP()
        df = Directory_FTP (cf.ftp)
        dirs = df.dirs
        ftp = cf.ftp

        for directory in dirs:
            ftp = cf.ftp
            ftp.cwd('/data/'+directory)    
            # empty list that will receive all the log entry
            log = [] 
            # we pass a callback function bypass the print_line that would be called by retrlines
            # we do that only because we cannot use something better than retrlines
            ftp.retrlines('LIST', callback=log.append)
            # we use rsplit because it more efficient in our case if we have a big file
            files = (line.rsplit(None, 1)[1] for line in log)
            # get you file list
            files_list = list(files)
            for file in files_list:
                print (directory)
                print (file)    
                try:
                    ftp.delete(file)
                except:
                    print ('ошибка. строка 56. ftp manager')
                
        
class Directory_FTP:
    
    def __init__ (self, ftp):
        self.dirs = []
        self.ftp = ftp
        self.list_dir_ftp()
        print ('Каталоги сервера: {}'.format(self.dirs))     
        
    def list_dir_ftp (self):
        ftp = self.ftp
        ftp.cwd('/data')
        dir_list = []
        ftp.dir(dir_list.append)
        dirs = []
        for line in dir_list:
            dirs += [line[29:].strip().split(' ')[-1]]
        self.dirs = dirs 
        
    def dirs_walker (self, client, path, filename):        
        if client not in self.dirs:
            print ('Нет клиента. Создаем папку')
            self.ftp.mkd(client)  
            print ('Директория создана')
        print ('Загружаю')
        Upload (self.ftp, client, path, filename)

#Пример синтаксиса
"""
cf = Connect_FTP()
df = Directory_FTP (cf.ftp)
df.dirs_walker('proverka', '', 'test.png')
"""