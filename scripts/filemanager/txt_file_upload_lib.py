import os

class Write_TXT:
    def __init__ (self, ftppath, file, data):        
        f = open((os.path.join(ftppath, file)), 'w', encoding="utf-8")
        f.write(data)
        f.close()
        
#client = 'smt'
#ftppath = 'clients/'+client + '/ready_data/graphs/base'      
#Write_TXT(ftppath, 'test.txt', er.txt)