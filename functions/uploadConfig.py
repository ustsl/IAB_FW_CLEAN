from yaml import load
import os


def config_func (client):
    
    # Загрузка конфига
    f = open(os.path.join('settings', 'config.yaml') , 'r')
    config = load(f)
    token = config['metrikatoken']
    f.close()        
    
    try:
        f = open(os.path.join('clients/'+client, 'config.yaml') , 'r')
        config = load(f)
        counter = str(config['yandex_counter'])
        f.close()
        return [token, counter]
            
    except:
        error = 'Клиент не найден'
        print (error)   
        return error