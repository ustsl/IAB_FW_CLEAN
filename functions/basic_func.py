from urllib import parse


def ctr_inner_func (row):
    try:
        result = row['conversions'] / row['visits'] * 100
        if result > 100:
            return 100
        else:
            return float('{:.2f}'.format(result))
    except:
        return 0 

#Работа с чистыми е-ком заявками    
def clean_ctr_inner_func (row):
    try:
        result = row['clean_conversions'] / row['visits'] * 100
        if result > 100:
            return 100
        else:
            return float('{:.2f}'.format(result))
    except:
        return 0      
    
    
def duration_func (row):
    return int(row['sumduration'] / row['visits']* 100) / 100   


def depth_func (row):
    return int(row['sumduration'] / row['visits']* 100) / 100   

    
def expand_func (data):
    data['duration'] = \
    data.apply(lambda x: int(x['sumduration']/ x['visits'] * 100) / 100,
               axis=1)
    data['depth'] = \
    data.apply(lambda x: int(x['sumdepth']/ x['visits'] * 100) / 100,
               axis=1)   
    data['ctr'] = data.apply(ctr_inner_func, axis=1)    

    #удаляем промежуточное
    del (data['sumdepth'])
    del (data['sumduration'])  
    
    if 'sumbounce' in list(data):
        data['bounce'] = data.apply\
        (lambda x: int(x['sumbounce']/ x['visits'] * 100) / 100,
         axis=1)     
        del(data['sumbounce'])

    return data    


def url_clean_func (row):
    parsed = parse.urlsplit(row['startURL'])
    return parsed.netloc + parsed.path


def sumdur_func (row):
    try:
        return row['visits'] * row['avgVisitDurationSeconds']
    except:
        return 0
    
            
def sumdepth_func (row):
    try:
        return row['visits'] * row['pageDepth']
    except:
        return 0     
    

def sumbounce_func (row):
    try:
        return row['visits'] * row['bounceRate']
    except:
        return 0            
