import pandas as pd

class Means:
    def __init__ (self, df):   
        """
        Функция для развертывания sum-данных
        """        
        try:
            df['duration'] = \
            df.apply(lambda x: int(x['sumduration'] / x['visits'] * 100) / 100, axis=1)
        except:
            None
            
        try:
            df['depth'] = \
            df.apply(lambda x: int(x['sumdepth'] / x['visits'] * 100) / 100, axis=1)
        except:
            None
            
        try:    
            df['ctr'] = \
            df.apply(lambda x: int(x['conversions'] / x['visits'] * 10000) / 100, axis=1)
        except:
            None
        try:
            del (df['sumduration'])
        except:
            None
            
        try:
            del (df['sumdepth'])
        except:
            None
        self.df = df                  