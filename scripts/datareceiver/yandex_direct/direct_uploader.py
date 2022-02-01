import pandas as pd
from scripts.datareceiver.yandex_direct.direct_preset import DirectCampaignStat

class Direct_Data_Upload:
    
    def __init__ (self, client, dates):
        print ('Запускаю DirectCampaignStat с датами - {}'.format(dates))
        dct = DirectCampaignStat (client, dates)
        data = dct.data
        
        data['Campaign_cost'] = data.apply(lambda x: x['Cost'] / 1000000, axis=1)

        def CR_func (row):
            if row['ConversionRate'] == '--':
                return 0
            return float(row['ConversionRate'])

        data['ConversionRate'] = data.apply(CR_func, axis=1)
        data['startOfMonth'] = dates[1]
        dataset = data[['startOfMonth', 'CampaignName', 'Impressions', 'Clicks', 'Ctr', 
                        'Campaign_cost', 'BounceRate', 'AvgPageviews', 'ConversionRate']]
        
        self.data = dataset