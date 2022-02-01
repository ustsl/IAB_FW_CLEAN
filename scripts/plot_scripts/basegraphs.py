"""
Базовые зависимости под отрисовку графика
"""

import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns
import warnings; warnings.filterwarnings(action='once')
import squarify

large = 22; med = 16; small = 12
params = {'axes.titlesize': large,
          'legend.fontsize': med,
          'figure.figsize': (16, 10),
          'axes.labelsize': med,
          'axes.titlesize': med,
          'xtick.labelsize': med,
          'ytick.labelsize': med,
          'figure.titlesize': large}
plt.rcParams.update(params)
plt.style.use('seaborn-whitegrid')
sns.set_style("white")
#%matplotlib inline

# Version
print(mpl.__version__)  #> 3.0.0
print(sns.__version__)  #> 0.9.0

"""
Для доступа к файлами
"""

#Работа с папками и конфигами
import os
from yaml import load


#Игнорирование предупреждений
import warnings
warnings.filterwarnings("ignore")

"""
График отрисовки столбчатых диаграмм.

Пример использования:

GraphColumns (angio,
            df,
            'ctr',
            "CTR в группах запросов Я.Директ, имеющих статистический вес",
            '# CTR',
            'quarterly_query_groups_in_yandex_direct.png'
            'sort'
           )


"""

#Выводим график столбцами

import random

class GraphColumns:
    
    def __init__ (self, client, df, met, gtitle, glabel, gfilename, folder, sort):
        if sort == 'sort':
            df = df.sort_values(met, ascending=False)
            
        n = df[df.columns[0]].unique().__len__()+1

        all_colors = ['#419B82', '#3E8CA8', '#42B3B2',
                     '#42B373', '#3EA84E']           

        random.seed(100)
        c = random.choices(all_colors, k=n)

        # Plot Bars
        plt.figure(figsize=(16,10), dpi= 80)
        plt.bar(df[df.columns[0]], df[met], color=c, width=.5)
        for i, val in enumerate(df[met].values):
            plt.text(i, val, float(val), horizontalalignment='center', verticalalignment='bottom', fontdict={'fontweight':500, 'size':12})

        # Decoration
        plt.gca().set_xticklabels(df[df.columns[0]], rotation=50, horizontalalignment= 'right')
        plt.title(gtitle, fontsize=22)
        plt.ylabel(glabel,fontsize=32)
        
        plt.savefig((os.path.join('data/'+client, 
                                  gfilename)), transparent=False,
                                        bbox_inches='tight', pad_inches=0)   
        #plt.show()
        
########        
        
"""
График отрисовки накопительных диаграмм.

Пример использования:


Сumulative_Graph ('angio',
            'df',
            'Название графика',
            'quarterly_query_groups_in_yandex_direct.png'       
           )
"""



class Сumulative_Graph:
    
    def __init__ (self, client, df, texttitle, filename, folder):

            col = df.columns

            # Decide Colors 
            mycolors = ['#5FB4C9', '#5D89FC', '#575A63',
                     '#D0A996', '#FC7B5D', '#593340',
                       '#3D3A17', '#19180B', '#FF91B8', 
                        '#FFF36B', '#FFBDAB', '#9ACC60',
                       '#8BB35D']      

            # Draw Plot and Annotate
            fig, ax = plt.subplots(1,1,figsize=(16, 9), dpi= 80)
            columns = df.columns[1:]
            labs = columns.values.tolist()
            
            # Prepare data
            x  = df['date'].values.tolist()
            y0 = df[col[1]].values.tolist() 

            stackdata = [y0]
            for stack in range(2, len(col)):
                stackdata += [df[col[stack]].values.tolist()]
                                
            y = np.vstack(stackdata)

            
            #Задаем максимальный размер графика
            
            """
            Формула простая:
                1. ищем самое большое число в наборе
                2. складываем самые большие числа в наборе
                3. умножаем на небольшой коэфициент, чтобы задать запас сверху
            """
            
            maxnum = 1
            for listnums in y:
                bignum = 0
                for listnum in listnums:
                    if listnum > bignum:
                        bignum = listnum
                maxnum += bignum
            
            maxnum = maxnum * 1.2

            # Plot for each column
            labs = columns.values.tolist()
            ax = plt.gca()
            ax.stackplot(x, y, labels=labs, colors=mycolors, alpha=0.8)

            # Decorations
            ax.set_title(texttitle, fontsize=18)
            ax.set(ylim=[0, maxnum])
            ax.legend(fontsize=13, ncol=3)
            plt.xticks(x[1::2], fontsize=12, horizontalalignment='center')
            plt.yticks(fontsize=12)
            plt.xlim(x[0], x[-1])

            # Lighten borders
            plt.gca().spines["top"].set_alpha(0)
            plt.gca().spines["bottom"].set_alpha(.3)
            plt.gca().spines["right"].set_alpha(0)
            plt.gca().spines["left"].set_alpha(.3)

            plt.savefig((os.path.join('data/'+client, 
                                      filename)), 
                        transparent=False,
                                    bbox_inches='tight', pad_inches=0) 
            #plt.show()
        
        
########
        
"""
График отрисовки линейных диаграмм.

Пример использования:


Line_Graph ('angio',
            'df',
            'Название графика',
            'quarterly_query_groups_in_yandex_direct.png'       
           )
"""

class Line_Graph:
    
    def __init__ (self, client, datalines, name, filename, folder):
        # Decide Colors 
        mycolors = ['#5FB4C9', '#5D89FC', '#575A63',
                     '#D0A996', '#FC7B5D','#593340',
                       '#3D3A17', '#19180B', '#FF91B8', 
                        '#FFF36B', '#FFBDAB', '#9ACC60',
                       '#8BB35D']           

        datacols = datalines.columns[1:]

        # Draw Plot
        plt.figure(figsize=(16,10), dpi= 80)
            
        a=0
        for column in datalines.columns[1:]:
            plt.plot('date', column, data=datalines, linewidth = 4, label=column,
                     color=mycolors[a])
            a+=1
        plt.legend(fontsize=12, ncol=4)#.legend(loc='upper left')

        # Decoration
        xtick_location = datalines.index.tolist()[::2]
        xtick_labels = [x[-5:] for x in datalines.date.tolist()[::2]]
        plt.xticks(ticks=xtick_location, 
                   labels=xtick_labels, 
                   rotation=0, 
                   fontsize=12, 
                   horizontalalignment='center', 
                   alpha=.7)
        plt.yticks(fontsize=12, alpha=.7)
        plt.title(name, fontsize=22)
        plt.grid(axis='both', alpha=.3)

        # Remove borders
        plt.gca().spines["top"].set_alpha(0.0)    
        plt.gca().spines["bottom"].set_alpha(0.3)
        plt.gca().spines["right"].set_alpha(0.0)    
        plt.gca().spines["left"].set_alpha(0.3)   
        plt.savefig((os.path.join('data/'+client, 
                                  filename)),
                    transparent=False,
                    bbox_inches='tight', 
                    pad_inches=0) 
            
        #plt.show()        
        
        
  
"""
График отрисовки линии визитов с отказным трафиком. Фиксированный вид.
Передаются только датасеты.

Пример использования:

GoodBadLine ('angio',
            goodline,
            badline
           )
"""

class GoodBadLine:
    
    def __init__ (self, client, goodline, badline, name, folder):
        # Decide Colors 
        # Get the Peaks and Troughs
        data = goodline['visits'].values
        doublediff = np.diff(np.sign(np.diff(data)))
        peak_locations = np.where(doublediff == -2)[0] + 1

        doublediff2 = np.diff(np.sign(np.diff(-1*data)))
        trough_locations = np.where(doublediff2 == -2)[0] + 1

        # Draw Plot
        plt.figure(figsize=(16,10), dpi= 80)
        plt.plot('date', 'visits', data=goodline, color='#5FB4C9', 
                 label='Безотказный трафик', linewidth = 6)
        plt.plot('date', 'visits', data=badline, color='#FC7B5D', 
                 label='Отказный трафик')
        ax = plt.gca()
        ax.stackplot('date', 'visits', data=badline, color='#D0A996', alpha=0.3)
        

        plt.scatter(goodline.date[peak_locations], goodline.visits[peak_locations], 
                    marker="o", 
                    color='#5FB4C9', 
                    s=500, 
                    label='Взлеты')        
        maxsizedata = goodline.visits.max() + badline.visits.max()
        # Decoration
        ax.set(ylim=[0, maxsizedata ])
        xtick_location = goodline.index.tolist()[::2]
        xtick_labels = goodline.date.tolist()[::2]

        plt.xticks(ticks=xtick_location, labels=xtick_labels, rotation=50, 
                   fontsize=16, alpha=.7)
        plt.title("Годовая динамика трафика", fontsize=32)
        plt.suptitle("Данные показаны с учетом пласта отказных визитов на сайт", 
                     fontsize=20)
        
        plt.yticks(fontsize=20, alpha=.7)

        # Lighten borders
        plt.gca().spines["top"].set_alpha(.1)
        plt.gca().spines["bottom"].set_alpha(.3)
        plt.gca().spines["right"].set_alpha(.0)
        plt.gca().spines["left"].set_alpha(1.3)

        plt.legend(loc='upper left', ncol=3)
        plt.grid(axis='y', alpha=.3)      
        plt.savefig\
        ((os.path.join('data/'+client,  name)), 
         transparent=False,
         bbox_inches='tight', pad_inches=0)   
        print ('График динамики сохранен')
        
        
"""
График отрисовки двухлиний с разными Y-диапазонами.


Пример использования:

GoodBadLine ('angio',
            x,
            y1,
            y2,
            'Имя графика',
            'Имя слева'
            'Имя справа'
            'Имя файла'
           )
"""
        
class TwoLines:
    def __init__ (self, client, x, y1, y2, name, namey1, namey2, namegraph, folder):
        fig, ax1 = plt.subplots(1,1,figsize=(16,9), dpi= 80)
        ax1.plot(x, y1, color='#5D89FC', linewidth=4)
        # Plot Line2 (Right Y Axis)
        ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis
        ax2.plot(x, y2, color='#FC7B5D',  linewidth=4)

        # Decorations        
        ax1.set_xlabel('Линия года', fontsize=20)        
        ax1.set_ylabel(namey1, color='#5D89FC', fontsize=20)
        ax1.tick_params(axis='y', rotation=0, labelcolor='#5D89FC' )
        ax1.grid(alpha=.4)            

        # ax2 (right Y axis)
        ax2.set_ylabel(namey2, color='#FC7B5D', fontsize=20)
        ax2.tick_params(axis='y', labelcolor='#FC7B5D')        
        ax2.set_xticks(np.arange(0, len(x), 2))
        ax2.set_title(name, fontsize=30)
        fig.tight_layout()        

        #Сохранение графика
        plt.savefig((os.path.join('data/'+client, 
                                  namegraph+'.png')), 
                                    transparent=False, 
                                    bbox_inches='tight', 
                                    pad_inches=0)  
        print ('Графики визитов и конверсий сохранены')

        
class InterPolare:
    def __init__ (self, client, df, folder):
        # Prepare Data
        x = df.date
        y_returns = df.result

        plt.figure(figsize=(16,10), dpi= 80)

        plt.fill_between(x[0:], y_returns[0:], 0, where=y_returns[0:] >= 0, facecolor='#419b82', 
                         interpolate=True, alpha=0.7)
        plt.fill_between(x[0:], y_returns[0:], 0, where=y_returns[0:] <= 0, facecolor='#9C5541', 
                         interpolate=True, alpha=0.7)

        plt.gca().set_xticklabels(x, rotation=60, 
                                  fontdict={'horizontalalignment': 'center', 
                                            'verticalalignment': 'center_baseline'})

        plt.title("Объем страниц, показавших динамику по трафику", fontsize=22)
        plt.ylabel('Сумма измений по объему страниц')
        plt.grid(alpha=0.5)

        plt.savefig((os.path.join('data/'+client,  
                                  'strategy_interpolare_visitsdynamic.png')), transparent=False,
                    bbox_inches='tight', pad_inches=0)   


        #plt.show() 
        
        
        
class SquarifyGraph:
    def __init__ (self, client, df, metric, title, gfilename, folder):    
        # Prepare Data
        labels = df.apply(lambda x: str(x[0]) + "\n (" + str(x[metric]) + ")", axis=1)
        sizes = df[metric].values.tolist()
        colors = ['#5FB4C9', '#C9CFFF', '#FFCBA3', '#C9CFFF', '#FFD9BD',
                 '#E3FFFB', '#D4B572']

        # Draw Plot
        plt.figure(figsize=(12,8), dpi= 80)
        squarify.plot(sizes=sizes, label=labels, color=colors, alpha=.8)

        # Decorate
        plt.title(title)
        plt.axis('off')
        plt.savefig((os.path.join('data/'+client, 
                                  gfilename)), transparent=False,
                    bbox_inches='tight', pad_inches=0)   
        #plt.show()