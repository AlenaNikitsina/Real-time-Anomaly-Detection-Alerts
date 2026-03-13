import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import telegram
import pandahouse as ph
from datetime import date
import io
import sys
import os
from airflow.decorators import dag, task
from datetime import datetime, timedelta, date

#Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'elena-prihodko',
    'depends_on_past': False,
    'retries': 1, 
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 2, 5),
    }

#Интервал запуска DAG (МСК)
schedule_interval = '0,15,30,45 * * * *'


connection = {
    'host': 'http://clickhouse.lab.karpov.courses:8123',
    'database':'simulator_20251220',
    'user':'student',
    'password':'...'
}
            
 #извлекаем данные из таблицы за вчера и сегодня с 15минутным интервалом: активные пользователи в ленте / мессенджере, просмотры, лайки, CTR, количество отправленных сообщений. 
q='''Select ts
    ,date
    ,hm
    ,users_feed,views, likes,ctr
    ,users_message, count_message
    From
        (SELECT toStartOfFifteenMinutes(time) as ts
            , toDate(toStartOfFifteenMinutes(time)) as date
            , formatDateTime(toStartOfFifteenMinutes(time), '%R') as hm
            , uniqExact(user_id) as users_feed
            , countIf(user_id, action='view') as views
            ,countIf(user_id, action='like') as likes
            ,CASE 
                WHEN countIf(user_id, action='view') > 0 
                THEN ROUND(countIf(user_id, action='like') * 100.0 / countIf(user_id, action='view'), 3)
                ELSE 0 
             END as ctr
        FROM simulator_20251220.feed_actions
        WHERE time >=  today() - 1 and time < toStartOfFifteenMinutes(now())
        GROUP BY ts,date, hm) t1
    FULL JOIN    
        (Select toStartOfFifteenMinutes(time) as ts
            , toDate(toStartOfFifteenMinutes(time)) as date
            , formatDateTime(toStartOfFifteenMinutes(time), '%R') as hm
            , uniqExact(user_id) as users_message
            , count(receiver_id) as count_message
        From simulator_20251220.message_actions
        WHERE time >=  today() - 1 and time < toStartOfFifteenMinutes(now())
        GROUP BY ts,date, hm) t2
        USING (ts, date, hm)
        Order by ts

'''

def check_anomaly(df, metric, a=4,n=5): 
    # функция check_anomaly предлагает алгоритм проверки значения на аномальность в данных (межквартальный размах)
    df['q25']= df[metric].shift(1).rolling(n).quantile(0.25) # shift - чтобы значения границ текущих 15-тиминуток не повлияли, сдвигаем на 1 период
    df['q75']=df[metric].shift(1).rolling(n).quantile(0.75)
    df['iqr']= df['q75']-df['q25'] #значение межквартильного размаха
    df['up']=  df['q75'] + a*df['iqr'] #верхняя граница 
    df['low']= df['q25'] - a*df['iqr'] #нижняя граница
    
    #сглаживаем данные
    df['up']=df['up'].rolling(n, center=True, min_periods=1).mean()
    df['low']=df['low'].rolling(n, center=True, min_periods=1).mean()
    
    if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1]>df['up'].iloc[-1]:
        is_alert=1
    else:
        is_alert=0
    return is_alert, df
    
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def prihodko_alerts():
    
     # Получение данных из базы данных
    @task()
    def extract_df(q, connection):
        data = ph.read_clickhouse(query=q, connection=connection)
        return data
    
    
    @task()
    def run_alerts(data,chat_id=None): #система алертов
        chat_id = ....
        bot = telegram.Bot(token='.....')

        metrics_list=['users_feed','views','likes','ctr','users_message', 'count_message']
        
        for metric in metrics_list:
            print(metric)
            df=data[['ts','date','hm',metric]].copy()
            is_alert, df = check_anomaly(df, metric)

            if is_alert==1:
                msg = '''Метрика {metric}:\n текущее значение = {current_val:.2f}\nотклонение от предыдущего значения {last_val_diff:.2%} \nГрафики с основными метриками:https://superset.lab.karpov.courses/superset/dashboard/8239/'''.format(metric=metric, current_val=df[metric].iloc[-1],last_val_diff=1-(df[metric].iloc[-1]/df[metric].iloc[-2]))
                sns.set(rc={'figure.figsize': (16, 10)}) # задаем размер графика
                plt.tight_layout()

                # строим линейный график
                ax = sns.lineplot(x=df['ts'], y= df[metric], label='metric')
                ax = sns.lineplot(x=df['ts'], y= df['up'], label='up')
                ax = sns.lineplot(x=df['ts'], y= df['low'], label='low')

                for ind, label in enumerate(ax.get_xticklabels()): # этот цикл нужен чтобы разрядить подписи координат по оси Х,каждый второй показывать
                    if ind % 2 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)

                ax.set(xlabel='time') # задаем имя оси Х
                ax.set(ylabel=metric) # задаем имя оси У

                ax.set_title(metric) # задае заголовок графика
                ax.set(ylim=(0, None)) # задаем лимит для оси У, от 0

                # формируем файловый объект
                plot_object = io.BytesIO()
                ax.figure.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = '{0}.png'.format(metric)
                plt.close()

                bot.sendMessage(chat_id=chat_id, text=msg)
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)
                
            
    data = extract_df(q, connection)
    run_alerts(data,chat_id=None)

prihodko_alerts = prihodko_alerts()