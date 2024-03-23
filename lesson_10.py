import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import telegram
import pandahouse as ph
from datetime import date
from datetime import datetime, timedelta
import io
import sys
import os

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20240120'
}


default_args = {
    'owner': 'k-gorbenko',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 2, 24),
}

schedule_interval = '*/15 * * * *'

chat_id = -938659451
    
    
def check_anomaly(df_alert, metrics, a = 2.5, n = 8):
    df_alert['q25'] = df_alert[metrics].shift(1).rolling(n).quantile(0.25)
    df_alert['q75'] = df_alert[metrics].shift(1).rolling(n).quantile(0.75)
    df_alert['IQR'] = df_alert['q75'] - df_alert['q25']
    df_alert['upper_border'] = df_alert['q75'] + a * df_alert['IQR']
    df_alert['lower_border'] = df_alert['q25'] - a * df_alert['IQR']
    
    df_alert['upper_border'].rolling(n, center = True).mean()
    df_alert['lower_border'].rolling(n, center = True).mean()
        
    if df_alert[metrics].iloc[-1] < df_alert['lower_border'].iloc[-1] or df_alert[metrics].iloc[-1] > df_alert['upper_border'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
    return is_alert, df_alert
    
def run_alert(chat_id):
    my_token = '6855618149:AAEt2VoHrMdHlmSiyKMmJLbSy0iMsw-Bb4I'
    bot = telegram.Bot(token=my_token)
    query = ''' select *
        from (select toDate(time) as day, toStartOfFifteenMinutes(time) as begin_15,
                     formatDateTime(toStartOfFifteenMinutes(time), '%R') as every_15_minutes,
                     uniqExact(user_id) as active_users_news, countIf(action = 'like') as likes,
                     countIf(action = 'view') as views, likes/views as CTR
              from simulator_20240120.feed_actions
              where toStartOfFifteenMinutes(time) < toStartOfFifteenMinutes(now()) and day >= today() - 1
              group by day, begin_15, every_15_minutes
              order by day, begin_15, every_15_minutes) s1

              full join

             (select toDate(time) as day, toStartOfFifteenMinutes(time) as begin_15,
                     formatDateTime(toStartOfFifteenMinutes(time), '%R') as every_15_minutes,
                     uniqExact(user_id) as active_users_messages, count(user_id) as count_messages
              from simulator_20240120.message_actions
              where toStartOfFifteenMinutes(time) < toStartOfFifteenMinutes(now()) and day >= today() - 1
              group by day, begin_15, every_15_minutes
              order by day, begin_15, every_15_minutes) s2 using day, begin_15, every_15_minutes '''
    data = ph.read_clickhouse(query, connection=connection)
    
    list_of_metrics = ['active_users_news', 'likes', 'views', 'CTR', 'active_users_messages', 'count_messages']
    
    for metrics in list_of_metrics:
        df_alert = data[['day', 'begin_15', 'every_15_minutes', metrics]].copy()
        is_alert, df_alert = check_anomaly(df_alert, metrics)
            
        if is_alert == 1:
            msg = '''Метрика {metrics}:\n Текущее значение {current_val:.2f}\nотклонение от предыдущего значения {last_val_diff:.2%}'''.format(metrics = metrics, current_val = df_alert[metrics].iloc[-1], last_val_diff = 1 - (df_alert[metrics].iloc[-1]/df_alert[metrics].iloc[-2]))
            sns.set(rc={'figure.figsize': (16, 10)})

            plt.tight_layout
                
            ax = sns.lineplot(x=df_alert['begin_15'], y = df_alert[metrics], label = 'metric')
            ax = sns.lineplot(x=df_alert['begin_15'], y = df_alert['upper_border'], label = 'up')
            ax = sns.lineplot(x=df_alert['begin_15'], y = df_alert['lower_border'], label = 'low')
                    
            for i, label in enumerate(ax.get_xticklabels()):
                if i % 15 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)
                    
            ax.set(xlabel = 'Time')
            ax.set(ylabel = metrics)
            ax.set_title(metrics)
            ax.set(ylim=(0, None))
                    
            plot_object = io.BytesIO()
            plt.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = 'Metric.png'
            plt.close()
                    
            bot.sendMessage(chat_id=chat_id, text =msg)
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)
            
    return
    
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def gorbenko_alert():
    @task
    def make_report():
        run_alert(chat_id)
    make_report()
    
gorbenko_alert = gorbenko_alert()