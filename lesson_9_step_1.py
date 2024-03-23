import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt
import pandahouse as ph
import seaborn as sns
from datetime import datetime, timedelta
import io
import telegram
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
    'start_date': datetime(2024, 2, 19),
}

schedule_interval = '0 11 * * *'

my_token = '6855618149:AAEt2VoHrMdHlmSiyKMmJLbSy0iMsw-Bb4I'
bot = telegram.Bot(token=my_token)

chat_id = -938659451

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def gorbenko_report_num_1():
    
    @task
    def ext_yesterday():
        q_1 = '''
        select toDate(time) as day,
               uniq(user_id) as dau,
               countIf(action = 'view') as views,
               countIf(action = 'like') as likes,
               likes/views as ctr
        from simulator_20240120.feed_actions
        where toDate(time) = yesterday()
        group by day
        '''
        report_yesterday = ph.read_clickhouse(q_1, connection = connection)
        return report_yesterday
    
    @task
    def ext_week():
        q_2 = '''
        select toDate(time) as day,
               uniq(user_id) as dau,
               countIf(action = 'view') as views,
               countIf(action = 'like') as likes,
               likes/views as ctr
        from simulator_20240120.feed_actions
        where toDate(time) >= today() - 7
        group by day
        '''
        
        report_week = ph.read_clickhouse(q_2, connection = connection)
        return report_week
    
    @task
    def load_message_report_day(report_yesterday, chat_id):
        dau = report_yesterday['dau'].sum()
        views = report_yesterday['views'].sum()
        likes = report_yesterday['likes'].sum()
        ctr = report_yesterday['ctr'].sum()
        day = report_yesterday['day'].max()
    
        message = \
        f'Ключевые метрики за {day}:\n' + \
        f'dau = {dau}\n' + \
        f'views = {views}\n' + \
        f'likes = {likes}\n' + \
        f'ctr = {ctr}'
    
        bot.sendMessage(chat_id=chat_id, text=message)
        
    @task
    def load_graph_week(report_week, chart_id):
        fig, ax = plt.subplots(4, 1, figsize=(15, 20))

        fig.suptitle('Динамика за последние 7 дней', fontsize=30)

        sns.lineplot(ax = ax[0], data = report_week, x = 'day', y = 'dau', color = 'red')
        ax[0].set_title('DAU')
        ax[0].grid()

        sns.lineplot(ax = ax[1], data = report_week, x = 'day', y = 'views', color = 'blue')
        ax[1].set_title('Views')
        ax[1].grid()

        sns.lineplot(ax = ax[2], data = report_week, x = 'day', y = 'likes', color = 'orange')
        ax[2].set_title('Likes')
        ax[2].grid()

        sns.lineplot(ax = ax[3], data = report_week, x = 'day', y = 'ctr', color = 'gold')
        ax[3].set_title('CTR')
        ax[3].grid()

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'Metrics.png'
        plt.close()

        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
    report_yesterday = ext_yesterday()
    report_week = ext_week()
    load_message_report_day(report_yesterday, chat_id)
    load_graph_week(report_week, chat_id)
    
gorbenko_report_num_1 = gorbenko_report_num_1()