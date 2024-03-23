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
    'start_date': datetime(2024, 2, 20),
}

schedule_interval = '0 11 * * *'

my_token = '6855618149:AAEt2VoHrMdHlmSiyKMmJLbSy0iMsw-Bb4I'
bot = telegram.Bot(token=my_token)

chat_id = -938659451

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def gorbenko_report_num_2():
    
    @task
    def extract_retention():
        q_1 = '''
        with s1 as(SELECT user_id, toDate(time) as day
                  FROM simulator_20240220.feed_actions sf
                  JOIN simulator_20240220.message_actions sm ON sf.time = sm.time)

        select day, start_day, count(user_id) as users
        from (SELECT *
              FROM (SELECT user_id, min(day) as start_day
                    FROM s1
                    GROUP BY user_id) t1
              JOIN
                    (SELECT DISTINCT user_id, toDate(day) as day
                     FROM s1) t2 using user_id
              WHERE start_day = today() - 7)
        group by day, start_day
        order by day
        '''
        extract_retention = ph.read_clickhouse(q_1, connection = connection)
        return extract_retention
    
    @task
    def extract_source():
        q_2 = '''
        SELECT toDate(time) as date, count(distinct user_id) as users, source
        FROM simulator_20240220.feed_actions sf
        JOIN simulator_20240220.message_actions sm ON sf.user_id = sm.user_id
        where toDate(time) >= today() - 7
        group by date, source
        order by date
        '''
        
        extract_source = ph.read_clickhouse(q_2, connection = connection)
        return extract_source
    
    @task
    def extract_os():
        q_3 = '''
        select count(distinct user_id) as users, os, day
        from (SELECT user_id, toDate(time) as day, os
              FROM simulator_20240220.feed_actions sf
              JOIN simulator_20240220.message_actions sm ON sf.time = sm.time)
        where day >= today() - 7
        group by day, os
        order by day
        '''
        extract_os = ph.read_clickhouse(q_3, connection = connection)
        return extract_os
    
    @task
    def extract_mess_news():
        q_4 = '''
        SELECT time,
       countIf(DISTINCT user_id, post_id != 0 and receiver_id = 0) as only_news,
       countIf(DISTINCT user_id, post_id = 0 and receiver_id != 0) as only_messenger,
       countIf(DISTINCT user_id, post_id != 0 and receiver_id != 0) as news_and_messengers
        FROM (SELECT user_id, post_id, toDate(toStartOfDay(time)) AS time, gender,
             os, country, source
              FROM simulator_20240120.feed_actions) AS t1
    
            FULL JOIN 
    
             (SELECT user_id, receiver_id, toDate(toStartOfDay(time)) AS time
              FROM simulator_20240120.message_actions ) AS t2
            using (user_id, time)
        where time = yesterday()
        group by time
        '''
        extract_mess_news = ph.read_clickhouse(q_4, connection = connection)
        return extract_mess_news
    
    
    @task
    def load_graph_retention(extract_retention, chat_id):
        plt.figure(figsize=(10, 6))
        sns.barplot(x='day', y='users', data=extract_retention, color='orange')
        plt.xlabel('День')
        plt.ylabel('Количество пользователей')
        plt.title(f"Retention за 7 дней от {extract_retention['day'].min()}")
        plt.grid(color='red', alpha=0.2)
        sns.set_style('whitegrid')
        plt.show()
        
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'Retention.png'
        plt.close()
        
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
    @task
    def load_graph_source(extract_source, chat_id):
        plt.figure(figsize=(12, 8))
        sns.lineplot(x='date', y='users', hue='source', data=extract_source)
        plt.xlabel('День')
        plt.ylabel('Количество пользователей, использующие приложение целиком')
        plt.title('График количества пользователей по source за 7 дней')
        plt.legend(title='Источник')
        plt.grid(axis='y', color='gray', linestyle='--', linewidth=0.5)
        plt.show()
        
        plot_object_1 = io.BytesIO()
        plt.savefig(plot_object_1)
        plot_object_1.seek(0)
        plot_object_1.name = 'Source.png'
        plt.close()
        
        bot.sendPhoto(chat_id=chat_id, photo=plot_object_1)
        
    @task
    def load_graph_os(extract_os, chat_id):
        plt.figure(figsize=(12, 8))
        sns.lineplot(x='day', y='users', hue='os', data=extract_os)
        plt.xlabel('День')
        plt.ylabel('Количество пользователей, использующие приложение целиком')
        plt.title('График количества пользователей по os за 7 дней')
        plt.legend(title='Операционная система')
        plt.grid(axis='y', color='red', linestyle='--', linewidth=0.5)
        plt.show()
        
        plot_object_2 = io.BytesIO()
        plt.savefig(plot_object_2)
        plot_object_2.seek(0)
        plot_object_2.name = 'Os.png'
        plt.close()
        
        bot.sendPhoto(chat_id=chat_id, photo=plot_object_2)
        
    @task
    def load_graph_mess_news(extract_mess_news, chat_id):
        extract_mess_news_melted = pd.melt(extract_mess_news, id_vars=['time'], value_vars=['only_news', 'only_messenger', 'news_and_messengers'], var_name='interaction')

        plt.figure(figsize=(10, 6))
        sns.barplot(x='time', y='value', hue='interaction', data=extract_mess_news_melted)
        plt.xlabel('День')
        plt.ylabel('Количество пользователей')
        plt.title(f"Взаимодействие сервисов от {extract_mess_news['time'].min()}")
        plt.grid(color='red', alpha=0.2)
        sns.set_style('whitegrid')
        plt.show()
        
        plot_object_3 = io.BytesIO()
        plt.savefig(plot_object_3)
        plot_object_3.seek(0)
        plot_object_3.name = 'Services.png'
        plt.close()
        
        bot.sendPhoto(chat_id=chat_id, photo=plot_object_3)
        
    extract_retention = extract_retention()
    extract_source = extract_source()
    extract_os = extract_os()
    extract_mess_news = extract_mess_news()
    load_graph_retention(extract_retention, chat_id)
    load_graph_source(extract_source, chat_id)
    load_graph_os(extract_os, chat_id)
    load_graph_mess_news(extract_mess_news, chat_id)
    
gorbenko_report_num_2 = gorbenko_report_num_2()