from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
from io import StringIO
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20240120',
                      'user':'student',
                      'password':'dpo_python_2020'
                      }

connection_test = {'host': 'https://clickhouse.lab.karpov.courses',
                  'database':'test',
                  'user':'student-rw', 
                  'password':'656e2b0c9c'}



default_args = {
    'owner': 'k-gorbenko',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes = 5),
    'start_date': datetime(2024, 2, 14)
}

schedule_interval = '0 22 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup = False)
def k_gorbenko_update_new():
    
    @task()
    def extract_count_views_likes():
        q = '''
        select toDate(time) as event_date, user_id, countIf(action = 'view') as views, countIf(action = 'like') as likes,
               gender, age, os
        from simulator_20240120.feed_actions
        where toDate(time) = yesterday()
        group by user_id, event_date, gender, age, os
        '''
        df_count_views_likes = ph.read_clickhouse(q, connection=connection)
        return df_count_views_likes
    
    @task
    def extract_messages():
        q_2 = '''
        select event_date, user_id, gender, age, os, messages_sent, users_sent, messages_received, users_received
        from
            (select toDate(time) as event_date, user_id, count(receiver_id) as messages_sent,
                    uniq(receiver_id) as users_sent, gender, age, os
            from simulator_20240120.message_actions
            where toDate(time) = yesterday()
            group by user_id, event_date, gender, age, os) sub1
        left join
            (select toDate(time) as event_date, receiver_id, count(user_id) as messages_received,
                    uniq(user_id) as users_received
             from simulator_20240120.message_actions
             where toDate(time) = yesterday()
             group by receiver_id, event_date) sub2
        on sub1.user_id = sub2.receiver_id
        '''
        df_messages = ph.read_clickhouse(q_2, connection=connection)
        return df_messages
    
    @task
    def transform_merge(df_count_views_likes, df_messages):
        df_count_views_likes_messages = df_count_views_likes.merge(df_messages, how = 'left', 
                                                                   left_on = ['event_date','user_id','gender','age', 'os'],
                                                        right_on = ['event_date','user_id','gender','age', 'os']).fillna(0)
        return df_count_views_likes_messages
    
    @task
    def transform_os(df_count_views_likes_messages):
        os_df = df_count_views_likes_messages.drop(['user_id', 'gender', 'age'], axis = 1).groupby(['event_date', 'os']) \
                                             .sum().astype('int64')
        return os_df
    
    @task
    def transform_gender(df_count_views_likes_messages):
        gender_df = df_count_views_likes_messages.drop(['user_id', 'os', 'age'], axis = 1).groupby(['event_date', 'gender']) \
                                             .sum().astype('int64')
        return gender_df
    
    @task
    def transform_age(df_count_views_likes_messages):
        age_df = df_count_views_likes_messages.drop(['user_id', 'os', 'gender'], axis = 1).groupby(['event_date', 'age']) \
                                             .sum().astype('int64')
        return age_df
    
    @task
    def transform_concat(os_df, gender_df, age_df):
        concat_df = pd.concat([os_df, gender_df, age_df],
                      keys=['os', 'gender', 'age'],
                      names=['dimension', 'event_date', 'dimension_value']).swaplevel(0,1).reset_index()
        return concat_df
                              
    @task
    def load_concat(concat_df):
        q_test_table = '''
            CREATE TABLE IF NOT EXISTS test.ETL_gorbenko_update_new
               (event_date Date,
                dimension String,
                dimension_value String,
                views Int64,
                likes Int64,
                messages_received Int64,
                messages_sent Int64,
                users_received Int64,
                users_sent Int64
                ) 
                ENGINE MergeTree()
                order by event_date
            '''
        ph.execute(query=q_test_table, connection=connection_test)
        ph.to_clickhouse(df=concat_df, table='ETL_gorbenko_update_new', index = False, connection=connection_test)
                              
    df_count_views_likes = extract_count_views_likes()
    df_messages = extract_messages()
                              
    df_count_views_likes_messages = transform_merge(df_count_views_likes, df_messages)
                              
    os_df = transform_os(df_count_views_likes_messages)
    gender_df = transform_gender(df_count_views_likes_messages)
    age_df = transform_age(df_count_views_likes_messages)
                              
    concat_df = transform_concat(os_df, gender_df, age_df)
    
    load_concat(concat_df)
                              
k_gorbenko_update_new = k_gorbenko_update_new()