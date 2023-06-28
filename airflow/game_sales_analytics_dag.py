import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

path_to_file = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
login = 'a-stepanyan'
year = 1994 + hash(f'{login}') % 23

default_args = {
    'owner': 'a-stepanyan',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 5, 4)
}

CHAT_ID = -1001757733778
try:
    BOT_TOKEN = Variable.get('telegram_secret')
except:
    BOT_TOKEN = ''

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Well done! Dag {dag_id} completed on {date}'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        pass

@dag(default_args = default_args, schedule_interval = '0 12 * * *', catchup = False)
def a_stepanyan_game_sales_dag():                  
    @task()
    def get_data():
        df = pd.read_csv(path_to_file).dropna(axis = 'index', how = 'any', subset = ['Year'])
        df.Year = df.Year.astype(int)
        prepared_data = df.query('Year == @year')
        return prepared_data

    @task()
    def get_bestseller_game(prepared_data):
        bestseller = prepared_data.groupby(['Name'], as_index = False)\
        .agg({'Global_Sales': 'sum'})\
        .sort_values(by = 'Global_Sales', ascending = False)\
        .head(1)
        bestseller_game = bestseller['Name'].to_list()
        return bestseller_game

    @task()
    def get_top_EU_genre(prepared_data):
        top_EU_genre = prepared_data.groupby(['Genre'], as_index=False)\
        .agg({'EU_Sales': 'sum'})\
        .query('EU_Sales == EU_Sales.max()')\
        .Genre.values.tolist()
        return top_EU_genre

    @task()
    def get_top_NA_platform(prepared_data):
        top_NA_platform = prepared_data.query('NA_Sales > 1')\
        .groupby(['Platform'], as_index = False)\
        .agg({'Name': 'nunique'})\
        .query('Name == Name.max()')\
        .Platform.values.tolist()
        return top_NA_platform

    @task()
    def get_publisher_top_avg_sales_japan(prepared_data):
        publisher_top_avg_sales_japan = prepared_data.groupby(['Publisher'], as_index = False)\
        .agg({'JP_Sales': np.mean})\
        .query('JP_Sales == JP_Sales.max()')\
        .Publisher.values.tolist()
        return publisher_top_avg_sales_japan
    
    @task()
    def get_EU_vs_JP(prepared_data):
        EU_and_JP = prepared_data.groupby(['Name'], as_index = False).agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'})
        EU_vs_JP = EU_and_JP[EU_and_JP.EU_Sales > EU_and_JP.JP_Sales].Name.count()
        return EU_vs_JP

    @task(on_success_callback=send_message)
    def print_data(bestseller_game, top_EU_genre, top_NA_platform, publisher_top_avg_sales_japan, EU_vs_JP):

        context = get_current_context()
        date = context['ds']
        
        print(f'Based on {year} data:')
        print(f'The best-selling game in the world: {bestseller_game}')
        print(f'The most popular game genres in Europe:: {top_EU_genre}')
        print(f'Top platforms with millions copies sold in North America: {top_NA_platform}')
        print(f'The highest average sales in Japan were by a publisher: {publisher_top_avg_sales_japan}')
        print(f'And {EU_vs_JP} games sold better in Europe than in Japan')
        

    prepared_data = get_data()
    bestseller_game = get_bestseller_game(prepared_data)
    top_EU_genre = get_top_EU_genre(prepared_data)
    top_NA_platform = get_top_NA_platform(prepared_data)
    publisher_top_avg_sales_japan = get_publisher_top_avg_sales_japan(prepared_data)
    EU_vs_JP = get_EU_vs_JP(prepared_data)
    print_data(bestseller_game, top_EU_genre, top_NA_platform, publisher_top_avg_sales_japan, EU_vs_JP)

a_stepanyan_game_sales_dag = a_stepanyan_game_sales_dag()

