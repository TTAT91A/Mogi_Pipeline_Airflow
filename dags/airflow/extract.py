from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator # type: ignore
from airflow.operators.python_operator import PythonOperator # type: ignore

import urllib.parse
import pandas as pd
import numpy as np
# from selenium_provider import Selenium
# from selenium_provider import get_into
import requests
from bs4 import BeautifulSoup
from datetime import date
import os
from pathlib import Path 

##########################
default_args = {
    'owner': 'TuanNguyen',
    'start_date': datetime.now(),
    'email': ['anhtuan.ltqb@gmail.com'],
    'email_on_failure': ['anhtuan.ltqb@gmail.com'],
    'email_on_retry': ['anhtuan.ltqb@gmail.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG( 
    'Temp_Extract_without_overpass',
    description='Scrape house prices in HCM City from mogi.vn',
    default_args=default_args,
    schedule_interval="@daily",
)

install_library = BashOperator(
    task_id='install_library',
    bash_command='pip install PyGithub pymongo python-dotenv',
    dag=dag,
)

