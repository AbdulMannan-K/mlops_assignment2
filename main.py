import requests
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

sources = ['https://www.dawn.com/', 'https://www.bbc.com/']


def extract():
    all_data = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    for source in sources:
        reqs = requests.get(source, headers=headers)
        soup = BeautifulSoup(reqs.text, 'html.parser')
        # Example for Dawn.com; you need to adjust selectors based on actual site structure
        articles = soup.find_all('article')
        for article in articles:
            if source == 'https://www.dawn.com/':
                title = article.find('h2')
                description = article.find(class_='story__excerpt')
                url = article.find('a', href=True)
                if title and description and url:
                    all_data.append({
                        'url': url['href'],
                        'title': title.get_text(strip=True),
                        'description': description.get_text(strip=True)
                    })

    return all_data


import re


def clean_text(text):
    """ Utility function to clean text by removing special characters and excessive whitespace. """
    text = re.sub(r'\s+', ' ', text)  # Replace multiple whitespaces with single space
    text = re.sub(r'[^\w\s]', '', text)  # Remove punctuation
    return text.strip().lower()


def transform(extracted_data):
    transformed_data = []
    for data in extracted_data:
        transformed_data.append({
            'url': data['url'],
            'title': clean_text(data['title']),
            'description': clean_text(data['description'])
        })
    return transformed_data

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import pandas as pd
import os


def load(transformed_data):
    # Convert data to DataFrame
    df = pd.DataFrame(transformed_data)
    filename = 'extracted_data.csv'
    df.to_csv(filename, index=False)

"""
for source in sources:
    extract(source)
    transform()
    load()
"""

default_args = {
    'owner': 'airflow-demo'
}

data = extract()

data = transform(data)

load(data)

# dag = DAG(
#     'mlops-dag',
#     default_args=default_args,
#     description='A simple '
# )
#
#
# task1 = PythonOperator(
#     task_id = "Task_1",
#     python_callable = extract,
#     dag = dag
# )
#
# task2 = PythonOperator(
#     task_id = "Task_2",
#     python_callable = transform,
#     dag=dag
# )
#
# task3 = PythonOperator(
#     task_id = "Task_3",
#     python_callable = load,
#     dag=dag
# )
#
# task1 >> task2 >> task3
