import requests
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

sources = ['https://www.dawn.com/', 'https://www.bbc.com/']

def extract():
    all_data = []
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    for source in sources:
        reqs = requests.get(source, headers=headers)
        soup = BeautifulSoup(reqs.text, 'html.parser')
        # Example for Dawn.com; you need to adjust selectors based on actual site structure
        articles = soup.find_all('article')
        for article in articles:
            title = article.find('h2')
            description = article.find('p')
            url = article.find('a', href=True)
            if title and description and url:
                all_data.append({
                    'url': url['href'],
                    'title': title.get_text(strip=True),
                    'description': description.get_text(strip=True)
                })
    return all_data


def transform():
    print("Transformation")

def load():
    print("Loading")

"""
for source in sources:
    extract(source)
    transform()
    load()
"""

default_args = {
    'owner' : 'airflow-demo'
}

dag = DAG(
    'mlops-dag',
    default_args=default_args,
    description='A simple '
)


task1 = PythonOperator(
    task_id = "Task_1",
    python_callable = extract,
    dag = dag
)

task2 = PythonOperator(
    task_id = "Task_2",
    python_callable = transform,
    dag=dag
)

task3 = PythonOperator(
    task_id = "Task_3",
    python_callable = load,
    dag=dag
)

task1 >> task2 >> task3