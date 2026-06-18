from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd
import logging
import os

default_args = {
    'owner': 'student',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    'final_pipeline_poc',
    default_args=default_args,
    description='POC пайплайн с ветвлением, retry и email',
    schedule_interval=None,
    catchup=False,
) as dag:

    def read_from_csv(**context):
        csv_path = os.path.expanduser('~/airflow/data/orders.csv')
        if not os.path.exists(csv_path):
            data = {
                'order_id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                'user_id': [101, 102, 103, 104, 105, 106, 107, 108, 109, 110],
                'total': [100, 200, 150, 300, 250, 180, 90, 400, 350, 120],
                'status': ['delivered', 'pending', 'delivered', 'shipped', 'delivered',
                          'pending', 'delivered', 'cancelled', 'delivered', 'shipped']
            }
            df = pd.DataFrame(data)
            os.makedirs(os.path.dirname(csv_path), exist_ok=True)
            df.to_csv(csv_path, index=False)
            logging.info(f"Создан тестовый файл с {len(df)} заказами")
        else:
            df = pd.read_csv(csv_path)
            logging.info(f"Прочитано {len(df)} заказов из файла")
        context['ti'].xcom_push(key='orders_count', value=len(df))
        return len(df)

    def analyze_and_branch(**context):
        orders_count = context['ti'].xcom_pull(task_ids='read_from_csv', key='orders_count')
        logging.info(f"Количество заказов: {orders_count}")
        if orders_count > 5:
            logging.info("Больше 5 заказов -> обрабатываем полностью")
            return 'process_data'
        else:
            logging.info("Меньше 5 заказов -> пропускаем обработку")
            return 'skip_processing'

    def process_data(**context):
        csv_path = os.path.expanduser('~/airflow/data/orders.csv')
        df = pd.read_csv(csv_path)
        total_orders = len(df)
        total_sum = df['total'].sum()
        avg_sum = df['total'].mean()
        report = f"""
        ОТЧЁТ ПО ЗАКАЗАМ
        Количество заказов: {total_orders}
        Общая сумма: {total_sum}
        Средняя сумма: {avg_sum:.2f}
        """
        report_path = os.path.expanduser('~/airflow/data/report.txt')
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        with open(report_path, 'w') as f:
            f.write(report)
        logging.info(f"Отчёт сохранён: {report_path}")
        logging.info(report)
        return "Обработка завершена"

    def check_quality(**context):
        report_path = os.path.expanduser('~/airflow/data/report.txt')
        if os.path.exists(report_path):
            logging.info("Проверка качества пройдена")
            return True
        else:
            logging.warning("Отчёт не найден")
            return False

    def send_success_notification(**context):
        logging.info("===== УВЕДОМЛЕНИЕ =====")
        logging.info("Пайплайн успешно выполнен!")
        logging.info(f"Дата запуска: {context['execution_date']}")
        logging.info("Отчёт сохранён в ~/airflow/data/report.txt")
        logging.info("===== КОНЕЦ УВЕДОМЛЕНИЯ =====")
        return True

    def send_failure_notification(**context):
        logging.info("===== УВЕДОМЛЕНИЕ =====")
        logging.info("Пайплайн завершился с ошибкой!")
        logging.info(f"Дата запуска: {context['execution_date']}")
        logging.info("Проверьте логи для выявления причины")
        logging.info("===== КОНЕЦ УВЕДОМЛЕНИЯ =====")
        return True

    read_task = PythonOperator(
        task_id='read_from_csv',
        python_callable=read_from_csv,
    )

    branch_task = BranchPythonOperator(
        task_id='analyze_and_branch',
        python_callable=analyze_and_branch,
    )

    process_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
        retries=3,
        retry_delay=timedelta(seconds=30),
    )

    skip_task = DummyOperator(
        task_id='skip_processing',
    )

    quality_task = PythonOperator(
        task_id='check_quality',
        python_callable=check_quality,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    email_success = PythonOperator(
        task_id='send_success_email',
        python_callable=send_success_notification,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    email_failure = PythonOperator(
        task_id='send_failure_email',
        python_callable=send_failure_notification,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    read_task >> branch_task
    branch_task >> [process_task, skip_task]
    process_task >> quality_task
    skip_task >> quality_task
    quality_task >> [email_success, email_failure]
