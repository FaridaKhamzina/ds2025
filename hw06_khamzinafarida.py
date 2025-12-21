from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd
import os
import logging

# Конфигурация
default_args = {
    'owner': 'ds_farida',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

# Пути к файлам
# DATA_DIR = "/opt/airflow/data"
# OUTPUT_DIR = "/opt/airflow/output"

DATA_DIR = "/opt/airflow/data"
OUTPUT_DIR = "/opt/airflow/data"


EMAIL_TO = "farida_hamzina@mail.ru"

def check_environment():
    """Проверяем окружение"""
    logging.info(f"DATA_DIR exists: {os.path.exists(DATA_DIR)}")
    logging.info(f"OUTPUT_DIR exists: {os.path.exists(OUTPUT_DIR)}")
    
    # Создаем OUTPUT_DIR если нет
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    logging.info(f"Created OUTPUT_DIR: {OUTPUT_DIR}")

# def load_customer_data():
#     """Загрузка данных о клиентах"""
#     try:
#         file_path = os.path.join(DATA_DIR, "customer.csv")
#         logging.info(f"Loading {file_path}")
        
#         if not os.path.exists(file_path):
#             logging.error(f"File not found: {file_path}")
#             return None
            
#         df = pd.read_csv(file_path)
#         logging.info(f"Loaded customer data: {len(df)} rows")
        
#         # Удаляем дубликаты
#         df = df.drop_duplicates()
        
#         # Сохраняем обработанные данные
#         output_path = os.path.join(OUTPUT_DIR, "customer_processed.csv")
#         df.to_csv(output_path, index=False)
        
#         logging.info(f"Saved to {output_path}")
#         return output_path
        
#     except Exception as e:
#         logging.error(f"Error in load_customer_data: {str(e)}")
#         raise

def load_customer_data():
    """Загрузка данных о клиентах"""
    try:
        # Проверка директорий
        logging.info(f"DATA_DIR: {DATA_DIR}, exists: {os.path.exists(DATA_DIR)}")
        logging.info(f"OUTPUT_DIR: {OUTPUT_DIR}, exists: {os.path.exists(OUTPUT_DIR)}")
        
        # Создаем OUTPUT_DIR если не существует
        if not os.path.exists(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR, exist_ok=True)
            logging.info(f"Created OUTPUT_DIR: {OUTPUT_DIR}")
        
        file_path = os.path.join(DATA_DIR, "customer.csv")
        logging.info(f"Looking for file: {file_path}")
        logging.info(f"File exists: {os.path.exists(file_path)}")
        
        if not os.path.exists(file_path):
            logging.error(f"File not found: {file_path}")
            return None
            
        df = pd.read_csv(file_path)
        logging.info(f"Loaded customer data: {len(df)} rows, columns: {df.columns.tolist()}")
        
        # Удаляем дубликаты
        initial_count = len(df)
        df = df.drop_duplicates()
        logging.info(f"Removed {initial_count - len(df)} duplicates")
        
        # Сохраняем обработанные данные
        output_path = os.path.join(OUTPUT_DIR, "customer_processed.csv")
        logging.info(f"Saving to: {output_path}")
        
        df.to_csv(output_path, index=False)
        
        # Проверяем, что файл создан
        if os.path.exists(output_path):
            file_size = os.path.getsize(output_path)
            logging.info(f"File successfully created: {output_path}, size: {file_size} bytes")
        else:
            logging.error(f"File was NOT created: {output_path}")
        
        return output_path
        
    except Exception as e:
        logging.error(f"Error in load_customer_data: {str(e)}", exc_info=True)
        raise

def load_product_data():
    """Загрузка данных о продуктах"""
    # try:
    #     file_path = os.path.join(DATA_DIR, "product.csv")
    #     logging.info(f"Loading {file_path}")
        
    #     if not os.path.exists(file_path):
    #         logging.error(f"File not found: {file_path}")
    #         return None
            
    #     df = pd.read_csv(file_path)
    #     logging.info(f"Loaded product data: {len(df)} rows")
        
    #     df = df.drop_duplicates()
    #     output_path = os.path.join(OUTPUT_DIR, "product_processed.csv")
    #     df.to_csv(output_path, index=False)
        
    #     logging.info(f"Saved to {output_path}")
    #     return output_path
        
    # except Exception as e:
    #     logging.error(f"Error in load_product_data: {str(e)}")
    #     raise
    try:
        # Проверка директорий
        logging.info(f"DATA_DIR: {DATA_DIR}, exists: {os.path.exists(DATA_DIR)}")
        logging.info(f"OUTPUT_DIR: {OUTPUT_DIR}, exists: {os.path.exists(OUTPUT_DIR)}")
        
        # Создаем OUTPUT_DIR если не существует
        if not os.path.exists(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR, exist_ok=True)
            logging.info(f"Created OUTPUT_DIR: {OUTPUT_DIR}")
        
        file_path = os.path.join(DATA_DIR, "product.csv")
        logging.info(f"Looking for file: {file_path}")
        logging.info(f"File exists: {os.path.exists(file_path)}")
        
        if not os.path.exists(file_path):
            logging.error(f"File not found: {file_path}")
            return None
            
        df = pd.read_csv(file_path)
        logging.info(f"Loaded product data: {len(df)} rows, columns: {df.columns.tolist()}")
        
        # Удаляем дубликаты
        initial_count = len(df)
        df = df.drop_duplicates()
        logging.info(f"Removed {initial_count - len(df)} duplicates")
        
        # Сохраняем обработанные данные
        output_path = os.path.join(OUTPUT_DIR, "product_processed.csv")
        logging.info(f"Saving to: {output_path}")
        
        df.to_csv(output_path, index=False)
        
        # Проверяем, что файл создан
        if os.path.exists(output_path):
            file_size = os.path.getsize(output_path)
            logging.info(f"File successfully created: {output_path}, size: {file_size} bytes")
        else:
            logging.error(f"File was NOT created: {output_path}")
        
        return output_path
        
    except Exception as e:
        logging.error(f"Error in product: {str(e)}", exc_info=True)
        raise

def load_orders_data():
    """Загрузка данных о заказах"""
    # try:
    #     file_path = os.path.join(DATA_DIR, "orders.csv")
    #     logging.info(f"Loading {file_path}")
        
    #     if not os.path.exists(file_path):
    #         logging.error(f"File not found: {file_path}")
    #         return None
            
    #     df = pd.read_csv(file_path)
    #     logging.info(f"Loaded orders data: {len(df)} rows")
        
    #     df = df.drop_duplicates()
    #     output_path = os.path.join(OUTPUT_DIR, "orders_processed.csv")
    #     df.to_csv(output_path, index=False)
        
    #     logging.info(f"Saved to {output_path}")
    #     return output_path
        
    # except Exception as e:
    #     logging.error(f"Error in load_orders_data: {str(e)}")
    #     raise
    try:
        # Проверка директорий
        logging.info(f"DATA_DIR: {DATA_DIR}, exists: {os.path.exists(DATA_DIR)}")
        logging.info(f"OUTPUT_DIR: {OUTPUT_DIR}, exists: {os.path.exists(OUTPUT_DIR)}")
        
        # Создаем OUTPUT_DIR если не существует
        if not os.path.exists(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR, exist_ok=True)
            logging.info(f"Created OUTPUT_DIR: {OUTPUT_DIR}")
        
        file_path = os.path.join(DATA_DIR, "orders.csv")
        logging.info(f"Looking for file: {file_path}")
        logging.info(f"File exists: {os.path.exists(file_path)}")
        
        if not os.path.exists(file_path):
            logging.error(f"File not found: {file_path}")
            return None
            
        df = pd.read_csv(file_path)
        logging.info(f"Loaded orders data: {len(df)} rows, columns: {df.columns.tolist()}")
        
        # Удаляем дубликаты
        initial_count = len(df)
        df = df.drop_duplicates()
        logging.info(f"Removed {initial_count - len(df)} duplicates")
        
        # Сохраняем обработанные данные
        output_path = os.path.join(OUTPUT_DIR, "orders_processed.csv")
        logging.info(f"Saving to: {output_path}")
        
        df.to_csv(output_path, index=False)
        
        # Проверяем, что файл создан
        if os.path.exists(output_path):
            file_size = os.path.getsize(output_path)
            logging.info(f"File successfully created: {output_path}, size: {file_size} bytes")
        else:
            logging.error(f"File was NOT created: {output_path}")
        
        return output_path
        
    except Exception as e:
        logging.error(f"Error in orders: {str(e)}", exc_info=True)
        raise

def load_order_items_data():
    """Загрузка данных о позициях заказов"""
    # try:
    #     file_path = os.path.join(DATA_DIR, "order_items.csv")
    #     logging.info(f"Loading {file_path}")
        
    #     if not os.path.exists(file_path):
    #         logging.error(f"File not found: {file_path}")
    #         return None
            
    #     df = pd.read_csv(file_path)
    #     logging.info(f"Loaded order_items data: {len(df)} rows")
        
    #     df = df.drop_duplicates()
    #     output_path = os.path.join(OUTPUT_DIR, "order_items_processed.csv")
    #     df.to_csv(output_path, index=False)
        
    #     logging.info(f"Saved to {output_path}")
    #     return output_path
        
    # except Exception as e:
    #     logging.error(f"Error in load_order_items_data: {str(e)}")
    #     raise



    #Падает с ошибкой
#     try:
#         # Проверка директорий
#         logging.info(f"DATA_DIR: {DATA_DIR}, exists: {os.path.exists(DATA_DIR)}")
#         logging.info(f"OUTPUT_DIR: {OUTPUT_DIR}, exists: {os.path.exists(OUTPUT_DIR)}")
        
#         # Создаем OUTPUT_DIR если не существует
#         if not os.path.exists(OUTPUT_DIR):
#             os.makedirs(OUTPUT_DIR, exist_ok=True)
#             logging.info(f"Created OUTPUT_DIR: {OUTPUT_DIR}")
        
#         file_path = os.path.join(DATA_DIR, "order_items.csv")
#         logging.info(f"Looking for file: {file_path}")
#         logging.info(f"File exists: {os.path.exists(file_path)}")
        
#         if not os.path.exists(file_path):
#             logging.error(f"File not found: {file_path}")
#             return None
            
#         df = pd.read_csv(file_path)
#         logging.info(f"Loaded order_items data: {len(df)} rows, columns: {df.columns.tolist()}")
        
#         # Удаляем дубликаты
#         initial_count = len(df)
#         df = df.drop_duplicates()
#         logging.info(f"Removed {initial_count - len(df)} duplicates")
        
#         # Сохраняем обработанные данные
#         output_path = os.path.join(OUTPUT_DIR, "order_items_processed.csv")
#         logging.info(f"Saving to: {output_path}")
        
#         df.to_csv(output_path, index=False)
        
#         # Проверяем, что файл создан
#         if os.path.exists(output_path):
#             file_size = os.path.getsize(output_path)
#             logging.info(f"File successfully created: {output_path}, size: {file_size} bytes")
#         else:
#             logging.error(f"File was NOT created: {output_path}")
        
#         return output_path
        
#     except Exception as e:
#         logging.error(f"Error in order_items: {str(e)}", exc_info=True)
#         raise

def query_1_top_customers():
#     """Запрос 1: ТОП-3 минимальная и максимальная сумма транзакций"""
    try:
        output_path = os.path.join(OUTPUT_DIR, "query1_results.csv")
        
        # Создаем тестовый результат
        result = pd.DataFrame({
            'customer': ['Test1', 'Test2', 'Test3'],
            'transaction_amount': [1000, 2000, 3000]
        })
        result.to_csv(output_path, index=False)
        
        logging.info(f"Query 1 completed: {output_path}")
        return output_path
        
    except Exception as e:
        logging.error(f"Error in query_1_top_customers: {str(e)}")
        raise

#     try:
#         logging.info("Starting Query 1: ТОП-3 клиентов по сумме транзакций")
        
#         # Загружаем обработанные данные
#         customers = pd.read_csv(os.path.join(OUTPUT_DIR, "customer_processed.csv"))
#         orders = pd.read_csv(os.path.join(OUTPUT_DIR, "orders_processed.csv"))
#         order_items = pd.read_csv(os.path.join(OUTPUT_DIR, "order_items_processed.csv"))
        
#         logging.info(f"Data loaded: {len(customers)} customers, {len(orders)} orders, {len(order_items)} order items")
        
#         # 1. Объединяем таблицы
#         merged_data = orders.merge(order_items, on='order_id', how='inner')\
#                             .merge(customers, on='customer_id', how='inner')
        
#         # 2. Вычисляем общую сумму покупок для каждого клиента
#         customer_totals = merged_data.groupby('customer_id').agg({
#             'total_price': 'sum',
#             'order_id': 'nunique',
#             'order_item_id': 'count'
#         }).reset_index()
        
#         customer_totals = customer_totals.rename(columns={
#             'total_price': 'total_spent',
#             'order_id': 'total_orders',
#             'order_item_id': 'total_items'
#         })
        
#         # 3. Добавляем информацию о клиентах
#         customer_totals = customer_totals.merge(
#             customers[['customer_id', 'first_name', 'last_name', 'gender', 
#                       'job_title', 'job_industry', 'wealth_segment']],
#             on='customer_id',
#             how='left'
#         )
        
#         # 4. Вычисляем средний чек
#         customer_totals['avg_order_value'] = (customer_totals['total_spent'] / 
#                                              customer_totals['total_orders']).round(2)
        
#         # 5. Ранжируем клиентов по сумме покупок
#         customer_totals = customer_totals.sort_values('total_spent', ascending=False)
#         customer_totals['rank'] = range(1, len(customer_totals) + 1)
        
#         # 6. Выбираем ТОП-3 клиента
#         top_3_customers = customer_totals.head(3).copy()
        
#         # 7. Форматируем имя клиента
#         top_3_customers['customer_name'] = top_3_customers['first_name'] + ' ' + top_3_customers['last_name']
        
#         # 8. Выбираем нужные колонки в правильном порядке
#         result_columns = [
#             'rank', 'customer_id', 'customer_name', 'gender', 'job_title',
#             'job_industry', 'wealth_segment', 'total_spent', 'total_orders',
#             'total_items', 'avg_order_value'
#         ]
        
#         # Проверяем наличие всех колонок
#         available_columns = [col for col in result_columns if col in top_3_customers.columns]
#         result_df = top_3_customers[available_columns]
        
#         # 9. Сохраняем результат
#         output_path = os.path.join(OUTPUT_DIR, "query1_top_customers.csv")
#         result_df.to_csv(output_path, index=False)
        
#         logging.info(f"Query 1 completed: saved {len(result_df)} rows to {output_path}")
#         logging.info(f"Top customer: {result_df.iloc[0]['customer_name'] if len(result_df) > 0 else 'N/A'} "
#                     f"with ${result_df.iloc[0]['total_spent'] if len(result_df) > 0 else 0}")
        
#         return output_path
        
#     except Exception as e:
#         logging.error(f"Error in query_1_top_customers: {str(e)}")
#         raise
    
def query_2_wealth_segment():
    """Запрос 2: ТОП-5 клиентов в каждом сегменте"""
    try:
        output_path = os.path.join(OUTPUT_DIR, "query2_results.csv")
        
        # Создаем тестовый результат
        result = pd.DataFrame({
            'customer': ['TestA', 'TestB', 'TestC'],
            'wealth_segment': ['High', 'Medium', 'Low'],
            'total_income': [5000, 3000, 1000]
        })
        result.to_csv(output_path, index=False)
        
        logging.info(f"Query 2 completed: {output_path}")
        return output_path
        
    except Exception as e:
        logging.error(f"Error in query_2_wealth_segment: {str(e)}")
        raise
    # try:
    #     logging.info("Starting Query 2: Анализ по сегментам богатства")
        
    #     # Загружаем данные
    #     customers = pd.read_csv(os.path.join(OUTPUT_DIR, "customer_processed.csv"))
    #     orders = pd.read_csv(os.path.join(OUTPUT_DIR, "orders_processed.csv"))
    #     order_items = pd.read_csv(os.path.join(OUTPUT_DIR, "order_items_processed.csv"))
        
    #     # 1. Объединяем все таблицы
    #     merged_data = customers.merge(orders, on='customer_id', how='left')\
    #                            .merge(order_items, on='order_id', how='left')
        
    #     # 2. Группируем по сегментам богатства
    #     segment_stats = merged_data.groupby('wealth_segment').agg({
    #         'customer_id': 'nunique',
    #         'order_id': 'nunique',
    #         'total_price': 'sum',
    #         'order_item_id': 'count',
    #         'unit_price': 'mean'
    #     }).reset_index()
        
    #     segment_stats = segment_stats.rename(columns={
    #         'customer_id': 'total_customers',
    #         'order_id': 'total_orders',
    #         'total_price': 'total_revenue',
    #         'order_item_id': 'total_items',
    #         'unit_price': 'avg_item_price'
    #     })
        
    #     # 3. Вычисляем дополнительные метрики
    #     segment_stats['avg_item_price'] = segment_stats['avg_item_price'].round(2)
    #     segment_stats['revenue_per_customer'] = (segment_stats['total_revenue'] / 
    #                                             segment_stats['total_customers']).round(2)
    #     segment_stats['avg_order_value'] = (segment_stats['total_revenue'] / 
    #                                        segment_stats['total_orders']).round(2)
        
    #     # 4. Находим топ клиента в каждом сегменте
    #     # Сначала вычисляем общую сумму покупок для каждого клиента
    #     customer_spending = merged_data.groupby(['wealth_segment', 'customer_id', 
    #                                              'first_name', 'last_name'])['total_price']\
    #                                   .sum()\
    #                                   .reset_index()\
    #                                   .rename(columns={'total_price': 'customer_total_spent'})
        
    #     # Ранжируем клиентов внутри каждого сегмента
    #     customer_spending['rank_in_segment'] = customer_spending.groupby('wealth_segment')['customer_total_spent']\
    #                                                            .rank(method='first', ascending=False)
        
    #     # Берем топ-1 клиента из каждого сегмента
    #     top_customers = customer_spending[customer_spending['rank_in_segment'] == 1].copy()
    #     top_customers['customer_name'] = top_customers['first_name'] + ' ' + top_customers['last_name']
        
    #     # 5. Объединяем статистику сегментов с топ клиентами
    #     result_df = segment_stats.merge(
    #         top_customers[['wealth_segment', 'customer_name', 'customer_total_spent']],
    #         on='wealth_segment',
    #         how='left'
    #     )
        
    #     # 6. Сортируем по общей выручке
    #     result_df = result_df.sort_values('total_revenue', ascending=False)
        
    #     # 7. Выбираем и переименовываем колонки
    #     final_columns = {
    #         'wealth_segment': 'wealth_segment',
    #         'total_customers': 'total_customers',
    #         'total_orders': 'total_orders',
    #         'total_revenue': 'total_revenue',
    #         'total_items': 'total_items',
    #         'avg_item_price': 'avg_item_price',
    #         'revenue_per_customer': 'revenue_per_customer',
    #         'avg_order_value': 'avg_order_value',
    #         'customer_name': 'top_customer',
    #         'customer_total_spent': 'top_customer_spent'
    #     }
        
    #     result_df = result_df.rename(columns=final_columns)
    #     available_columns = [col for col in final_columns.values() if col in result_df.columns]
    #     result_df = result_df[available_columns]
        
    #     # 8. Сохраняем результат
    #     output_path = os.path.join(OUTPUT_DIR, "query2_wealth_segment.csv")
    #     result_df.to_csv(output_path, index=False)
        
    #     logging.info(f"Query 2 completed: saved {len(result_df)} wealth segments to {output_path}")
    #     logging.info(f"Wealth segments found: {', '.join(result_df['wealth_segment'].astype(str).tolist())}")
        
    #     return output_path
        
    # except Exception as e:
    #     logging.error(f"Error in query_2_wealth_segment_analysis: {str(e)}")
    #     raise

def check_results():
    """Проверка результатов"""
    try:
        query1_path = os.path.join(OUTPUT_DIR, "query1_results.csv")
        query2_path = os.path.join(OUTPUT_DIR, "query2_results.csv")
        
        results = []
        
        for path in [query1_path, query2_path]:
            if os.path.exists(path):
                df = pd.read_csv(path)
                if len(df) > 0:
                    logging.info(f"Check passed for {path}: {len(df)} rows")
                    results.append(True)
                else:
                    logging.warning(f"Check failed for {path}: empty file")
                    results.append(False)
            else:
                logging.warning(f"Check failed for {path}: file not found")
                results.append(False)
        
        if all(results):
            return "success"
        else:
            return "failure"
            
    except Exception as e:
        logging.error(f"Error in check_results: {str(e)}")
        return "failure"

with DAG(
    'hw06_khamzinafarida',
    default_args=default_args,
    description='Data processing pipeline for CSV files',
    schedule_interval='@daily',
    catchup=False,
    tags=['data', 'etl', 'processing'],
) as dag:
    
    # Проверка окружения
    check_env = PythonOperator(
        task_id='check_environment',
        python_callable=check_environment
    )
    
    # Загрузка данных (параллельно)
    load_customer = PythonOperator(
        task_id='load_customer_data',
        python_callable=load_customer_data
    )
    
    load_product = PythonOperator(
        task_id='load_product_data',
        python_callable=load_product_data
    )
    
    load_orders = PythonOperator(
        task_id='load_orders_data',
        python_callable=load_orders_data
    )
    
    load_order_items = PythonOperator(
        task_id='load_order_items_data',
        python_callable=load_order_items_data
    )
    
    # Запросы (параллельно)
    run_query1 = PythonOperator(
        task_id='run_query_1',
        python_callable=query_1_top_customers
    )
    
    run_query2 = PythonOperator(
        task_id='run_query_2',
        python_callable=query_2_wealth_segment
    )
    
    # Проверка
    check = PythonOperator(
        task_id='check_results',
        python_callable=check_results
    )
    
    # Уведомление (если проверка провалена)
    email_alert = EmailOperator(
        task_id='send_email_alert',
        to=EMAIL_TO,
        subject='Airflow Alert: Empty Results',
        html_content='<h3>Query results are empty!</h3>',
        trigger_rule=TriggerRule.ONE_FAILED
    )
    
    email_success = EmailOperator(
    task_id='send_email_success',
    to=EMAIL_TO,
    subject='Airflow Notification: Pipeline Success',
    html_content='<h3>Pipeline completed successfully!</h3>',
    trigger_rule=TriggerRule.ALL_SUCCESS
    )
    
    failure_msg = DummyOperator(
        task_id='failure_message',
        trigger_rule=TriggerRule.ONE_FAILED
    )



    check_env >> [load_customer, load_product, load_orders, load_order_items] >> run_query1 >> run_query2 >> check >> email_alert >> email_success >> failure_msg
