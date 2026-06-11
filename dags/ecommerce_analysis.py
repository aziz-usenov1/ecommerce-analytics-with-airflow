import csv
from datetime import datetime
from pathlib import Path
from airflow.sdk import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    dag_id="ecommerce_analysis",
    start_date=datetime(2022, 6, 1),
    schedule="0 2 * * *",
    catchup=False,
)
def ecommerce_analysis():
    
    @task.python
    def check_tables_exist():
        hook = PostgresHook(postgres_conn_id="postgres")
        
        required_tables = {
            "customers",
            "products",
            "clicks",
            "transactions",
        }
        
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute(
                """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE 1=1
                    and table_schema = 'raw'
                    AND table_name = ANY(%s);
                """, (list(required_tables),))

        existing_tables = {row[0] for row in cursor.fetchall()}
        missing_tables = required_tables - existing_tables
        
        cursor.close()
        conn.close()
        
        if missing_tables:
            raise ValueError(f"Missing tables: {missing_tables}")
        
        print("All required raw tables exist.")
            
    
    @task.python
    def check_previous_day_data(processing_date:str):
        hook = PostgresHook(postgres_conn_id = "postgres")
        
        conn = hook.get_conn()
        cursor = conn.cursor()

        checks = {
            "raw.transactions":"created_at",
            "raw.clicks":"event_time"
        }
        
        for table, date_column in checks.items():
            cursor.execute(
                    f"""
                        SELECT COUNT(*)
                        FROM {table}
                        WHERE 1=1
                        and {date_column}::date = %s;          
                    """, (processing_date,))
        
            row_count = cursor.fetchone()[0]
            if row_count == 0:
                raise ValueError(f"{table} does not have data for date {processing_date}")
            print(f"{table} has {row_count} rows for date {processing_date}")
        
        cursor.close()
        conn.close()
    
    
    @task.branch
    def branch_transactions_exist(processing_date:str):
        hook = PostgresHook(postgres_conn_id="postgres")
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute(
            """
                SELECT COUNT(*)
                FROM raw.transactions
                WHERE 1=1
                and created_at::date = %s
            """, (processing_date,)
        )
        
        rows_count = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        if rows_count > 0:
            return ["daily_sales_mart",
                     "daily_product_performance",
                     "customer_metrics_mart"]

        return "skip_transaction_marts"
    
    
    @task.python
    def daily_sales_mart(processing_date:str):
        hook = PostgresHook(postgres_conn_id="postgres")
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            # 1. Delete existing data for this date
            cursor.execute(
                """
                    DELETE FROM bi.daily_sales
                    WHERE 1=1
                    and sales_date = %s
                """, (processing_date,)
            )
            
            # 2. Insert refreshed data
            cursor.execute(
                """
                    INSERT INTO bi.daily_sales(
                        sales_date
                        , total_orders
                        , total_customers
                        , total_revenue
                        , avg_order_value
                        , successful_payments
                        , failed_payments
                    )
                    SELECT
                        created_at::date as sales_date
                        , COUNT(DISTINCT booking_id) as total_orders
                        , COUNT(DISTINCT customer_id) as total_customers
                        , SUM(total_amount) as total_amount
                        , ROUND(AVG(total_amount), 2) as avg_order_value
                        , COUNT(*) FILTER(WHERE payment_status = 'Success') as successful_payments
                        , COUNT(*) FILTER(WHERE payment_status = 'Failed') as failed_payments
                    FROM bi.raw_transactions_v
                    WHERE 1=1
                    and created_at::date = %s
                    GROUP BY created_at::date 
                """, (processing_date, )
            )
            
            conn.commit()
            print(f"Daily Sales Mart refreshed for {processing_date}")
        
        except Exception as e:
            conn.rollback()
            raise e

        finally:
            cursor.close()
            conn.close()
    
    
    @task.python
    def daily_product_performance(processing_date:str):
        hook = PostgresHook(postgres_conn_id="postgres")
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            # 1. Delete existing data for this date
            cursor.execute(
                """
                    DELETE FROM bi.daily_product_performance
                    WHERE 1=1
                    and sales_date = %s
                """, (processing_date, )
            )
            
            # 2. Insert refreshed data
            cursor.execute(
                """
                    INSERT INTO bi.daily_product_performance(
                        sales_date
                        , product_id
                        , product_name
                        , category
                        , sub_category
                        , article_type
                        , total_quantity
                        , total_revenue
                        , total_orders     
                    )
                    SELECT
                        t.created_at::date as sales_date
                        , t.product_id
                        , p.productDisplayName as product_name
                        , p.masterCategory as category
                        , p.subCategory as sub_category
                        , p.articleType as article_type
                        , SUM(t.quantity) as total_quantity
                        , SUM(t.quantity * t.item_price) as total_revenue
                        , COUNT(DISTINCT t.booking_id) as total_orders
                    FROM
                        bi.raw_transactions_v t
                    LEFT JOIN raw.products p on t.product_id=p.id
                    WHERE 1=1
                    and t.created_at::date = %s
                    and t.payment_status = 'Success'
                    GROUP BY
                        t.created_at::date,
                        t.product_id,
                        p.productDisplayName,
                        p.masterCategory,
                        p.subCategory,
                        p.articleType
                """, (processing_date, )
            )

            conn.commit()
            print(f"Product performance mart refreshed for {processing_date}")
    
        except Exception as e:
            conn.rollback()
            raise e
        
        finally:
            cursor.close()
            conn.close()
            
    
    @task.python
    def customer_metrics_mart(processing_date:str):
        hook = PostgresHook(postgres_conn_id="postgres")
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute(
                # 1. Delete existing data for this date
                """
                    DELETE FROM bi.daily_customer_metrics
                    WHERE sales_date::date = %s
                """, (processing_date, )
            )
            
            # 2. Insert refreshed data
            cursor.execute(
                """
                    INSERT INTO bi.daily_customer_metrics(
                        sales_date
                        , customer_id
                        , gender
                        , device_type
                        , total_payments
                        , successful_payments
                        , failed_payments
                        , total_spent
                        , avg_payment_value
                    )
                    SELECT
                        t.created_at::date as sales_date
                        , t.customer_id
                        , c.gender
                        , c.device_type
                        , COUNT(DISTINCT t.booking_id) as total_payments
                        , COUNT(*) FILTER(WHERE t.payment_status = 'Success') as successful_payments
                        , COUNT(*) FILTER(WHERE t.payment_status = 'Failed') as failed_payments
                        , SUM(CASE
                                WHEN t.payment_status = 'Success' THEN t.total_amount
                                ELSE 0 END) as total_spent
                        , AVG(CASE
                                WHEN t.payment_status = 'Success' THEN t.total_amount
                                ELSE 0 END) as avg_payment_value
                    FROM bi.raw_transactions_v t
                    LEFT JOIN raw.customers c on t.customer_id=c.customer_id
                    WHERE 1=1
                    and t.created_at::date = %s
                    GROUP BY
                        t.created_at::date
                        , t.customer_id
                        , c.gender
                        , c.device_type
                """, (processing_date,)
            )
            
            conn.commit()
            print(f"Customer metrics mart refreshed for {processing_date}")

        except Exception as e:
            conn.rollback()
            raise e

        finally:
            cursor.close()
            conn.close()
    
    
    @task.python
    def daily_traffic_source_performance(processing_date:str):
        hook = PostgresHook(postgres_conn_id="postgres")
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute(
                """
                    DELETE FROM bi.daily_traffic_source_performance
                    WHERE event_date = %s
                """, (processing_date,)
            )
            
            cursor.execute(
                """
                    INSERT INTO bi.daily_traffic_source_performance(
                        event_date
                        , traffic_source
                        , total_sessions
                        , total_events
                        , unique_events
                    )
                    SELECT
                        event_time::date as event_date
                        , COALESCE(traffic_source, 'Unknown') as traffic_source
                        , COUNT(DISTINCT session_id) as total_sessions
                        , COUNT(*) as total_events
                        , COUNT(DISTINCT event_name) as unique_events
                    FROM raw.clicks
                    WHERE 1=1
                    and event_time::date = %s
                    GROUP BY 
                        event_time::date
                        , COALESCE(traffic_source, 'Unknown')
                """, (processing_date,)
            )
            
            conn.commit()
            print(f"Traffic source mart refreshed for {processing_date}")
            
        except Exception as e:
            conn.rollback()
            raise e
        
        finally:
            cursor.close()
            conn.close()
            
    
    @task
    def pipeline_success():
        print("Pipeline has been successfully completed")        
    
    
    skip_transaction_marts = EmptyOperator(
        task_id = "skip_transaction_marts"
    )
    
    join_after_transaction_branch = EmptyOperator(
        task_id = "join_after_transaction_branch",
        trigger_rule = "none_failed_min_one_success"
    )
    
    target_date = "{{ (data_interval_start - macros.timedelta(days=1)) | ds }}"
    
    tables_ok = check_tables_exist()
    data_ok = check_previous_day_data(target_date)
    traffic = daily_traffic_source_performance(target_date)
    branch = branch_transactions_exist(target_date)
    daily_sales = daily_sales_mart(target_date)
    product_perf = daily_product_performance(target_date)
    customer_metrics = customer_metrics_mart(target_date)
    success = pipeline_success()
    
    tables_ok >> data_ok
    data_ok >>  traffic
    data_ok >>  branch
    
    branch >> [
                daily_sales, 
                product_perf, 
                customer_metrics, 
                skip_transaction_marts
            ]
    
    [
        daily_sales, 
        product_perf, 
        customer_metrics, 
        skip_transaction_marts
    ] >> join_after_transaction_branch
    
    [
        traffic, 
        join_after_transaction_branch
    ] >> success
    
ecommerce_analysis()
