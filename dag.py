from airflow.sdk import dag, task
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


BUCKET="l3-external-storage-753900908173"
S3_KEY = '{{ dag.dag_id }}/extract/{{ ds }}/author_page_views.csv',


@dag
def author_metrics():

    @task
    def extract(extract_key):
        
        import pathlib
        import pandas as pd
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        
        csv_path = pathlib.Path(__file__).parent / 'author_page_views.csv'
        df = pd.read_csv(csv_path)

        hook = S3Hook()

        hook.load_string(
            string_data=df.to_csv(index=False),
            bucket=BUCKET,
            key=extract_key
        )

    load = S3ToRedshiftOperator(
        task_id="quotes",
        table="author_page_views",
        schema="scraped_quotes",
        s3_bucket=BUCKET,
        s3_key=S3_KEY,
        method='UPSERT',
        upsert_keys=['page_view_id'],
        copy_options=[
            "CSV",
            "IGNOREHEADER 1",
            ],
        )
    
    transform = SQLExecuteQueryOperator(
        task_id="transform",
        conn_id="redshift_default",
        sql="""
            SELECT author.author_name,
                COUNT(page_views.page_view_id),
                COUNT(quotes.quote)
            FROM raw.scraped_quotes.authors
            LEFT JOIN raw.scraped_quotes.page_views
                USING(author_name)
            LEFT JOIN raw.scraped_quotes.quotes
                ON author.author = authors.author_name
        """
    )

    
    extract() >> load >> transform


author_metrics()
    
    
    




