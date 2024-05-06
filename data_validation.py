import pandas as pd
from google.cloud import bigquery
import logging
from datetime import datetime, timedelta

def validate_data(source_table, execution_date, anomaly_threshold):
    client = bigquery.Client()
    
    watermark_column = source_table['watermark_column']

    # Get the current execution date and previous date
    execution_date = datetime.strptime(execution_date, '%Y-%m-%d')
    previous_date = execution_date - timedelta(days=1) - timedelta(days=365 * 2)
    
    print("execution_date:", execution_date)
    print("previous_date:", previous_date)
    
    source_table_addr = f"{source_table['project']}.{source_table['dataset']}.{source_table['table']}"

    # Get the number of records for the previous day 
    query = f"""
        SELECT COUNT(*) as current_records
        FROM `{source_table_addr}`
        WHERE DATE({watermark_column}) = '{previous_date.date()}'
    """
    print(query)
    current_records_job = client.query(query)
    current_records = current_records_job.to_dataframe()['current_records'][0]

    # Get the average number of records for the last month 
    last_month_query = f"""
        SELECT avg(trips) as avg_records from (
            SELECT DATE({watermark_column}), count(*) as trips 
            FROM `{source_table_addr}`
            WHERE DATE({watermark_column}) between DATE_SUB('{previous_date.date()}', interval 1 month) and '{previous_date.date()}'
            GROUP BY DATE({watermark_column})
        )
    """
    print(last_month_query)
    avg_records_job = client.query(last_month_query)
    avg_records = avg_records_job.to_dataframe()['avg_records'][0]

    #CHECK TOTAL NO OF RECORDS

    # Check for anomalies
    max_threshold, min_threshold = anomaly_threshold['max'], anomaly_threshold['min']
    if current_records > max_threshold * avg_records or current_records < min_threshold * avg_records:
        logging.warning(f"Anomaly detected! Current records: {current_records}, Average records: {avg_records}")
        raise ValueError("Task failed due as an Anomaly was detected")
    else:
        logging.info("Data validation passed.")