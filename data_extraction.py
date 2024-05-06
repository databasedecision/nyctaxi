import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery

def extract_data(source_table, watermark_column, execution_date, output_path = 'auto', pipeline_name = 'NA'):
    client = bigquery.Client()
    
    if output_path == 'auto':
        output_path = f'/tmp/{pipeline_name}_extracted.json'
        
    print('output_path', output_path)

    # Parse execution date
    execution_date = datetime.strptime(execution_date, '%Y-%m-%d')
    previous_date = execution_date - timedelta(days=1) - timedelta(days=365 * 2)
    
    source_table_addr = f"{source_table['project']}.{source_table['dataset']}.{source_table['table']}"

    # Build the query
    query = f"""
        SELECT *
        FROM `{source_table_addr}`
        WHERE DATE({watermark_column}) = '{previous_date.date()}'
    """
    print(query)
    
    # Execute the query and return the data as a DataFrame
    query_job = client.query(query)
    data_df = query_job.to_dataframe()

    # Save the data to a temporary JSON file
    data_df.to_json(output_path, orient='records')
    
    print(f"Data successfuly extracted from {source_table_addr}")