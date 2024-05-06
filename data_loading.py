from google.cloud import bigquery
import pandas as pd
import decimal
import json

def load_data(destination_table, input_path = 'auto', bq_schema = None, pipeline_name = 'NA', partition = 'NA'):
    client = bigquery.Client()
    
    if input_path == 'auto':
        input_path = f'/tmp/{pipeline_name}_transformed.json'
    
    print('input_path:', input_path)
    
    with open(input_path, 'r') as f:
        data_df = pd.DataFrame(json.load(f))

    print(data_df.head(1).to_json(orient='records'))
    print(data_df.dtypes)
    
    schema = [bigquery.SchemaField(field['name'], field['type']) for field in bq_schema]
    
    print(schema)
    
    for row in schema:
        if row.field_type in ['TIMESTAMP', 'DATE']:
            if data_df[row.name].dtype == 'int64':
                unit = 's' if len(str(data_df[row.name][0])) == 10 else 'ms'
                data_df[row.name] = pd.to_datetime(data_df[row.name], unit=unit)
        if row.field_type == 'NUMERIC':
            data_df[row.name] = data_df[row.name].astype(str).map(decimal.Decimal)
    
    if 'write_disposition' in destination_table:
        write_disposition = destination_table['write_disposition']
    else:
        write_disposition = 'WRITE_APPEND'
    
    # Create a job configuration
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        schema=schema
    )
    
    if partition != 'NA':
        if partition['type'].upper() == 'HOUR':
            partition_type = bigquery.TimePartitioningType.HOUR
        elif partition['type'].upper() == 'MONTH':
            partition_type = bigquery.TimePartitioningType.MONTH
        elif partition['type'].upper() == 'YEAR':
            partition_type = bigquery.TimePartitioningType.YEAR
        else:
            partition_type = bigquery.TimePartitioningType.DAY
        
        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=partition_type,
            field=partition['column']
        )

    # Load data into the destination table
    client.load_table_from_dataframe(
        data_df,
        f"{destination_table['project']}.{destination_table['dataset']}.{destination_table['table']}",
        job_config=job_config
    ).result()
    
    print(f"Data loaded to {destination_table['project']}.{destination_table['dataset']}.{destination_table['table']}")
