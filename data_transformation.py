import json
import pandas as pd
import pytz
from datetime import datetime, timedelta
from google.cloud import bigquery

def convert_to_est(timestamp): 
    #fetch the timezone information
    est = pytz.timezone('US/Eastern')
    timestamp = datetime.utcfromtimestamp(timestamp / 1000)
    #convert utc to est
    est_time = timestamp.astimezone(est)
    timestamp = est.localize(timestamp)
    #return the converted value
    return est_time.isoformat()



def add_location_names(data_df, lookup_table, columns):
    client = bigquery.Client()

    # Load the lookup table
    lookup_query = f"SELECT zone_id, zone_name FROM `{lookup_table['project']}.{lookup_table['dataset']}.{lookup_table['table']}`" #add these to metadata
    lookup_data = client.query(lookup_query).to_dataframe()
    
    lookup_key, lookup_value = lookup_table['key_col'], lookup_table['value_col'] 
    
    for column in columns:
        location_col = column.split('_')[0] + "_location_name"
        data_df = data_df.merge(lookup_data, left_on=column, right_on=lookup_key, how='left')
        data_df.rename(columns={lookup_value: location_col}, inplace=True)
        data_df.drop(lookup_key, axis=1, inplace=True)

    return data_df

def apply_aggregate(transformation, source_table, execution_date):
    client = bigquery.Client()

    # Parse execution date
    execution_date = datetime.strptime(execution_date, '%Y-%m-%d')
    previous_date = execution_date - timedelta(days=1) - timedelta(days=365 * 2)
    
    source_table_addr = f"{source_table['project']}.{source_table['dataset']}.{source_table['table']}"
    
    formatted_query = transformation['query'].format(
        watermark_column=source_table['watermark_column'], 
        aggr_col=transformation['aggr_col'],
        source_table_addr=source_table_addr,
        previous_date=previous_date.date()
    )
    
    print(formatted_query)
    
    data_df = client.query(formatted_query).to_dataframe()
    
    return data_df


def transform_data(transformations, input_path = 'auto', output_path = 'auto', pipeline_name = 'NA', source_table = 'NA', execution_date = 'NA'):
    if input_path == 'auto':
        input_path = f'/tmp/{pipeline_name}_extracted.json'
    if output_path == 'auto':
        output_path = f'/tmp/{pipeline_name}_transformed.json'
        
    print('input_path:', input_path)
    
    if input_path != 'ignore':
        with open(input_path, 'r') as f:
            data_df = pd.DataFrame(json.load(f))
        
        print(data_df.head(1).to_json(orient='records'))
        print(data_df.dtypes)
    
    for transformation in transformations:
        if transformation['name'] == 'convert_to_est':
            for col in transformation['columns']:
                new_name = col.split('_')[0] + '_timestamp'
                data_df[new_name] = data_df[col].apply(lambda x: convert_to_est(x))
        elif transformation['name'] == 'add_location_names':
            data_df = add_location_names(data_df, transformation['lookup_table'], transformation['columns'])
        elif transformation['name'] == 'yellow_passengers_hour':
            data_df = apply_aggregate(transformation, source_table, execution_date)
        elif transformation['name'] == 'green_tips_day':
            data_df = apply_aggregate(transformation, source_table, execution_date)
            
    print(data_df.head(1).to_json(orient='records'))
    print(data_df.dtypes)
    
    if output_path != 'ignore':        
        # Save the transformed data to an output JSON file
        data_df.to_json(output_path, orient='records')
        print(f"Output written to {output_path}")