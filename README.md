# nyctaxi
Set up of project following creation of google cloud account with free credits
1.	Set Up Google Cloud Service Account:
The Set-up
1.	Create a separate Google Service Account in each of your These projects will require the following roles:
1.	BigQuery JobUser to issue queries.
2.	BigQuery Data Editor for the use of persistent derived tables (PDTs).
3.	Additional roles required: Composer Administrator, Composer Worker, Compute Admin, Storage Admin

 
2.	Spin up Composer 2 environment with autoscaling
 
3.	Upload pipeline and metadata yaml files into GCS bucket and modules for ETL & validation in nested within DAG folder 
  	   
5.	Specify as below within admin -> variable
 
Future Iterations
1.	Data Catalog for data discovery
2.	Cloud logging and Cloud monitoring observability and health of applications
3.	IAM roles at dataset/table level to control access to BQ transformed data

