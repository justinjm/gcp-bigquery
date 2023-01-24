

######################################################################################
#
#   Google Cloud BigQuery
#
#   https://cloud.google.com/bigquery/docs/
#   https://googleapis.github.io/google-cloud-python/latest/bigquery/reference.html
#
######################################################################################


import sys
import argparse
from google.cloud import bigquery


# Create BigQuery Dataset
def bq_create_dataset(dataset_id, location):
    '''
        Creates a BigQuery Dataset
        
        Input(s):   dataset_id: Dataset ID (name)
                    location:   US, EU, asia-northeast1 (Tokyo), europe-west2 (London), asia-southeast1 (Singapore), australia-southeast1 (Sydney)
        
        Datasets are top-level containers that are used to organize and control
        access to your tables and views. A table or view must belong to a dataset.
        
        Access Control:
        
            Tables and views are child resources of datasets — they inherit permissions from their parent dataset.
            You share access to BigQuery tables and views using project-level IAM roles and dataset-level access controls.
            Currently, you cannot apply access controls directly to tables or views.
    
            Project-level access controls determine the users, groups, and service accounts allowed to access all datasets, tables, views, and table data within a project.
            Dataset-level access controls determine the users, groups, and service accounts allowed to access the tables, views, and table data in a specific dataset.
            Access controls cannot be applied during dataset creation in the UI or command-line tool.
            However, Using the API, you can apply access controls during dataset creation by calling the datasets.insert method, or you can apply access controls after dataset creation by calling the datasets.patch method.
        Dataset Limitations:
        
            - Dataset names must be unique per project.
            - All tables referenced in a query must be stored in datasets in the same location.
            - When copying a table, datasets containing the source and destination table must reside in the same location.
            - After a dataset has been created, the geographic location becomes immutable. There are two options:
                1) Regional (Tokyo, Sydney, London, etc)
                2) Multi-Regional (US or EU)
        Location Considerations:
        
            - Colocate your BigQuery dataset and your external data source. (BigQuery must be in same region as data source)
            - Colocate your Cloud Storage buckets for loading data.
            - Colocate your Cloud Storage buckets for exporting data.
            EXCEPTION: If your dataset is in the US multi-regional location, you can load/export data from a Cloud Storage bucket in any regional or multi-regional location
        Moving BigQuery data between locations:
            - You cannot change the location of a dataset after it is created.
            - You cannot move a dataset from one location to another.
            - If you need to move a dataset from one location to another, follow this process:
                1) Export the data from your BigQuery tables to a regional or multi-region Cloud Storage bucket in the same location as your dataset.
                2) Copy or move the data from your Cloud Storage bucket to a regional or multi-region bucket in the new location
                3) Create a new BigQuery dataset (in the new location).
                4) Load your data from the Cloud Storage bucket into BigQuery.
    
    '''
    try:
        client               = bigquery.Client()
        dataset_ref          = client.dataset(dataset_id)
        dataset_obj          = bigquery.Dataset(dataset_ref)
        dataset_obj.location = location
        dataset              = client.create_dataset(dataset_obj)
        print('[ INFO ] Created {} at {}'.format(dataset_id, dataset.created))
    except Exception as e:
        print('[ ERROR ] {}'.format(e))





def bq_create_table_empty(dataset_id, table_id):
    '''
        Creates an empty BigQuery Table
        
        USAGE:
        bq_create_table_empty(  dataset_id = 'ztest1',
                                table_id = 'ztable1')
        
        Required Permissions:
        To create a table, you must have WRITER access at the dataset level,
        or you must be assigned a project-level IAM role that includes bigquery.tables.create permissions.
        The following predefined, project-level IAM roles include bigquery.tables.create permissions:
            bigquery.dataEditor
            bigquery.dataOwner
            bigquery.admin
    
    '''
    try:
        schema = [
                                            bigquery.SchemaField('id',        'INTEGER',  mode='REQUIRED'),
                                            bigquery.SchemaField('name',      'STRING',   mode='NULLABLE'),
                                            bigquery.SchemaField('state',     'STRING',   mode='NULLABLE'),
                                            bigquery.SchemaField('loan_amnt', 'FLOAT',    mode='NULLABLE'),
                                            bigquery.SchemaField('flag',      'INTEGER',  mode='NULLABLE'),
                                         ]
        
        client      = bigquery.Client()
        dataset_ref = client.dataset(dataset_id)
        table_ref   = dataset_ref.table(table_id)
        table       = bigquery.Table(table_ref, schema=schema)
        table       = client.create_table(table)
        
        assert table.table_id == table_id
        print('[ INFO ] Created {} at {}'.format(table_id, table.created))
    except Exception as e:
        print('[ ERROR] {}'.format(e))





def bq_create_table_from_gcs(dataset_id, table_id, gcs_path):
    '''
        Create BigQuery Native Table from Google Cloud Storage (Schema is auto-detected)
        
        USAGE:
        bq_create_table_from_gcs( dataset_id = 'demo_dataset1',
                                  table_id   = 'table_loans',
                                  gcs_path   = 'gs://demos-vertex-ai-bq-staging/loan_200k.csv')
        
        Optional Schema Config:
        schema= [
                    bigquery.SchemaField('id',              'STRING',   mode='REQUIRED'),
                    bigquery.SchemaField('member_id',       'STRING',   mode='REQUIRED'),
                    bigquery.SchemaField('loan_amnt',       'INTEGER',  mode='NULLABLE'),
                    bigquery.SchemaField('term_in_months',  'INTEGER',  mode='NULLABLE'),
                    bigquery.SchemaField('interest_rate',   'FLOAT',    mode='NULLABLE'),
                    bigquery.SchemaField('payment',         'FLOAT',    mode='NULLABLE'),
                    bigquery.SchemaField('grade',           'STRING',   mode='NULLABLE'),
                    bigquery.SchemaField('sub_grade',       'STRING',   mode='NULLABLE'),
                    bigquery.SchemaField('employment_length', 'INTEGER',mode='NULLABLE'),
                    bigquery.SchemaField('home_owner',      'INTEGER',  mode='NULLABLE'),
                    bigquery.SchemaField('income',          'INTEGER',  mode='NULLABLE'),
                    bigquery.SchemaField('verified',        'INTEGER',  mode='NULLABLE'),
                    bigquery.SchemaField('default',         'INTEGER',  mode='NULLABLE'),
                    bigquery.SchemaField('purpose',         'STRING',   mode='NULLABLE'),
                    bigquery.SchemaField('zip_code',        'STRING',   mode='NULLABLE'),
                    bigquery.SchemaField('addr_state',      'STRING',   mode='NULLABLE'),
                    bigquery.SchemaField('open_accts',      'INTEGER',  mode='NULLABLE'),
                    bigquery.SchemaField('credit_debt',     'INTEGER',  mode='NULLABLE')
                ],
    
    '''
    try:
        client      = bigquery.Client()
        dataset_ref = client.dataset(dataset_id)
        
        job_config = bigquery.LoadJobConfig()
        job_config.autodetect = True
        job_config.skip_leading_rows = 1
        job_config.source_format = bigquery.SourceFormat.CSV
        load_job = client.load_table_from_uri(
            gcs_path,
            dataset_ref.table(table_id),
            job_config=job_config)
        
        print('[ INFO ] Starting BigQuery load job {}'.format(load_job.job_id))
        load_job.result()
        
        destination_table = client.get_table(dataset_ref.table(table_id))
        print('[ INFO ] Loaded {} rows into {}'.format(destination_table.num_rows, table_id))
    
    except Exception as e:
        print('[ ERROR] {}'.format(e))





def bq_insert_rows(dataset_id, table_id, rows_to_insert):
    '''
        Insert rows into a BigQuery Table via the streaming API
        
        Note:
            The table must already exist and have a defined schema
            rows_to_insert = List of variables (id, date, value1, value2, etc.)

    '''
    try:
        client    = bigquery.Client()
        table_ref = client.dataset(dataset_id).table(table_id)
        table     = client.get_table(table_ref)
        errors    = client.insert_rows(table, rows_to_insert)
        if errors == []:
            print('[ INFO ] Inserted {} rows into BigQuery table {}'.format(len(rows_to_insert), table_id))
    except Exception as e:
        print('[ ERROR] {}'.format(e))





def bq_query(query, location='US'):
    '''
        Query BigQuery Table(s)
        
        location: US, EU, asia-northeast1 (Tokyo), europe-west2 (London), asia-southeast1 (Singapore), australia-southeast1 (Sydney)
        
    '''
    try:
        client = bigquery.Client()
        
        query_job = client.query(query, location=location)
        
        for i, row in enumerate(query_job):
            if i <= 10:
                print(row)
        
        print('[ INFO ] Query returned {} row(s)'.format( i+1 ))
        return query_job
    except Exception as e:
        print('[ ERROR] {}'.format(e))





def bq_create_view(view_dataset_id, view_id, query):
    '''
        Create BigQuery View
        
        USAGE:
        bq_create_view( view_dataset_id = 'demo_dataset1',
                        view_id = 'demo_view1',
                        query = " select member_id, loan_amnt, zip_code, `default` from `{}.{}.{}` ".format(args['project_id'], args['dataset_id'], args['table2_id'])
                      )
        
    '''
    try:
        client = bigquery.Client()
        shared_dataset_ref = client.dataset(view_dataset_id)
        
        view_ref = shared_dataset_ref.table(view_id)
        view = bigquery.Table(view_ref)
        
        view.view_query = query
        view = client.create_table(view)
        
        print('[ INFO ] Successfully created view at {}'.format(view.full_table_id))
    except Exception as e:
        print('[ ERROR] {}'.format(e))





def bq_delete_dataset(dataset_id):
    '''
        Deletes a Dataset (Deleting a dataset is permanent)
        
        USAGE:
        bq_delete_dataset('ztest1')
        
        Required Permissions:
        Must have OWNER access at the dataset level,
        or you must be assigned a project-level IAM role that includes bigquery.datasets.delete permissions.
        If the dataset contains tables, bigquery.tables.delete is also required.
        The following predefined, project-level IAM roles include both bigquery.datasets.delete and bigquery.tables.delete permissions:
            bigquery.dataOwner
            bigquery.admin
    
    '''
    try:
        client = bigquery.Client()
        dataset_ref = client.dataset(dataset_id)
        client.delete_dataset(dataset_ref, delete_contents=True)  # Set delete_contents=True to delete Dataset Tables
        print('Dataset {} has been deleted.'.format(dataset_id))
    except Exception as e:
        print('[ ERROR] {}'.format(e))





######################################################################################
#
#   Main
#
######################################################################################


if __name__ == "__main__":

    # ARGS - Used for Testing
    '''
    args =  {
                "project_id":   "demos-vertex-ai",
                "dataset_id":   "demo_dataset1",
                "location":     "US",
                "table1_id":    "table_empty",
                "table2_id":    "table_loans",
                "gcs_path":     "gs://demos-vertex-ai-bq-staging/loan_200k.csv"
            }
    '''
    
    # Arguments
    ap = argparse.ArgumentParser()
    ap.add_argument("--project_id", required=True, help="GCP Project ID")
    ap.add_argument("--dataset_id", required=True, help="BigQuery Dataset ID")
    ap.add_argument("--location",   required=True, help="BigQuery Dataset Geographic Location")
    ap.add_argument("--table1_id",  required=True, help="BigQuery Table Name (empty table)")
    ap.add_argument("--table2_id",  required=True, help="BigQuery Table Name (GCS loaded table)")
    ap.add_argument("--gcs_path",   required=True, help="Google Cloud Storage location")
    ap.add_argument("--view_id",    required=True, help="Name/ID of BigQuery View")
    args = vars(ap.parse_args())
    
    # Create BigQuery Dataset
    bq_create_dataset(dataset_id=args['dataset_id'], location=args['location'])
    
    # Create BigQuery Table (empty table)
    bq_create_table_empty(dataset_id=args['dataset_id'], table_id=args['table1_id'] )
    
    # Create BigQuery Table (from Google Cloud Storage)
    bq_create_table_from_gcs( dataset_id=args['dataset_id'], table_id=args['table2_id'], gcs_path=args['gcs_path'] )
    
    # Pause for user input
    input_resp = input('[ INFO ] BigQuery datasets and tables have been created. Press y to continue:  ')
    if input_resp == 'y':
        pass
    else:
        sys.exit()
    
    # Query Table1
    query = ''' select count(*) as count from `{}.{}.{}` '''.format(args['project_id'], args['dataset_id'], args['table1_id'])
    print('\n[ INFO ] Executing query against {}\n{}'.format(args['table1_id'], query) )
    bq_query( query, location=args['location'] )
    
    # Query Table2
    query = ''' select count(*) as count from `{}.{}.{}` '''.format(args['project_id'], args['dataset_id'], args['table2_id'])
    print('\n[ INFO ] Executing query against {}\n{}'.format(args['table2_id'], query) )
    bq_query( query, location=args['location'] )
    
    # Pause for user input
    input_resp = input('[ INFO ] Table 1 still needs data, so the next step will insert records into the empty table.\n[ INFO ] Press y to continue:  ')
    if input_resp == 'y':
        pass
    else:
        sys.exit()
    
    # Insert data
    rows_to_insert = [
            ('1000', 'dan',   'NC', 100.20, 0),
            ('1001', 'dan',   'NC',  50.00, 1),
            ('1002', 'frank', 'CA', 500.00, 0),
            ('1003', 'dean',  'NV',  10.10, 1)
        ]
    bq_insert_rows(args['dataset_id'], args['table1_id'], rows_to_insert)
    
    # Query Table2 (again)
    query = ''' select count(*) as count from `{}.{}.{}` '''.format(args['project_id'], args['dataset_id'], args['table1_id'])
    print('\n[ INFO ] Executing query against {}\n{}'.format(args['table1_id'], query) )
    bq_query( query, location=args['location'] )
    
    # Pause for user input
    input_resp = input('[ INFO ] Data is loaded in both tables. Next step will create a view on top of table 2.\n[ INFO ] Press y to continue:  ')
    if input_resp == 'y':
        pass
    else:
        sys.exit()
    
    # Create View on Loan Table
    bq_create_view( view_dataset_id = args['dataset_id'],
                        view_id = args['view_id'],
                        query = " select member_id, loan_amnt, zip_code, `default` from `{}.{}.{}` ".format(args['project_id'], args['dataset_id'], args['table2_id'])
                      )
    
    # Pause for user input
    input_resp = input('[ INFO ] Demoflow is complete. Press y to DELETE all assets or any other key to keep the assets:  ')
    if input_resp == 'y':
        pass
    else:
        sys.exit()
    
    # Cleanup - Delete Dataset and Tables
    bq_delete_dataset(args['dataset_id'])
