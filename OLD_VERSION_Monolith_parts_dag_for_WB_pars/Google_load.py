from google.cloud import bigquery


def check_created(client: bigquery, dataset_id:str, table_id:str) -> None:
    """Check dataset and table created

    Args:
        client (bigquery): bigquery.Client() function
        dataset_id (str): Dataset name
        table_id (str): Table name
    """
    try:
        client.create_dataset(dataset_id)
        print(f"Created dataset {dataset_id}")
    except Exception as E:
        print(f"Dataset {dataset_id} already exists", E, sep='\n')

    try:
        client.create_table(table_id)
        print(f"Created table {table_id}")
    except Exception as E:
        print(f"Table {table_id} already exists", E, sep='\n')


def load_CSV_to_bq(**kwargs) -> None | bool:
    """Load in BigQuery use CSV (StringIO)

    Returns:
        None | bool: Success of the download process
    """
    data, dataset_id, table_id = kwargs['data'], \
    kwargs['dataset_id'], \
    kwargs['table_id']

    # Define the Python function that encapsulates Example 1 logic
    try:
        client = bigquery.Client()
        check_created(client, dataset_id, table_id)
        job_config = bigquery.LoadJobConfig()
        job_config.skip_leading_rows = 1
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.field_delimiter = '|'
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job_config.autodetect = True

        table_ref = client.dataset(dataset_id).table(table_id)
        job = client.load_table_from_file(
            data,
            table_ref,
            job_config=job_config
        )
        job.result()
    except Exception as e:
        print("Error in load CSV to BigQuery", e, sep='\n')
        return False
