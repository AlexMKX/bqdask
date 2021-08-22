###### BQDask - the fast incremental synchronizer from BigQuery to Pandas using DASK

Purpose:
Sometime you prefer to access your data, which resides in BigQuery locally using pandas.

Usage:

``bqd = bqdask.BQDask(project_id='Google Cloud Project', path_parquet='Path where table date should be saved',
                        export_bucket='Goocle Cloud Storage Bucket name', service_acct_json='Service account.json path', bq_location='EU')``

```
   def df_fixer(df):
        df = df.replace({'somefield': {1.0: True, 0.0: False}})

        #required for incremental sync to work.
        df.increment_stamp = dd.to_datetime(df.created_timestamp,
                                              unit='ms')
        #create partition column for partitioning
        df = partition_by(df, "created_timestamp")
        return df

    refresh_table(table='bq_project.bq_dataset.bq_table', query_increment_column='created_timestamp',
                  unique_column='id',
                  limit_columns='id,field1,field2,field3'.split(
                      ','), df_fixer=df_fixer)
```

