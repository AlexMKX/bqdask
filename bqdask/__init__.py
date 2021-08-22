import multiprocessing
import dask.bag as db
import time, ipaddress
import hashlib
import pandas as pd
import logging
import tempfile
import os
import concurrent.futures
import dask.dataframe as dd
import dask.distributed
import uuid
import gcsfs
import numpy as np
from google.oauth2 import service_account
import datetime
from google.cloud import bigquery
from pathlib import Path

log = logging.getLogger(__name__)


def measure_time(func):
    # This function shows the execution time of
    # the function object passed
    def wrap_func(*args, **kwargs):
        t1 = time.time()
        result = func(*args, **kwargs)
        t2 = time.time()
        log.debug(f'Function {func.__name__!r} executed in {(t2 - t1):.4f}s')
        return result

    return wrap_func


def ip_to_int(ip) -> int:
    try:
        return ipaddress.IPv4Address(str(ip))
    except Exception as e:
        return 0


def hash_sha1(string_to_hash):
    hashed_str = hashlib.sha1(string_to_hash.encode())
    hashed_str = hashed_str.hexdigest()
    return hashed_str


class BQDask:
    credentials: str = None
    PROJECT_ID: str = None
    BQ_LOCATION: str = None
    OFFSET_DAYS: int = 9
    DASK_CHUNK_SIZE: int = 10000000
    SHORT_RUN: int = None
    DASK_PARTITIONS: int = 1500

    def __init__(self, project_id, path_parquet, export_bucket, service_acct_json, bq_location):

        dask.config.set({
            'distributed.comm.timeouts.connect': 50,
            'distributed.comm.retry.count': 50,
            'temporary-directory': os.path.join(tempfile.gettempdir(), 'dask')
        })
        cpus = multiprocessing.cpu_count()
        dcluster = dask.distributed.LocalCluster()
        dclient = dask.distributed.Client(dcluster)
        THREADS = np.sum([x.nthreads for x in dcluster.workers.values()])

        self.SHORT_RUN = BQDask.SHORT_RUN
        self.DASK_CHUNK_SIZE = BQDask.DASK_CHUNK_SIZE
        self.OFFSET_DAYS = BQDask.OFFSET_DAYS
        self.BQ_LOCATION = BQDask.BQ_LOCATION
        self.PROJECT_ID = BQDask.PROJECT_ID
        self.credentials = BQDask.credentials
        self.PROJECT_ID = project_id
        self.PATH_PARQUET = path_parquet
        self.EXPORT_BUCKET = export_bucket
        self.credentials = service_account.Credentials.from_service_account_file(
            service_acct_json, scopes=[
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/bigquery",
                "https://www.googleapis.com/auth/devstorage.full_control"
            ])
        self.service_acct_json = service_acct_json
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.service_acct_json
        self.BQ_LOCATION = bq_location

    def get_backfill_timestamp(self, last_date, offset=OFFSET_DAYS):
        import_from_date = datetime.datetime.combine(last_date.date() - datetime.timedelta(days=offset),
                                                     datetime.time(0, 0), tzinfo=last_date.tzinfo)
        return import_from_date

    def get_bq(self):
        client = bigquery.Client(credentials=self.credentials, project=self.PROJECT_ID, location=self.BQ_LOCATION)
        return client

    @measure_time
    def parallel_save(self, df_frame: pd.DataFrame, df_name: str, starting_partition=None):
        log.debug(f"Saving {df_name}")

        futures = []

        @measure_time
        def saver(df, partition, df_name):
            if partition is not None:
                dsv = df.loc[df['partition'] == partition]
                save_file = os.path.join(save_dir, f'{df_name}-{partition}.gzip')
            else:
                dsv = df
                save_file = os.path.join(save_dir, f'{df_name}.gzip')
            if os.path.exists(save_file):
                os.unlink(save_file)
            dsv.to_parquet(save_file, compression='gzip', index=False,
                           engine='pyarrow')

        if 'partition' in df_frame.columns:
            partitions = df_frame['partition'].unique()
        else:
            partitions = None
        save_dir = os.path.join(self.PATH_PARQUET, df_name)
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)
        with concurrent.futures.ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
            if partitions is not None:
                for partition in [x for x in partitions if starting_partition is None or (x >= starting_partition)]:
                    log.debug(f'Saving partition {partition} for {df_name}')
                    futures.append(executor.submit(saver, df_frame, partition, df_name))
            else:
                futures.append(executor.submit(saver, df_frame, None, df_name))
        log.debug(f'Saved {len(futures)} partitions for {df_name}')
        for f in concurrent.futures.as_completed(futures):
            if not f.exception() is None:
                log.error(f'While saving {df_name}, the exception occurred: {f.exception()}')

    @measure_time
    def bq_initial_load(self, ds_table, df_fixer=lambda x: x, limit_columns=None) -> (dd.DataFrame, str):
        if limit_columns is None:
            limit_columns = []
        client = self.get_bq()

        def bag_fix_numeric_na(record, schema):
            for n in [x.name for x in schema if x.field_type == 'FLOAT']:
                if n in record.keys():
                    record[n] = float(record[n] or 0)
            for n in [x.name for x in schema if x.field_type == 'INTEGER']:
                if n in record.keys():
                    record[n] = int(record[n] or 0)
            return record

        # ds, table = \
        table_parts = ds_table.split('.')
        table = table_parts[-1]
        ds = table_parts[-2]
        if len(table_parts) == 3:
            project = table_parts[-3]
        else:
            project = self.PROJECT_ID
        fmask = f"{uuid.uuid4().hex}{ds_table}*"
        destination_uri = f"gs://{self.EXPORT_BUCKET}/{fmask}"
        dataset_ref = bigquery.DatasetReference(project, ds)
        table_ref = dataset_ref.table(table)
        extract_job = client.extract_table(
            table_ref,
            destination_uri,
            location=self.BQ_LOCATION,
            job_config=bigquery.ExtractJobConfig(destination_format="AVRO")
        )
        r = extract_job.result()  # Waits for job to complete.
        table_ref = dataset_ref.table(table)
        schema = client.get_table(table_ref).schema
        log.debug(f'Field names and types in {ds_table}')
        for af in schema:
            log.debug(f"{af.name}: {af.field_type}")

        df_meta = self.get_bq().query(f"SELECT * from `{ds_table}` limit 1").to_dataframe()
        info_q = list()
        for field in [f.name for f in schema if f.field_type == 'INTEGER']:
            info_q.append(
                f'select "{field}" as name, max({field}) as field_max, min({field}) as field_min from `{ds_table}`')
        iq = " UNION ALL ".join(info_q)
        if len(iq) > 0:
            for i in client.query(iq).result():
                for t in [np.int16, np.int32, np.int64]:
                    if np.iinfo(t).max >= i.field_max * 2 and np.iinfo(t).min <= i.field_min * 2:
                        log.debug(f'Set {ds_table}.{field} to {t}')
                        df_meta[i.name] = df_meta[i.name].fillna(0).astype(t)
                        break

        info_q = list()
        for field in [f.name for f in schema if f.field_type == 'FLOAT']:
            info_q.append(
                f'select "{field}" as name, max({field}) as field_max, min({field}) as field_min from `{ds_table}`')
        iq = " UNION ALL ".join(info_q)
        if len(iq) > 0:
            for i in client.query(iq).result():
                # float16 can't be serialized to arrow
                for t in [np.float32, np.float64]:
                    if np.finfo(t).max >= i.field_max * 2 and np.finfo(t).min <= i.field_min * 2:
                        log.debug(f'Set {ds_table}.{i.name} to {t}')
                        df_meta[i.name] = df_meta[i.name].fillna(0.0).astype(t)
                        break
        try:
            bag = db.read_avro(
                destination_uri, blocksize=self.DASK_CHUNK_SIZE,
                storage_options={"project": self.PROJECT_ID, "token": self.credentials})
        except google.auth.exceptions as e:
            self.credentials.refresh(google.auth.transport.requests.Request())
            bag = db.read_avro(
                destination_uri, blocksize=self.DASK_CHUNK_SIZE,
                storage_options={"project": self.PROJECT_ID, "token": self.credentials})

        df = bag.map(lambda x: bag_fix_numeric_na(x, schema)).to_dataframe(meta=df_meta)

        total_rows = list(client.query(f"SELECT count(1) as total_rows from `{ds_table}`"))[0].total_rows
        cat_q = []
        for field in [f.name for f in schema if f.field_type == 'STRING']:
            cat_q.append(
                f'select "{field}" as name, SAFE_DIVIDE(APPROX_COUNT_DISTINCT({field}),{total_rows}) as uniq from `{ds_table}`')
        if len(cat_q) > 0:
            cq = " UNION ALL ".join(cat_q)
            for field_stat in client.query(cq).result():
                log.debug(f'Field {ds_table}.{field_stat.name} approx uniqueness {field_stat.uniq * 100}%')
                # !=0 because if column doesnt contain any value, categories will throw error
                if field_stat.uniq < 0.0001 and field_stat.uniq != 0:
                    log.debug(f'Set {ds_table}.{field_stat.name} to category')
                    df[field_stat.name] = df[field_stat.name].fillna('').astype('category')

        for field in [f.name for f in schema if f.field_type == 'DATETIME']:
            df[field] = dask.dataframe.to_datetime(df[field], utc=True)

        if len(limit_columns) > 0:
            for x in df.columns:
                if x not in limit_columns:
                    df = df.drop(x, axis=1)

        p_df = df_fixer(df).compute()
        gcsfs.GCSFileSystem(project=self.PROJECT_ID).delete(destination_uri)
        return p_df

    @measure_time
    def parallel_load(self, df_name):
        df_load = pd.DataFrame()
        data_dir = Path(os.path.join(self.PATH_PARQUET, df_name))
        if not os.path.exists(str(data_dir)):
            return df_load
        loaders = list()
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
        for i, parquet_path in enumerate(data_dir.glob('*.gzip')):
            loaders.append(executor.submit(pd.read_parquet, str(parquet_path), use_threads=True))
            if self.SHORT_RUN:
                break

        for f in concurrent.futures.as_completed(loaders):
            log.debug('appending..')
            if not f.exception() is None:
                log.error(f'Exception occurred: {f.exception()}')
            else:
                df_load = df_load.append(pd.DataFrame(f.result()))
        return df_load

    @staticmethod
    def conform_types(dd_old, dd_new, exclude=[]) -> dask.dataframe.DataFrame:
        """
        Makes dd_new dask dataframe types equal to the old dask dataframe types
        :param dd_old: the one types should conform to
        :param dd_new: the new dataframe
        :param exclude: list of column names to exclude
        :return: the new dataframe with changed types
        """
        for k, v in dd_new.dtypes.to_dict().items():
            if k not in exclude and v != dd_old.dtypes.to_dict()[k]:
                if dd_old.dtypes.to_dict()[k].kind != 'M':
                    dd_new[k] = dd_new[k].astype(dd_old.dtypes.to_dict()[k].name)
                else:
                    dd_new[k] = dask.dataframe.to_datetime(dd_new[k], utc=True)
        return dd_new

    def load_and_pull_bq(self, bq_table, local_name, df_fixer: lambda x: x,
                         limit_columns=None) -> dask.dataframe.DataFrame:
        if limit_columns is None:
            limit_columns = []
        dd_table = None
        parquet_path = os.path.join(self.PATH_PARQUET, local_name, '*.gzip')
        if os.path.exists(os.path.join(self.PATH_PARQUET, local_name)):
            dd_table: dd.DataFrame = dd.read_parquet(parquet_path, engine='pyarrow')
        if dd_table is None or len(dd_table.head(1)) == 0:
            dd_table = self.bq_initial_load(bq_table, df_fixer, limit_columns)
            self.parallel_save(dd_table, local_name, None)
            # re-read for the code simplicity
            return dd.read_parquet(parquet_path, engine='pyarrow')
        return dd_table

    @staticmethod
    def partition_by(dd: dd.DataFrame, column: str, part_function=None) -> dd.DataFrame:
        if part_function is None:
            dd['partition'] = dd[column].apply(
                lambda x: x.year * 100 + x.month, meta=('partition', np.uint32))
        else:
            dd['partition'] = dd[column].apply(part_function, meta=('partition', np.uint32))
        return dd

    @measure_time
    def refresh_table(self, table, query_increment_column=None,
                      df_fixer=lambda x: x, unique_column=None, limit_columns: list = None, backfill_offset=0):
        if limit_columns is None:
            limit_columns = []
        if unique_column is None and backfill_offset > 0:
            raise RuntimeError(
                f"{table} backfill_offset must be specified together with unique_column or duplicates will be created")
        dd_old = self.load_and_pull_bq(table, table, df_fixer, limit_columns)
        increment_stamp = None
        if query_increment_column is not None:
            increment_stamp = dd_old.increment_stamp.max().compute()
            log.debug(f'Last data in {table} parquet @ {increment_stamp}')
            increment_stamp = self.get_backfill_timestamp(increment_stamp, backfill_offset)
            log.debug(f'Backfilling {table} from: {increment_stamp}')
        if len(limit_columns) == 0:
            columns = "*"
        else:
            columns = ','.join(limit_columns)
        query = F"""
        select {columns}
        FROM `{table}`"""
        if increment_stamp is not None:
            query += F"""
        WHERE {query_increment_column} > @query_increment_column
    """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(name="query_increment_column", type_='TIMESTAMP',
                                                  value=increment_stamp),
                ]
            )
        else:
            job_config = bigquery.QueryJobConfig()
        if self.SHORT_RUN:
            query += f" LIMIT {self.SHORT_RUN}"
        client = self.get_bq()
        r = client.query(query, job_config=job_config).result()
        new_rows = r.total_rows
        log.debug(f'{new_rows} rows to fetch in {table}')
        dd_new = dd.from_pandas(r.to_dataframe(), chunksize=self.DASK_CHUNK_SIZE)
        if new_rows > 0:
            dd_new = df_fixer(dd_new)
            dd_new = self.conform_types(dd_old, dd_new)

            log.debug(f'Persisting {table}')
            dd_new = dd_new.persist()
            log.debug(f'Persisted {table}')
            if unique_column is not None:
                dd_old = dd_old[~dd_old[unique_column].isin(dd_new[unique_column].compute())]
                log.debug(f'Duplicates for {table} removed')
                dd_old = dd_old.append(dd_new)
                log.debug(f'Dataframe for {table} appended')
            else:
                dd_old = dd_new
            if 'partition' in dd_new.columns:
                starting_partition = int(dd_new.partition.min().compute())
                log.debug(f'Starting partition calculated: {starting_partition} for {table}')
                dd_save = dd_old.loc[dd_old['partition'] >= starting_partition]
            else:
                dd_save = dd_old
                starting_partition = None
            log.debug(f'Computing dataframe for {table}')
            df_save = dd_save.compute()
            log.debug(f'Saving dataframe for {table}')
            self.parallel_save(df_save, table, starting_partition)
