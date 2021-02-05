
from __future__ import print_function

import time
from builtins import range
from pprint import pprint

from airflow.utils.dates import days_ago

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
import pickle
from airflow.hooks.base_hook import BaseHook
from multiprocessing import Process, Lock, Pool, cpu_count
import pandas as pd
import calendar
from datetime import datetime, timedelta
from dateutil import relativedelta
import os
import shutil
import cx_Oracle

class EtlPeriod:
    MONTHLY="MONTHLY"
    WEEKLY="WEEKLY"
    DAILY="DAILY"


class ETLDagWrapper:
    def __init__(self, dag, etl_args):

        self.etl_args = etl_args
        self.dag = dag       

        self.etl_default_staging_parent_folder = "/root/Documents/Projects/ETL Staging"
        if not self.does_dir_exists(self.etl_default_staging_parent_folder):
            os.makedirs(self.etl_default_staging_parent_folder)
        # set default values for optional etl variables
        self.etl_args["etl_end_date"] = datetime.now()-timedelta(days=1) if self.etl_args["etl_end_date"] is None else self.etl_args["etl_end_date"]
        self.etl_args["staging_directory"] = os.path.join(self.etl_default_staging_parent_folder, self.etl_args["dest_table_name"]) if self.etl_args["staging_directory"] is None else self.etl_args["staging_directory"]
        self.etl_args["dest_reference_date_field_name"] = "REFERENCE_DATE" if self.etl_args["dest_reference_date_field_name"] is None else self.etl_args["dest_reference_date_field_name"]
        self.etl_args["dest_control_date_field_name"] = "CONTROL_DATE" if self.etl_args["dest_control_date_field_name"] is None else self.etl_args["dest_control_date_field_name"]
        self.etl_args["dest_table_columns_definition"][self.etl_args["dest_reference_date_field_name"]] = ("DATE", datetime)
        self.etl_args["dest_table_columns_definition"][self.etl_args["dest_control_date_field_name"]] = ("DATE", datetime)    
        # convert the dest table columns to upper()
        self.etl_args["dest_table_columns_definition"] = {k.upper():v for k, v in self.etl_args["dest_table_columns_definition"].items()}
        self.etl_args["dest_table_name"] = self.etl_args["dest_table_name"].upper()
        # derive variables
        self.compute_run_from_date()
        self.compute_sourcing_sql_dates()

        self.task_dress_sourcing_sql_queries = PythonOperator(
            task_id='dress_sourcing_sql_queries',
            provide_context=True,
            python_callable=self.dress_sourcing_sql_queries,
            dag=dag,
            trigger_rule='none_failed_or_skipped'
        )

        self.task_does_staging_directory_exists = BranchPythonOperator(
            task_id='does_staging_directory_exists',
            provide_context=True,
            python_callable=self.does_staging_directory_exists,
            dag=dag,
        )

        self.task_clear_staging_directory = PythonOperator(
            task_id='clear_staging_directory',
            provide_context=True,
            python_callable=self.clear_staging_directory,
            dag=dag,
        )

        self.task_create_staging_directory = PythonOperator(
            task_id='create_staging_directory',
            provide_context=True,
            python_callable=self.create_staging_directory,
            dag=dag,
        )

        self.task_does_destination_table_exists = BranchPythonOperator(
            task_id='does_destination_table_exists',
            provide_context=True,
            python_callable=self.does_destination_table_exists,
            dag=dag,
        )

        self.task_create_destination_table = PythonOperator(
            task_id="create_destination_table",
            provide_context=True,
            python_callable=self.create_destination_table,
            dag=dag
        )

        # self.task_process_ends = DummyOperator(task_id='process_ends', dag=dag)
        self.task_extract_starts = DummyOperator(task_id='extract_starts', dag=dag, trigger_rule="none_failed_or_skipped")
        self.task_extract_ends = DummyOperator(task_id='extract_ends', dag=dag)

        self.task_load = PythonOperator(
            task_id="load",
            provide_context=True,
            python_callable=self.load,
            dag=dag,
            trigger_rule='none_failed_or_skipped'
        )

        self.task_unique_test = PythonOperator(
            task_id="unique_test",
            provide_context=True,
            python_callable=self.unique_test,
            dag=dag,
        )

        self.task_filter_sourcing_sql_dates = PythonOperator(
            task_id="filter_sourcing_sql_dates",
            provide_context=True,
            python_callable=self.filter_sourcing_sql_dates,
            dag=dag
        )    
    

    def does_table_exists(self, table_name):
        table_name = table_name.upper()
        ret = False
        con = ETLDagWrapper.get_conn_object()
        cursor = con.cursor()
        try:
            query = f"select * from {table_name}"
            cursor.execute(query)
            ret = cursor.fetchone()
            # if the fetch function does not raise exception, then table exists
            ret = True
            print('ret: ', ret, query, cursor.fetchone())
        except :
            pass
        finally:
            cursor.close()
            con.close()
        return ret

    def read_df_from_db(self, q):
        con = ETLDagWrapper.get_conn_object()
        res = pd.read_sql_query(q, con)
        con.close()
        return res
        
    def does_dir_exists(self, path):
        return os.path.exists(path) and os.path.isdir(path)

    def clear_directory(self, folder_path):
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)

    @staticmethod
    def get_conn_object():
        conn = BaseHook.get_connection('data_analyst')
        un=conn.login
        pwd=conn.password
        db_uri=conn.host
        con = cx_Oracle.connect(un, pwd, db_uri)
        return con

    def execute_query_without_commit(self, query, con):
        cursor = con.cursor()
        cursor.execute(query)
        cursor.close()

    def insert_rows(self, df, table_nm, con, chunksize=1000):
        end_of_df=True
        lower_limit = 0
        while end_of_df:
            upper_limit = lower_limit+chunksize
            # convert rows and convert to list of tuples
            rows = list(df[lower_limit:upper_limit].itertuples(index=False, name=None))
            cur = con.cursor()

            insert_keys = ','.join(df.columns)
            insert_values = ', '.join([':'+str(i) for i in range(1, len(df.columns)+1)])
            cur.executemany("insert into {} ({}) values ({})".format(table_nm, insert_keys, insert_values), rows)
            cur.close()

            end_of_df = upper_limit<len(df)
            lower_limit = upper_limit

    def get_files(self, target_directory, get_full_path=True):
        fns = os.listdir(target_directory)
        if get_full_path:
            fns = [os.path.join(target_directory,f) for f in fns]
        return fns

    def does_destination_table_exists(self, **kwargs):
        dest_table_name = self.etl_args["dest_table_name"]
        if self.does_table_exists(dest_table_name):
            return "filter_sourcing_sql_dates"
        else:
            return "create_destination_table"

    def filter_sourcing_sql_dates(self, **kwargs):
        sourcing_dates_list = self.etl_args["sourcing_sql_dates"]
        # print("old sourcing dates list")
        # print(sourcing_dates_list)
        _etl_args = kwargs['ti'].xcom_pull(key=None, task_ids="init_etl_variables")
        dest_table_name = self.etl_args["dest_table_name"] 
        dest_reference_date_field_name = self.etl_args["dest_reference_date_field_name"]
        new_sourcing_dates_list = []
        # get ref_dates from dest_table
        loaded_ref_dates = list(self.read_df_from_db(f"select distinct({dest_reference_date_field_name}) from {dest_table_name}")[dest_reference_date_field_name])
        for d in sourcing_dates_list:
            if (not d[0] in loaded_ref_dates) or (d[0]==max(loaded_ref_dates)):
                new_sourcing_dates_list.append(d)
        kwargs["ti"].xcom_push(key="sourcing_sql_dates", value=new_sourcing_dates_list)

    def create_destination_table(self, **kwargs):
        dest_table_name = self.etl_args["dest_table_name"]
        dest_reference_date_field_name = self.etl_args["dest_reference_date_field_name"]
        dest_control_date_field_name = self.etl_args["dest_control_date_field_name"]
        dest_table_columns_definition = self.etl_args["dest_table_columns_definition"]
        print("create_destination_table")
        fields = [k+' '+v[0] for k, v in dest_table_columns_definition.items()]
        create_query = f"create table {dest_table_name} ({','.join(fields)})"
        con = ETLDagWrapper.get_conn_object()
        self.execute_query_without_commit(create_query, con)
        con.close()

    def dress_sourcing_sql_queries(self, **kwargs):
        # layer first outer sql query that adds reference-date and control_dates in its output, and where clause to filter control dates
        # second outer layer includes the aggregation if submitted
        agg_measures = self.etl_args['agg_measures']
        agg_columns = self.etl_args['agg_columns']
        source_control_date_field_name = self.etl_args['source_control_date_field_name']
        source_query = self.etl_args['source_query']
        dest_control_date_field_name = self.etl_args['dest_control_date_field_name']
        dest_reference_date_field_name = self.etl_args['dest_reference_date_field_name']
        sourcing_dates_list = kwargs["ti"].xcom_pull(key="sourcing_sql_dates")
        # print(sourcing_dates_list)
        if sourcing_dates_list is None:
            sourcing_dates_list = self.etl_args["sourcing_sql_dates"]
        dressed_queries = []
        for s in sourcing_dates_list:
            if s[0]>s[1]:
                raise Exception("The first item in the tuple cannot be greater than the second")

            if isinstance(s, tuple):
                _from_str = datetime.strftime(s[0],"%d%b%Y")
                _to_str =  datetime.strftime(s[1],"%d%b%Y")
                _where_clause = f"{source_control_date_field_name} between '{_from_str}' and '{_to_str}'"
            elif isinstance(s, datetime):
                _date_str = datetime.strftime(s,"%d%b%Y")
                _from_str = _date_str
                _to_str = _date_str
                _where_clause =  f"{source_control_date_field_name} = '{_date_str}'"
            else:
                raise Exception("Item in sourcing_dates_lists is not an expected type")

            if isinstance(s, tuple) and self.etl_args["optimize_extraction"]:
                _qs=[]
                cur_date=s[0]
                while cur_date <= s[1]:
                    original_source_query =  self.etl_args["source_query"]
                    _qs.append(f"select * from ({original_source_query}) where {source_control_date_field_name}='{datetime.strftime(cur_date,'%d%b%Y')}'")
                    cur_date = cur_date + timedelta(days=1)
                source_query = " union all ".join(_qs)

            _dressed_query = f"select to_date('{_from_str}') {dest_reference_date_field_name}, max({source_control_date_field_name}) over() {dest_control_date_field_name}, src.* \
                from ({source_query}) src where {_where_clause}"

            # include aggregations if not None
            if (agg_columns is not None) and (len(agg_columns)>0):
                _agg_columns = [dest_reference_date_field_name, dest_control_date_field_name] + agg_columns
                _agg_measures = ','.join([m+' '+column_alias for m, column_alias in agg_measures])
                _dressed_query = f"select {','.join(_agg_columns)}, {_agg_measures} from ({_dressed_query}) group by {','.join(_agg_columns)}"
            # add new dressed_query to list of queries
            dressed_queries.append(((s[0], s[1]), _dressed_query))
        return dressed_queries
    
    def does_staging_directory_exists(self, **kwargs):
        staging_directory = self.etl_args["staging_directory"]
        if(self.does_dir_exists(staging_directory)):
            return "clear_staging_directory"
        else:
            return "create_staging_directory"

    def clear_staging_directory(self, **kwargs):
        staging_directory = self.etl_args["staging_directory"]
        self.clear_directory(staging_directory)

    def create_staging_directory(self, **kwargs):
        staging_directory = self.etl_args["staging_directory"]
        os.makedirs(staging_directory)

    def extract(self, **kwargs):
        d_from = kwargs["d_from"]
        d_to = kwargs["d_to"]
        staging_directory = self.etl_args["staging_directory"]
        sourcing_sql_queries = kwargs["ti"].xcom_pull(key=None, task_ids="dress_sourcing_sql_queries")
        # grab the query that matches the d_from date 
        try:
            sourcing_query = [q for d,q in sourcing_sql_queries if d[0]==d_from][0]
            # skip if filter above does not return result
            print("Extracting: ")
            print(d_from)
            print(sourcing_query)
            file_to_save = os.path.join(staging_directory, f"{d_from}_{d_to}.csv")
            df = self.read_df_from_db(sourcing_query)
            df.to_csv(file_to_save, index=False)
        except IndexError:
            print("Already loaded: ")
            print(d_from)

    def load(self, **kwargs):
        print('load')
        staging_directory = self.etl_args["staging_directory"]
        dest_table_name = self.etl_args["dest_table_name"]
        dest_reference_date_field_name = self.etl_args["dest_reference_date_field_name"]
        dest_table_columns_definition = self.etl_args["dest_table_columns_definition"]
        etl_run_from_date = self.etl_args["etl_run_from_date"]
        etl_end_date = self.etl_args["etl_end_date"]

        # remove data not within range (i.e. etl_run_from_date and etl_end_date)
        con = ETLDagWrapper.get_conn_object()
        delete_query = f"delete from {dest_table_name} where {dest_reference_date_field_name} < '{datetime.strftime(etl_run_from_date, '%d%b%Y')}' and {dest_reference_date_field_name} > '{datetime.strftime(etl_end_date, '%d%b%Y')}'"           
        self.execute_query_without_commit(delete_query, con)    
        con.commit()
        con.close()

        files = self.get_files(staging_directory)
        for f in files:
            print(f)
            df = pd.read_csv(f)
            print(df.dtypes)
            # convert specified column types to pandas native types
            new_dict_types={}
            for k, v in dest_table_columns_definition.items():
                old_type = v[1]
                new_type = "str"
                if old_type == "datetime" or old_type == datetime:
                    new_type="datetime64[ns]"
                elif old_type == "int" or old_type == int:
                    new_type="int"
                elif old_type == "float" or old_type == float:
                    new_type="float"
                new_dict_types[k]=new_type
            
            print(new_dict_types)
            print(df.columns)

            df = df.astype(new_dict_types)
            if len(df)>0:
                try:
                    con = ETLDagWrapper.get_conn_object()
                    ref_date = df[dest_reference_date_field_name][0]
                    print(ref_date)
                    delete_query = f"delete from {dest_table_name} where {dest_reference_date_field_name} = '{datetime.strftime(ref_date, '%d%b%Y')}'"           
                    print("deletion")
                    print(delete_query)
                    self.execute_query_without_commit(delete_query, con)
                    print("insertion")
                    self.insert_rows(df, dest_table_name, con=con)
                    con.commit()
                    con.close()
                except:
                    con.rollback()
                    con.close()
                    raise
        
    def compute_run_from_date(self):
        dest_table_name = self.etl_args['dest_table_name']
        dest_reference_date_field_name = self.etl_args['dest_reference_date_field_name']
        etl_period = self.etl_args['etl_period']
        etl_end_date = self.etl_args['etl_end_date']
        etl_period_freq = self.etl_args['etl_period_freq']

        if etl_period==EtlPeriod.DAILY:
            etl_run_from_date = etl_end_date-timedelta(days=etl_period_freq-1)
        elif etl_period==EtlPeriod.WEEKLY:
            etl_end_date_w_day = etl_end_date.timetuple().tm_wday+1 if etl_end_date.timetuple().tm_wday<6 else 0
            etl_end_date_first_w_day = etl_end_date - timedelta(days=etl_end_date_w_day)
            etl_run_from_date = etl_end_date_first_w_day - timedelta(weeks=etl_period_freq-1)
        elif etl_period==EtlPeriod.MONTHLY:
            etl_end_date_first_m_day = etl_end_date.replace(day=1)
            etl_run_from_date = etl_end_date_first_m_day - relativedelta.relativedelta(months=etl_period_freq-1)
        # push etl_run_from_date through xcom
        # return etl_run_from_date
        self.etl_args["etl_run_from_date"]=etl_run_from_date
        
    def compute_sourcing_sql_dates(self):
        '''
        '''
        etl_period = self.etl_args['etl_period']
        etl_period_freq = self.etl_args['etl_period_freq']
        etl_end_date = self.etl_args['etl_end_date']

        etl_run_from_date = self.etl_args['etl_run_from_date']
        # set sourcing_dates_list
        # if empty then compute using: etl_period and freq
        sourcing_dates_list = []
        if etl_period==EtlPeriod.DAILY:
            sourcing_dates_list = [(etl_run_from_date+timedelta(days=f),etl_run_from_date+timedelta(days=f)) for f in range(etl_period_freq)]
        elif etl_period==EtlPeriod.WEEKLY:
            sourcing_dates_list = []
            for f in range(etl_period_freq):
                _from = etl_run_from_date+timedelta(days=7*f)
                _to = _from+timedelta(days=6)
                _to = _to if _to <= etl_end_date else etl_end_date
                sourcing_dates_list.append((_from, _to))
        elif etl_period==EtlPeriod.MONTHLY:
            sourcing_dates_list = []
            for f in range(etl_period_freq):
                _from = etl_run_from_date+relativedelta.relativedelta(months=f)
                _to = _from.replace(day=calendar.monthrange(_from.year, _from.month)[1])
                # restore the to_date to end_date if it crossed the end_date
                _to = _to if _to <= etl_end_date else etl_end_date
                sourcing_dates_list.append((_from, _to))

        # print('\n\n sourcing_dates_list \n\n')
        # print(etl_run_from_date)
        # print(etl_end_date)
        # print(self.etl_args['etl_period'])
        # print(sourcing_dates_list)

        sourcing_dates_list = [(t[0].date(), t[1].date()) for t in sourcing_dates_list]
        # return sourcing_dates_list
        self.etl_args["sourcing_sql_dates"]=sourcing_dates_list

    def unique_test(self, **kwargs):
        dest_columns = set([c.upper() for c in self.etl_args["dest_table_columns_definition"].keys()])
        agg_measures_columns = set([c[1].upper() for c in self.etl_args["agg_measures"]])
        dest_reference_date_field_name = self.etl_args["dest_reference_date_field_name"]
        dest_control_date_field_name = self.etl_args["dest_control_date_field_name"]
        dest_table_name = self.etl_args["dest_table_name"]
        colummns_to_check_for_uniqueness = list(dest_columns - agg_measures_columns) 
        _q_cols = ",".join(colummns_to_check_for_uniqueness)
        _q = f"select {_q_cols} from {dest_table_name} group by {_q_cols} having count(1)>1"
        print('_q')
        print(_q)
        df = self.read_df_from_db(_q)
        if len(df)==0:
            return True
        else:
            # truncate dest table when test fail or not ; still thinking
            raise Exception("ETL failed unique test")

def load_pkl(pkl_file):
    infile = open(pkl_file, "rb")
    queries = pickle.load(infile)
    infile.close()
    return infile
    
def dump_pkl(obj, file_path):
    outfile = open(file_path, "wb")
    pickle.dump(obj, outfile)
    outfile.close()