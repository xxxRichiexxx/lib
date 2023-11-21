from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

from datetime import datetime as dt

import pyodbc 
from airflow.hooks.base import BaseHook
import os

import psycopg2
import psycopg2.extras


class MSSQLOperator(BaseOperator):

    @apply_defaults
    def __init__(
        self,
        source_connection_id,
        source_script_path,
        dwh_connection_id,
        dwh_script_path,
        source_table_name=None,
        dwh_table_name=None,
        ts_field_name = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.source_con = BaseHook.get_connection(source_connection_id)
        self.source_script_path = source_script_path
        self.dwh_con = BaseHook.get_connection(dwh_connection_id)
        self.dwh_script_path = dwh_script_path
        self.source_table_name = source_table_name
        self.dwh_table_name = dwh_table_name
        self.ts_field_name = ts_field_name

    def execute(self, context):

        self.context = context

        dwh_connection = psycopg2.connect(
            host=self.dwh_con.host,
            port=self.dwh_con.port,
            database=self.dwh_con.schema,
            user=self.dwh_con.login,
            password=self.dwh_con.password,
        )

        self.dwh_cur = dwh_connection.cursor()

        if os.name == 'nt':
            driver = 'SQL Server'
        else:
            driver = 'ODBC Driver 18 for SQL Server'  

        source_connection = pyodbc.connect(
            'DRIVER={'+driver+'};SERVER='+self.source_con.host \
            + ';DATABASE='+self.source_con.schema \
            + ';ENCRYPT=no;UID='+self.source_con.login \
            + ';PWD=' + self.source_con.password
        )        

        self.source_cur = source_connection.cursor()

        with dwh_connection, source_connection:
            with self.dwh_cur, self.source_cur:
                self.extract()
                self.transform()
                self.load()


    def extract(self):

        print('Извлечение данных из MSSQL СУБД.')

        kwargs = {}
        kwargs['source_table_name'] = self.source_table_name

        if self.ts_field_name:
            self.dwh_cur.execute(
                f"""
                SELECT MAX({self.ts_field_name})
                FROM {self.dwh_table_name};
                """
            )
            self.max_dwh_ts = self.dwh_cur.fetchone()
        
            print('Максимальный TS данных в хранилище:', self.max_dwh_ts)

            kwargs{'min_source_ts'} = self.max_dwh_ts
            kwargs{'max_source_ts'} = (self.context['execution_date'].replace(day=28) + dt.timedelta(days=4)) \
                    .replace(day=1)

        print('Открываю sql-скрипт:', self.source_script_path)

        with open(
            self.source_script_path,
            'r',
            encoding="utf-8",
        ) as f:
            query = f.read().format(**kwargs)
        print(query)

        print('Выполняю запрос к источнику')
        self.source_cur.execute(query)
        self.data = self.source_cur.fetchall()

    def transform(self):
        pass

    def load(self):

        print('Загрузка данных в хранилище.')

        print('Обеспечиваю идемпотентность, открываю sql-скрипт:',  self.dwh_script_path)

        ids = ','.join([row[0] for row in self.data])

        with open(
            self.dwh_script_path,
            'r',
            encoding="utf-8",
        ) as f:
            query = f.read().format(
                ids=ids,
                dwh_table_name=self.dwh_table_name,
            )
        print(query)

        print('Выполняю запрос к dwh')
        self.dwh_cur.execute(query)                

        insert_stmt = f"INSERT INTO {self.dwh_table_name} VALUES %s"
        psycopg2.extras.execute_values(self.dwh_cur, insert_stmt, self.data)