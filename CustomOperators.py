import datetime as dt
import os
import pyodbc 
import psycopg2
import psycopg2.extras
import pytz

from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


class MSSQLOperator(BaseOperator):

    """
    Данный класс извлекает данные из СУБД MSSQL и записывает их 
    в СУБД Greenplum.

    Атрибуты:
    ----------
    source_connection_id: str
        Идентификатор подключения Airflow для источника MSSQL
    source_script_path: str
        Путь до файла *.sql, который содержит скрипт извлечения
        данных из источника.
        Данный скрипт может быть шаблонизирован с помощью 
        следующих переменных: 

            source_table_name - название таблицы в источнике
            ts_field_name - название поля с датой изменения (ts)
            min_source_ts - минимальное значение ts для батча
            max_source_ts - максимальное значение ts для батча

    dwh_connection_id: str
        Идентификатор подключения Airflow для хранилища Greenplum
    dwh_script_path: str
        Путь до файла *.sql, который содержит скрипт удаления 
        (обеспечение идемпотентности) данных в источнике
        Данный скрипт может быть шаблонизирован с помощью 
        следующих переменных:
            dwh_table_name - название таблицы в dwh
            ts_field_name - название поля с датой изменения (ts)
            min_source_ts - минимальное значение ts для батча
            max_source_ts - максимальное значение ts для батча
            ids - перечень идентификаторов записей в батче

    source_table_name: str
        название таблицы в источнике
    dwh_table_name: str
        название таблицы в dwh
    ts_field_name: str
        название поля с датой изменения (ts)
    """

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
        self.data_for_templating = {}
        self.source_con = BaseHook.get_connection(source_connection_id)
        self.source_script_path = source_script_path
        self.dwh_con = BaseHook.get_connection(dwh_connection_id)
        self.dwh_script_path = dwh_script_path
        self.data_for_templating['source_table_name'] = source_table_name
        self.data_for_templating['dwh_table_name'] = dwh_table_name
        self.data_for_templating['ts_field_name'] = ts_field_name

    def execute(self, context):
        """
        Данный метод запускается автоматически при использовании оператора в Airflow.
        """

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
                if self.data:
                    self.transform()
                    self.load()
                    self.check()
                else:
                    print('Нет данных для загрузки.')


    def extract(self):
        """
        Извлекает данные из MSSQL.
        """
        print('Извлечение данных из MSSQL СУБД.')

        if self.data_for_templating['ts_field_name']:
            self.dwh_cur.execute(
                f"""
                SELECT MAX({self.data_for_templating['ts_field_name']}::TIMESTAMP)
                FROM {self.data_for_templating['dwh_table_name']};
                """
            )
            self.max_dwh_ts = self.dwh_cur.fetchone()[0]
        
            print('Максимальный TS данных в хранилище:', self.max_dwh_ts)

            self.data_for_templating['max_source_ts'] = (self.context['execution_date'].replace(day=28)
                                                         + dt.timedelta(days=4)).replace(day=1)

            if (not self.max_dwh_ts 
                or self.max_dwh_ts.replace(tzinfo=pytz.UTC) > self.data_for_templating['max_source_ts']):
                self.data_for_templating['min_source_ts'] = self.context['execution_date'] - dt.timedelta(days=1)
            else:
                self.data_for_templating['min_source_ts'] = self.max_dwh_ts

        print('Открываю sql-скрипт:', self.source_script_path)

        with open(
            self.source_script_path,
            'r',
            encoding="utf-8",
        ) as f:
            query = f.read().format(**self.data_for_templating)
        print(query[:100])

        print('Выполняю запрос к источнику')
        self.source_cur.execute(query)
        self.data = self.source_cur.fetchall()

    def transform(self):
        """
        Трансформирует данные.
        Должен быть переопределен, если необходима трансформация данных перед записью в DWH.
        """
        pass

    def load(self):
        """
        Запись данных в DWH.
        """
        print('Загрузка данных в хранилище.')

        print(
            'Обеспечиваю идемпотентность, открываю sql-скрипт:', 
            self.dwh_script_path
        )

        self.data_for_templating['ids'] = ','.join(["'"+str(row[0])+"'" for row in self.data])

        with open(
            self.dwh_script_path,
            'r',
            encoding="utf-8",
        ) as f:
            query = f.read().format(
                **self.data_for_templating,
            )
        print(query[:100])

        print('Выполняю запрос к dwh')
        self.dwh_cur.execute(query)                

        insert_stmt = f"INSERT INTO {self.data_for_templating['dwh_table_name']} VALUES %s"
        psycopg2.extras.execute_values(self.dwh_cur, insert_stmt, self.data)

    def check(self):
        """
        Проверка результата записи.
        """

        initial_rows_number = len(self.data)

        if self.data_for_templating['ts_field_name']:

            self.dwh_cur.execute(
                f"""
                SELECT COUNT(*)
                FROM {self.data_for_templating['dwh_table_name']}
                WHERE {self.data_for_templating['ts_field_name']} > '{self.data_for_templating['min_source_ts']}'
                    AND {self.data_for_templating['ts_field_name']} < '{self.data_for_templating['max_source_ts']}';
                """
            )

        else:

            self.dwh_cur.execute(
                f"""
                SELECT COUNT(*)
                FROM {self.data_for_templating['dwh_table_name']};
                """
            )

        total_rows_number = self.dwh_cur.fetchone()[0] 

        if total_rows_number != initial_rows_number:
            raise Exception(
                'Загруженное число строк не совпадает с полученным:',
                total_rows_number,
                initial_rows_number,
            )
        else:
            print('Загружено', initial_rows_number, 'строк.')       