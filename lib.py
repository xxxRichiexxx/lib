import pandas as pd
import datetime as dt
import requests
import os
import psycopg2
import psycopg2.extras
import sqlalchemy as sa
from urllib.parse import quote
import pyodbc


class ETL:

    def __init__(
        self,
        # Параметры хранилища. Передаются обязательно:
        dwh_host,
        dwh_database,
        dwh_user,
        dwh_password,
        dwh_scheme,
        dwh_port='5432',
        # Тип источника данных. Передается обязательно:
        source_type=None,
        # Параметры REST API.
        # Если в параметрах запроса или теле запроса должна передаваться дата,
        # то необходимо параметризировать данные строки,
        # чтобы в методе etl_start() произошла подстановка значений даты
        # (которые переданы явно или взяты из контекста).
        # Пример: rest_api_params_str='?last_modified_at=gte.{start_date}&last_modified_at=lt.{end_date}'
        # Если необходимо нормализовать json, то укажите параметры rest_api_json_normalize, например:
        # rest_api_json_normalize={
        #         'record_path': 'answers',
        #         'meta': ['id', 'shop_id'],
        #         'meta_prefix': "check_",
        #     }
        # Если необходимо нормализовать xml, то укажите следующие параметры(пимер):
        # rest_api_xml_transform={
        #    'xpath': "//KR",     
        #}      
        rest_api_endpoint=None,
        rest_api_method=None,
        rest_api_auth=None,
        rest_api_params_dict=None,
        rest_api_params_str=None,
        rest_api_headers=None,
        rest_api_data=None,
        rest_api_json_normalize=None,
        rest_api_xml_normalize=None,
        # Параметры SQL СУБД
        # Для работы с SQL-источниками, необходимо рядом с файлоь py разместить файл sql-запроса, например:
        # EXECUTE dbo.хп_ДляДашбордов_ЗаявкиДилера '{start_date}'
        source_host=None,
        source_database=None,
        source_port='5432',
        source_user=None,
        source_password=None,
        sql_script_path=os.path.dirname(os.path.abspath(__file__)),
        sql_normalize=True,
    ):
        """
        В конструктор всегда необходимо подавать параметры хранилища данных.
        Кроме того, необходимо подать параметры одного из источников данных:
        REST API или SQL СУБД.
        """
        self.__conn = psycopg2.connect(
            host=dwh_host,
            port=dwh_port,
            database=dwh_database,
            user=dwh_user,
            password=dwh_password,
        )
        self.__dwh_scheme = dwh_scheme
        # Сохранение типа источника
        self.source_type = source_type
        # Сохранение параметров REST API
        self.rest_api_endpoint = rest_api_endpoint
        self.rest_api_method = rest_api_method
        self.rest_api_auth = rest_api_auth
        self.rest_api_params_dict = rest_api_params_dict
        self.rest_api_params_str = rest_api_params_str
        self.rest_api_headers = rest_api_headers
        self.rest_api_data = rest_api_data
        self.rest_api_json_normalize = rest_api_json_normalize
        self.rest_api_xml_normalize = rest_api_xml_normalize
        # Сохранение параметров SQL СУБД
        self.source_host = source_host
        self.source_database = source_database
        self.source_port = source_port
        self.source_user = source_user
        self.source_password = source_password
        self.sql_script_path = sql_script_path
        self.sql_normalize = sql_normalize

    def etl_start(
        self,
        # Общие настройки
        data_type=None,
        periodic_data=True,
        start_date=None,
        end_date=None,
        end_date_EXCLUSIVE=True,
        month_offset=0,
        # Параметры трансформации,
        column_names=None,
        **context,
    ):
        """Запуск процесса ETL."""

        self.data_type = data_type
        self.periodic_data = periodic_data

        if start_date and end_date:
            self.start_date = start_date
            self.end_date = end_date
        else:
            month = context['execution_date'].month - month_offset
            if month <= 0:
                month = 12 + month
                execution_date = context['execution_date'].date().replace(month = month, year = context['execution_date'].year - 1, day=1)
            else:
                execution_date = context['execution_date'].date().replace(month = month, day=1)
        
            self.start_date = execution_date

            if end_date_EXCLUSIVE:
                self.end_date = (execution_date.replace(day=28) + dt.timedelta(days=4)) \
                    .replace(day=1)
            else:
                self.end_date = (execution_date.replace(day=28) + dt.timedelta(days=4)) \
                    .replace(day=1) - dt.timedelta(days=1)
            
        # Параметры трансформации,
        self.column_names = column_names

        self.manage()

    def manage(self):
        """Выбор загрузчика для требуемого источника данных."""

        try:
            data_extract = getattr(self, f'{self.source_type}_extract')
        except AttributeError:
            raise Exception(
                f"""Извлечение такого типа данных
                ({self.source_type}_extract) не предусмотрено."""
            )

        data_extract()
        if len(self.data) != 0:
            self.transform()
            self.load()
        else:
            print('Нет новых данных для загрузки.')

    def rest_api_extract(self):
        """Извлечение данных из REST API."""
        
        print('Извлечение данных из REST API.')

        if self.rest_api_params_dict:
            counter = len(self.rest_api_params_dict) - 1
            query_string = '?'
            for param, value in self.rest_api_params_dict.items():
                query_string += str(param) + '=' + str(value)
                if counter > 0:
                    query_string += '&'
                    counter -= 1
        elif self.rest_api_params_str:
            query_string = self.rest_api_params_str
        else:
            query_string = ''

        url = self.rest_api_endpoint + query_string \
            .format(start_date=self.start_date, end_date=self.end_date)

        if self.rest_api_data:
            self.rest_api_data = self.rest_api_data \
                .format(start_date=self.start_date, end_date=self.end_date)

        print(
            'Параметры подключения:',
            'url:',
            url,
            'Метод:',
            self.rest_api_method,
            'Аутентификация:',
            self.rest_api_auth,
            'Заголовки:',
            self.rest_api_headers,
            'Данные:',
            self.rest_api_data,
        )

        response = getattr(requests, self.rest_api_method)(
            url,
            auth=self.rest_api_auth,
            headers=self.rest_api_headers,
            data=self.rest_api_data,
            verify=False,
        )

        response.raise_for_status()

        # Раскомментировать строку ниже, если проблемы с кодировкой
        # response.encoding = 'utf-8-sig'

        if self.rest_api_json_normalize:
            json_key = self.rest_api_json_normalize.get('json_key', None)
            if json_key:
                self.data = response.json()[json_key]
            else:
                self.data = response.json()

            self.data = pd.json_normalize(
                self.data,
                self.rest_api_json_normalize.get('record_path', None),
                self.rest_api_json_normalize.get('meta', None),
                self.rest_api_json_normalize.get('meta_prefix', None),
            ).values           
        elif self.rest_api_xml_normalize:
            self.data = pd.read_xml(
                response.text,
                xpath=self.rest_api_xml_normalize.get('xpath', None)
            ).values
        else:
            self.data = [(response.text,)]

    def sql_extract(self):
        """Извлечение данных из SQL СУБД."""
        
        print('Извлечение данных из SQL СУБД.')

        print('Путь до sql-скрипта:', self.sql_script_path)

        with open(
            os.path.join(self.sql_script_path, f'{self.data_type}.sql'),
            'r',
            encoding="utf-8",
        ) as f:
            query = f.read().format(
                start_date=self.start_date,
                end_date=self.end_date
            )
        print(query)

        if os.name == 'nt':
            driver = 'SQL Server'
        else:
            driver = 'ODBC Driver 18 for SQL Server'

        if not self.sql_normalize:

            eng_str = (fr'mssql://{self.source_user}:{quote(self.source_password)}'
                       fr'@{self.source_host}/{self.source_database}?driver={driver}')

            print('Строка подключения', eng_str)

            source_engine = sa.create_engine(eng_str)

            json_data = pd.read_sql_query(
                query,
                source_engine,
            ).to_json(orient="records").encode('utf-8').decode('unicode-escape')

            self.data = [(json_data,)]
            
        else:
            con = pyodbc.connect(
                'DRIVER={'+driver+'};SERVER='+self.source_host \
                + ';DATABASE='+self.source_database \
                + ';ENCRYPT=no;UID='+self.source_user \
                + ';PWD=' + self.source_password
            )
            with con:
                with con.cursor() as cursor:
                    cursor.execute(query)
                    self.data = cursor.fetchall()

        print(self.data[:10])

    def transform(self):
        """Преобразование/трансформация данных."""

        print('ТРАНСФОРМАЦИЯ ДАННЫХ')

        result = []
        for item in self.data:
            new_item = list(item)
            if self.periodic_data:
                new_item.append(self.start_date)
            new_item.append(dt.datetime.now())
            result.append(tuple(new_item))
        self.data = result

    def load(self):
        """Загрузка данных в хранилище."""

        print('Загрузка данных в хранилище.')

        initial_rows_number = len(self.data)

        with self.__conn:
            with self.__conn.cursor() as cursor:

                if self.periodic_data:
                    cursor.execute(
                        f"""
                        DELETE FROM {self.__dwh_scheme}.{self.data_type}
                        WHERE period >= '{self.start_date}'
                            AND period < '{self.end_date}';
                        """
                    )
                else:
                    cursor.execute(
                        f"""
                        DELETE FROM {self.__dwh_scheme}.{self.data_type};
                        """
                    )

                if initial_rows_number > 1:
                    insert_stmt = f"INSERT INTO {self.__dwh_scheme}.{self.data_type} VALUES %s"
                    psycopg2.extras.execute_values(cursor, insert_stmt, self.data)
                else:
                    placeholders = ', '.join(['%s'] * len(self.data[0]))
                    insert_stmt = f"INSERT INTO {self.__dwh_scheme}.{self.data_type} VALUES ({placeholders})"
                    cursor.execute(insert_stmt, self.data[0])

                if self.periodic_data:
                    cursor.execute(
                        f"""
                        SELECT COUNT(*)
                        FROM {self.__dwh_scheme}.{self.data_type}
                        WHERE period >= '{self.start_date}'
                            AND period < '{self.end_date}';
                        """
                    )
                else:
                    cursor.execute(
                        f"""
                        SELECT COUNT(*)
                        FROM {self.__dwh_scheme}.{self.data_type};
                        """
                    )
                
                total_rows_number = cursor.fetchone()[0]

                if total_rows_number != initial_rows_number:
                    raise Exception(
                        'Загруженное число строк не совпадает с полученным:',
                        total_rows_number,
                        initial_rows_number,
                    )
                else:
                    print('Загружено', initial_rows_number, 'строк.')


