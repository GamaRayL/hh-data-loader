from abc import ABC, abstractmethod

import psycopg2


class Engine(ABC):

    @abstractmethod
    def _connect_to_database(self):
        pass

    @abstractmethod
    def _create_database(self):
        pass


class PostgresDB(Engine):
    def __init__(self, database_name: str, table_vacancies: str, table_employers: str, params: dict):
        self.conn = psycopg2.connect(dbname='postgres', **params)
        self.cur = self.conn.cursor()

        self.database_name = database_name
        self.table_vacancies = table_vacancies
        self.table_employers = table_employers
        self.params = params

        self._create_database()
        self._create_table()

    def _connect_to_database(self):
        self.conn = psycopg2.connect(dbname=self.database_name, **self.params)
        self.cur = self.conn.cursor()
        self.conn.autocommit = True

    def _create_database(self):
        self.conn.autocommit = True

        self.cur.execute(f"DROP DATABASE IF EXISTS {self.database_name}")
        self.cur.execute(f"CREATE DATABASE {self.database_name}")

        self.cur.close()
        self.conn.close()

    def _create_table(self):
        self._connect_to_database()

        with self.conn.cursor() as cur:
            cur.execute(f"""
                       CREATE TABLE IF NOT EXISTS {self.table_employers} (
                       employer_id SERIAL PRIMARY KEY,
                       name VARCHAR(255),
                       url TEXT,
                       description TEXT
                       );
                   """)

        with self.conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.table_vacancies} (
                vacancy_id SERIAL PRIMARY KEY,
                employer_id INT REFERENCES {self.table_employers}(employer_id),
                name VARCHAR(255),
                salary_from INT,
                salary_to INT,
                currency VARCHAR(10),
                published_at DATE,
                url TEXT
                );
            """)

    def insert(self, vacancies: list[dict], employers):
        self._connect_to_database()

        with self.conn.cursor() as cur:

            for emp in employers:
                cur.execute(f"""
                    INSERT INTO {self.table_employers} (name, url, description)
                    VALUES (%s, %s, %s) RETURNING employer_id""",
                            (emp['name'], emp['url'], emp['description']))

                employer_id = cur.fetchone()[0]

                for vc in vacancies:
                    if vc['hh_id'] == emp['hh_id']:
                        cur.execute(f"""
                                       INSERT INTO {self.table_vacancies} (
                                       employer_id, name, salary_from, salary_to, currency, published_at, url)
                                       VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                                    (employer_id, vc['name'], vc['salary_from'], vc['salary_to'], vc['currency'],
                                     vc['published_at'],
                                     vc['url']))
