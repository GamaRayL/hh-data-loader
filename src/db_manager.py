# Создайте класс DBManager, который будет подключаться к БД PostgreSQL и иметь следующие методы:

# get_companies_and_vacancies_count()
#  — получает список всех компаний и количество вакансий у каждой компании.
# get_all_vacancies()
#  — получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию.
# get_avg_salary()
#  — получает среднюю зарплату по вакансиям.
# get_vacancies_with_higher_salary()
#  — получает список всех вакансий, у которых зарплата выше средней по всем вакансиям.
# get_vacancies_with_keyword()
#  — получает список всех вакансий, в названии которых содержатся переданные в метод слова, например python.
import psycopg2


class DBManager:
    def __init__(self, database_name: str, table_vacancies: str, table_employers: str, params: dict):
        self.conn = psycopg2.connect(dbname='postgres', **params)
        self.cur = self.conn.cursor()

        self.database_name = database_name
        self.table_vacancies = table_vacancies
        self.table_employers = table_employers
        self.params = params

    def _connect_to_database(self):
        self.conn = psycopg2.connect(dbname=self.database_name, **self.params)
        self.cur = self.conn.cursor()
        self.conn.autocommit = True

    def get_companies_and_vacancies_count(self, ):
        """Получает список всех компаний и количество вакансий у каждой компании."""
        self._connect_to_database()

        result = []

        with self.conn.cursor() as cur:
            cur.execute(f"""
                SELECT e.name AS Компания, COUNT(*) AS Количество_вакансий
                FROM {self.table_employers} e
                JOIN {self.table_vacancies} v USING (employer_id)
                GROUP BY e.name
            """)

            rows = cur.fetchall()

            for row in rows:
                company_name, vacancies_count = row
                result.append({'Компания': company_name, 'Количество вакансий': vacancies_count})

        return result

    def get_all_vacancies(self):
        """Получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на
        вакансию."""
        self._connect_to_database()

        result = []

        with self.conn.cursor() as cur:
            cur.execute(f"""
                SELECT e.name AS Компания, v.name AS Вакансия, v.salary_from AS Зарплата_от,
                        v.salary_to AS Зарплата_до, v.currency AS Валюта, v.url AS Ссылка_на_вакансию
                FROM {self.table_vacancies} v
                JOIN {self.table_employers} e USING (employer_id)
            """)

            rows = cur.fetchall()

            for row in rows:
                company_name, vacancy_name, salary_from, salary_to, currency, url = row
                result.append({
                    'Компания': company_name,
                    'Вакансия': vacancy_name,
                    'Зарплата от': salary_from,
                    'Зарплата до': salary_to,
                    'Валюта': currency,
                    'Ссылка на вакансию': url
                })

        return result

    def get_avg_salary(self):
        """Получает среднюю зарплату по вакансиям."""
        self._connect_to_database()

        with self.conn.cursor() as cur:
            cur.execute(f"""
                    SELECT AVG((salary_from + salary_to) / 2.0)
                    
                    FROM {self.table_vacancies} v
                    WHERE v.currency = 'RUR'
            """)

            row = cur.fetchone()[0]

        return {"Средняя зарплата по вакансиям": round(float(row), 2)}

    def get_vacancies_with_higher_salary(self):
        """Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям."""
        self._connect_to_database()

        result = []

        with self.conn.cursor() as cur:
            cur.execute(f"""
                    SELECT name, salary_from, salary_to, currency, url FROM {self.table_vacancies} v
                    WHERE (v.salary_from + v.salary_to) / 2.0 > (
                        SELECT AVG((salary_from + salary_to) / 2.0) 
                        FROM {self.table_vacancies}
                        WHERE currency = 'RUR'
                    )
                    AND v.currency = 'RUR';
                   """)

            rows = cur.fetchall()

            for row in rows:
                name, salary_from, salary_to, currency, url = row
                result.append({
                    'Вакансия': name,
                    'Зарплата от': salary_from,
                    'Зарплата до': salary_to,
                    'Валюта': currency,
                    'Ссылка на вакансию': url
                })

        return result

    def get_vacancies_with_keyword(self, keyword):
        """Получает список всех вакансий, в названии которых содержатся переданные в метод слова, например python."""
        self._connect_to_database()

        result = []

        with self.conn.cursor() as cur:
            keyword_pattern = f"%{keyword}%"

            cur.execute(f"""
                    SELECT name, salary_from, salary_to, currency, url FROM vacancies v
                    WHERE v.name LIKE %s
                   """, (keyword_pattern,))

            rows = cur.fetchall()

            for row in rows:
                name, salary_from, salary_to, currency, url = row
                result.append({
                    'Вакансия': name,
                    'Зарплата от': salary_from,
                    'Зарплата до': salary_to,
                    'Валюта': currency,
                    'Ссылка на вакансию': url
                })

        return result
