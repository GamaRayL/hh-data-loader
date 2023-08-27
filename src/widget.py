from src.config import config
from src.db_manager import DBManager
from src.hh_api import HeadHunterAPI
from src.postgres_db import PostgresDB


def widget():
    employers = {
        'Ozon': 2180,
        'Skyeng': 1122462,
        '05.ru': 1150295,
        'Space307': 2913350,
        'Яндекс': 1740,
        'Wildberries': 87021,
        'Тинькофф': 78638,
        'МТС': 3776,
        'Сбербанк': 3529,
        'Азбука вкуса': 2120
    }

    all_vacancies = []
    all_employers = []

    hh_api = HeadHunterAPI()
    for emp in employers.values():
        vacancies = hh_api.get_vacancies(emp)
        employer = hh_api.get_employer(emp)

        all_vacancies.extend(vacancies)
        all_employers.append(employer)

    params = config()
    db = PostgresDB('db', 'vacancies', 'employers', params)
    db.insert(all_vacancies, all_employers)
    db_manager = DBManager('db', 'vacancies', 'employers', params)

    print('Список компаний и количества вакансий у них имеющихся.')
    print(f'{db_manager.get_companies_and_vacancies_count()}\n')

    print('Список всех вакансий с подробностями.')
    print(f'{db_manager.get_all_vacancies()}\n')

    print('Средняя зарплата по вакансиям.')
    print(f'{db_manager.get_avg_salary()}\n')

    print('Список всех вакансий, у которых зарплата выше средней по всем вакансиям.')
    print(f'{db_manager.get_vacancies_with_higher_salary()}\n')

    print('Список всех вакансий, в названии которых содержатся переданные в метод слова, например Python.')
    print(db_manager.get_vacancies_with_keyword('Python'))
