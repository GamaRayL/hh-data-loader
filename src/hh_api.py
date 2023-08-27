from abc import ABC, abstractmethod

import requests

from src.exceptions import ParsingError


class Engine(ABC):

    @abstractmethod
    def _get_request(self, employer):
        pass

    @abstractmethod
    def get_vacancies(self, employer):
        pass


class HeadHunterAPI(Engine):
    BASE_URL = 'https://api.hh.ru'

    def __init__(self):
        self.params = {
            'page': 1,
            'per_page': 20,
            "archived": False
        }

    @staticmethod
    def _extract_salary(salary):
        if not salary:
            return None, None, None
        return salary.get('from'), salary.get('to'), salary.get('currency')

    def _convert_vacancy(self, vacancy):
        salary_from, salary_to, currency = self._extract_salary(vacancy['salary'])

        return {
            "hh_id": vacancy['employer']['id'],
            "name": vacancy['name'],
            "salary_from": salary_from,
            "salary_to": salary_to,
            "currency": currency,
            "published_at": vacancy['published_at'],
            "url": vacancy['alternate_url'],
        }

    def _get_request(self, endpoint):
        response = requests.get(self.BASE_URL + endpoint, params=self.params)

        if response.status_code != 200:
            raise ParsingError(f'Ошибка получения вакансий! Статус {response.status_code}')

        data = response.json()
        if not data:
            raise ParsingError('Пустой ответ от API')

        return data

    def get_vacancies(self, employer_id):
        endpoint = f'/vacancies?employer_id={employer_id}'
        vacancies = self._get_request(endpoint)['items']

        return [self._convert_vacancy(vc) for vc in vacancies]

    def get_employer(self, employer_id):
        endpoint = f'/employers/{employer_id}'
        employer = self._get_request(endpoint)

        return {
            "hh_id": employer['id'],
            "name": employer['name'],
            "url": employer['alternate_url'],
            "description": employer['description']
        }
