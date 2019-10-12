#!/bin/usr/python3

import unittest
import json
import requests
import pandas as pd
import data_loader # Модуль загрузчика
import data_prep   # Модуль подготовки данных
from time import sleep
from os import path, mkdir

class TestRamblerGroups(unittest.TestCase):
    """
    Для тестов нужно нормальное интернет-соединение и актуальный access_token к VK api
    """
    def test_api_response(self):
        # Тестируем, приходит ли нормальный response
        groups_info_parameters = {
            'group_id': 'scandikonur',
            'fields': ",".join(data_loader.group_data_fields),
            'access_token': data_loader.access_token, 'v': data_loader.api_version}
        subscribers_count = data_loader.get_vk_data(api_method="groups.getById", parameters=groups_info_parameters)

        self.assertIsInstance(subscribers_count[0]['members_count'], int)

    def test_data_load(self):
        # Тестируем работу загрузчика на некоторой группе
        group_member_parameters = {
            'group_id': 'angelvivaldi',
            'fields': ",".join(data_loader.user_data_fields),
            'access_token': data_loader.access_token, 'v': data_loader.api_version,
            'sort': 'id_asc', 'count': 1000, 'offset': 0
        }
        subscribers_names = data_loader.get_vk_data(api_method="groups.getMembers", parameters=group_member_parameters)

        self.assertIn('first_name', subscribers_names['items'][1])
        self.assertIn('last_name', subscribers_names['items'][1])


    def test_data_merge(self):
        # Тестируем, корректно ли мерджатся таблицы
        lj = pd.DataFrame({ 'id': ['219', '512', '550', '628', '834'],
                            'first_name': ['Роман', 'Егор', 'Назарій', 'Василий', 'Сергей'],
                            'last_name': ['Акамёлков', 'Деметрадзе', 'Куля', 'Ефанов', 'Тарасов'],
                            'bdate': ['NULL', 'NULL', 'NULL', 'NULL', 'NULL'],
                            'city': ['NULL', 'NULL', 'Львов', 'Москва', 'Санкт-Петербург'],
                            'group_subscribed': ['livejournal', 'livejournal', 'livejournal', 'livejournal', 'livejournal']  })

        champ = pd.DataFrame({ 'id': ['219', '309', '347', '348', '374'],
                            'first_name': ['Роман', 'Ilya', 'Андрей', 'Галина', 'Сергей'],
                            'last_name': ['Акамёлков', 'Krivonogov', 'Бойко', 'Румянцева', 'Волошин'],
                            'bdate': ['NULL', '18.10.1988', 'NULL', 'NULL', '3.3.1985'],
                            'city': ['NULL', 'Санкт-Петербург', 'NULL', 'NULL', 'Санкт-Петербург'],
                            'group_subscribed': ['championat', 'championat', 'championat', 'championat', 'championat']  })

        afisha = pd.DataFrame({ 'id': ['109', '348', '619', '628', '834'],
                            'first_name': ['Ольга', 'Галина', 'Алексей', 'Василий', 'Сергей'],
                            'last_name': ['Горюнова', 'Румянцева', 'Бардаш', 'Ефанов', 'Тарасов'],
                            'bdate': ['22.3', 'NULL', 'NULL', 'NULL', 'NULL'],
                            'city': ['Санкт-Петербург', 'NULL', 'NULL', 'Москва', 'Санкт-Петербург'],
                            'group_subscribed': ['afisha', 'afisha', 'afisha', 'afisha', 'afisha']  })

        resulting_groups = ['afisha', 'afisha',
                            'championat', 'championat', 'championat',
                            'championat,afisha',
                            'livejournal', 'livejournal',
                            'livejournal,afisha', 'livejournal,afisha',
                            'livejournal,championat']

        merged = data_prep.subscribers_tables_merge(lj, champ, verbose=False)
        merged = data_prep.subscribers_tables_merge(merged, afisha, verbose=False)

        self.assertEqual(sorted(list(merged['group_subscribed'])), resulting_groups)


if __name__ == "__main__":
    print("Для тестов нужно нормальное интернет-соединение и актуальный access_token к VK api")
    unittest.main()
