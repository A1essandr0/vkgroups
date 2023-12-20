import os
import pandas as pd

from processor import DataProcessor
from libs.config_loader import Config


async def test_data_merge():
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

        dp = DataProcessor(config=Config())

        merged = dp._merge_tables(lj, champ, verbose=False)
        merged = dp._merge_tables(merged, afisha, verbose=False)
        assert sorted(list(merged['group_subscribed'])) == resulting_groups
