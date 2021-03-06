#!/usr/bin/python

# Задача: сделать ежедневно обновляемую базу данных о подписчиках некоторого множества групп ВК. 
# Например:
# 'rambler', 'ramblermail', 'horoscopesrambler', 'championat', 'championat.auto', 'championat_cybersport', 'livejournal',
# 'afisha'.
# В базе должны содержаться записи вида: id, Имя, Фамилия, Дата рождения (если указана), Город (если указан), Членство в
# группах (это текстовое поле, в котором через запятую перечислены группы интересующего множества, например 'rambler,
# afisha, livejournal'). 

# Получать изменения состава подписчиков (дельты) через api, по-видимому, нельзя. Есть один обходной
# путь - получать списки отсортированными по времени (sort=time_desc) и отбрасывать вчерашние, но для этого нужно иметь права
# модератора сообщества. Кроме того, так мы не получим данных о тех, кто покинул сообщество.
# В итоге приходится ежедневно забирать не дельты, а списки подписчиков целиком. Для сообществ с миллионами участников
# (например, Чемпионат) это занимает некоторое время (из-за ограничений vk api на загрузку). В результате в процессе 
# загрузки данные уже могут измениться.

# Т.к. как база используется sqlite, то решено было не расшивать отношение M:N Подписчики-Сообщества в три нормализованные
# таблицы, а просто сделать одну плоскую, описанную выше. Жертвуем атомарностью столбца "Членство в группах".

# ***

# Весь процесс, повторяемый ежедневно:
# 1. Загружаем данные о подписчиках по каждому из сообществ, получаем несколько таблиц в csv.
# 2. Проверяем корректность / чистим от дубликатов внутри каждой таблицы.
# 3. Соединяем таблицы в одну средствами pandas, получая новое состояние базы.
# (4.) Выгружаем текущее состояние базы и рассчитываем дельту между этими двумя состояниями (список участников, которые покинули
# или вступили в группу)
# (5.) Применяем дельту к текущему состоянию базы.
# Т.к. работает все довольно быстро и узкое место в производительности - взаимодействие с VK, то последние два пункта
# решено не применять. Вместо дельт просто каждый день создается новая база.

import pandas as pd
from os import path
from datetime import date
from sqlite3 import connect

import config # Настройки
import data_loader # Модуль загрузчика
import data_loader_async # Асинхронный вариант загрузчика
import data_prep   # Модуль подготовки данных

# Если состояние базы актуально, скрипт останавливается
if path.exists('subscribers.db') and date.fromtimestamp(path.getctime("subscribers.db")) == date.today():
    raise FileExistsError("Состояние базы актуально")

# Блок обращений к vk api (синхронный data_loader или асинхронный data_loader_async)
# Если csv-файлы уже загружены, установить loader в None.
loader = data_loader_async
if loader:
    loader.get_subscribers_data_to_csv()

# Блок загрузки в базу
GList = list(config.groups_list)
current_table = pd.read_csv(config.csvpath + GList.pop() + '.csv', sep=";", header=0, dtype=str)

while GList != []:
    next_group = GList.pop()
    current_table = data_prep.subscribers_tables_merge(current_table, next_group, verbose=True)
    print("{0} обработано".format(next_group))

fields = ', '.join(
    [field_name + ' TEXT' for field_name in config.user_data_fields]
) + ', group_subscribed TEXT'
if ";" in fields:
    raise Exception("Возможна SQL-инъекция")

conn = connect("subscribers.db")
creating_query = "CREATE TABLE if not exists Subscribers ({0})".format(fields)
conn.execute(creating_query)

current_table.to_sql("Subscribers", conn, index=False, if_exists='replace')
print("Загрузка данных выполнена")
