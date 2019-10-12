#!/usr/bin/python3

import pandas as pd
from config import csvpath

def subscribers_tables_merge(tablename1, tablename2, csv_path=csvpath, verbose=True):
    """
    Сводит таблицы, полученные data_loader.py, в одну. Может принимать pandas.DataFrame или имя группы, в этом
    случае группа должна быть в списке групп, а соответствующий файл - в <csv_path>

    """
    if isinstance(tablename1, pd.DataFrame):
        table1 = tablename1
    else:
        table1 = pd.read_csv(csv_path + tablename1 + '.csv', sep=";", header=0, dtype=str)
    if isinstance(tablename2, pd.DataFrame):
        table2 = tablename2
    else:
        table2 = pd.read_csv(csv_path + tablename2 + '.csv', sep=";", header=0, dtype=str)

    concatenated = table1.append(table2, ignore_index=True)

    # Выявляем тех, кто подписан на несколько групп
    # Условие gs_x != gs_x проверяет, не является ли значение NaN
    outer_joined = pd.merge(table1[{'id', 'group_subscribed'}],
                            table2[{'id', 'group_subscribed'}],
                            on='id', how='outer')
    outer_joined['groups'] = outer_joined['group_subscribed_x'] + ',' + outer_joined['group_subscribed_y']
    outer_joined.loc[   outer_joined.group_subscribed_x != outer_joined.group_subscribed_x,
                        'groups'] = outer_joined.group_subscribed_y
    outer_joined.loc[   outer_joined.group_subscribed_y != outer_joined.group_subscribed_y,
                        'groups'] = outer_joined.group_subscribed_x

    # Сводим воедино и чистим
    L_joined = pd.merge(concatenated, outer_joined[{'id', 'groups'}], on='id', how='left')
    L_joined['group_subscribed'] = L_joined['groups']
    L = L_joined.drop_duplicates('id')

    if verbose:
        print("{0} и {1} обработаны".format(str(tablename1), str(tablename2)))
    return L[L.columns[0:6]]

if __name__ == "__main__":
    print("Этот модуль должен вызываться из другой программы")
