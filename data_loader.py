#!/bin/usr/python3

import json
import requests
from time import sleep
from os import path, mkdir
from config import groups_list, api_version, user_data_fields, group_data_fields, csvpath
from sconfig import access_token

def get_vk_data(api_method, parameters):
    """
    Функция обращается к названному методу из списка https://vk.com/dev/methods, параметры метода передаются в словаре
    parameters. Возвращает список, состоящий из словарей.
    """
    method_url = "https://api.vk.com/method/" + api_method
    req = requests.post(method_url, data=parameters)
    return json.loads(req.text)['response']

def get_subscribers_data_to_csv(f_path=csvpath, verbose=True,
                            groups=groups_list, group_data_fields=group_data_fields, user_data_fields=user_data_fields,
                            access_token=access_token, api_version=api_version):
    """
    Для каждого сообщества ВК, указанного в groups_list функция вытаскивает число подписчиков сообществ, а затем выгружает
    для всех из них данные полей, указанных в user_data_fields.
    Работает долго из-за того, что можно вытаскивать не более 1000 полей за раз и ограничений на однотипные запросы, из-за
    которых приходится делать паузы.
    Выгрузки помещаются в <path>/<grp>.csv, после завершения работы они не удаляются и могут занимать прилично места.
    """
    if not path.exists(f_path):
        mkdir(f_path)

    for grp in groups:

        groups_info_parameters = {
            'group_id': grp,
            'fields': ",".join(group_data_fields),
            'access_token': access_token, 'v': api_version
        }
        subscribers_count = get_vk_data(api_method="groups.getById", parameters=groups_info_parameters)
        N = subscribers_count[0]['members_count']
        batch_offset_points = range(0, N, 1000) # У api есть ограничения на выгрузки, поэтому загружаем партиями

        fh = open(f_path + grp + '.csv', "w")
        print(";".join(user_data_fields) + ";group_subscribed", file=fh)

        for offset in batch_offset_points:
            if offset != 0 and offset % 100000 == 0: # паузы между сотнями партий
                if verbose:
                    print("Пауза 1 секунда")
                sleep(1)
            if offset != 0 and offset % 300000 == 0: # полезно делать еще и такую паузу
                if verbose:
                    print("Пауза 5 секунд")
                sleep(5)
            group_member_parameters = {
                'group_id': grp,
                'fields': ",".join(user_data_fields),
                'access_token': access_token, 'v': api_version,
                'sort': 'id_asc', 'count': 1000, 'offset': offset
            }
            subscribers_names = get_vk_data(api_method="groups.getMembers", parameters=group_member_parameters)

            for user in subscribers_names['items']:
                if user['first_name'] == 'DELETED': # удаленные аккаунты не нужны
                    continue
                if 'city' not in user:
                    user['city'] = 'NULL' # отсутствующие поля превращаются в строку 'NULL'
                else:
                    user['city'] = user['city']['title']
                if ';' in user['first_name']:
                    user['first_name'] = user['first_name'].replace(';','') # ; используется как разделитель в csv
                if ';' in user['last_name']:
                    user['last_name'] = user['last_name'].replace(';','')
                if ';' in user['city']:
                    user['city'] = user['city'].replace(';','')

                line = ";".join(
                    (   str(user['id']),
                        user['first_name'],
                        user['last_name'],
                        user.get('bdate', 'NULL'),
                        user['city'],
                        group_member_parameters['group_id']
                    ))
                print(line, file=fh)

            if verbose:
                print("Группа {0}, партия {1} обработана".format(grp, offset))
            sleep(0.1) # пауза между партиями

        fh.close()

# todo: забирать список групп из аргументов, если дата_лоадер запущен как самостоятельная программа
if __name__ == "__main__":
   get_subscribers_data_to_csv()
