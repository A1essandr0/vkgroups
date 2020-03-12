#!/bin/usr/python

import json
from time import sleep, perf_counter
from os import path, mkdir

import asyncio
import aiofiles
import aiohttp
from aiohttp import ClientSession

from config import groups_list, api_version, user_data_fields, group_data_fields, csvpath, batch
from sconfig import access_token
from data_loader import validate_user_data, get_vk_data

method_url = "https://api.vk.com/method/groups.getMembers"

async def a_request(client, requested_url, group_id, batch_size, offset=0):
    """
    Асинхронный вариант запроса
    """
    parameters = {
        'group_id': group_id,
        'fields': ",".join(user_data_fields),
        'access_token': access_token, 'v': str(api_version),
        'sort': 'id_asc', 'count': batch_size, 'offset': offset
    }
    async with client.request(method='POST', url=requested_url, params=parameters) as resp:
        print('(async) Группа {2}, партия {0}, http resp {1}'.format(offset, resp.status, parameters['group_id']))
        return await resp.json()


async def load(f_path, verbose, batch_size, groups, group_data_fields, user_data_fields, access_token, api_version):
    """
    Загрузка в обозначенный csv-файл
    """
    if not path.exists(f_path):
        mkdir(f_path)

    for grp in groups:
        fh = open(f_path + grp + '.csv', "w")
        print(";".join(user_data_fields) + ";group_subscribed", file=fh)

        groups_info_parameters = {
            'group_id': grp,
            'fields': ",".join(group_data_fields),
            'access_token': access_token, 'v': api_version
        }
        subscribers_count = get_vk_data(api_method="groups.getById", parameters=groups_info_parameters)
        N = subscribers_count[0]['members_count']

        async with aiohttp.ClientSession() as client:
            # Лимит vk api - 3 запроса в секунду
            for n in range(0, N, batch_size*3):
                responses = await asyncio.gather(
                    a_request(client=client, requested_url=method_url, batch_size=batch_size, group_id=grp, offset=n),
                    a_request(client=client, requested_url=method_url, batch_size=batch_size, group_id=grp, offset=n+1000),
                    a_request(client=client, requested_url=method_url, batch_size=batch_size, group_id=grp, offset=n+2000),
                )
                for r in responses:
                    lines = validate_user_data(r['response']['items'], group_id=grp)
                    for line in lines:
                        print(line, file=fh)
                
                await asyncio.sleep(1)

        await asyncio.sleep(1)
        fh.close()


def get_subscribers_data_to_csv():
    start_time = perf_counter()
    asyncio.run(
        load(   # В основной event loop подается load
        f_path=csvpath, verbose=True, batch_size=batch,
        groups=groups_list, group_data_fields=group_data_fields, user_data_fields=user_data_fields,
        access_token=access_token, api_version=api_version
        )
    )
    elapsed = perf_counter() - start_time
    print(f"Группы {groups_list} загружены за {elapsed:0.2f} сек.")

# Если запущено как standalone, то обновляет версию данных в csv файлах
if __name__ == "__main__":
   get_subscribers_data_to_csv()