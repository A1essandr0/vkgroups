import logging
import json
from time import perf_counter
from os import path, mkdir

import aiohttp
import asyncio
import requests

from libs.singleton import MetaSingleton
from libs.config_loader import Config


class VkDataSource(metaclass=MetaSingleton):
    def __init__(self, config: Config):
        self.name = "VK"
        self.config = config
        self.client_session = None


    def should_get_data(self) -> bool:
        return not self.config.get("CSV_ALREADY_LOADED")


    async def get_data_from_source(self):
        if self.client_session is None:
            self.client_session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(
                    # default params
                    keepalive_timeout=30,
                    limit=100,
                    limit_per_host=0,
                )
            )

        groups_list = self.config.get("GROUPS_LIST")
        user_data_fields = self.config.get("USER_DATA_FIELDS")
        group_data_fields = self.config.get("GROUP_DATA_FIELDS")
        csv_path = self.config.get("CSV_PATH")
        batch_size = self.config.get("BATCH_SIZE")
        max_simultaneous_requests = self.config.get("MAX_SIMULTANEOUS_REQUESTS")
        vk_request_timeout = self.config.get("VK_REQUEST_TIMEOUT")
        vk_api_version = self.config.get("VK_API_VERSION")

        start_time = perf_counter()
        if not path.exists(csv_path):
            mkdir(csv_path)

        for grp in groups_list:
            fh = open(csv_path + grp + '.csv', "w")
            print(";".join(user_data_fields) + ";group_subscribed", file=fh)

            groups_info_parameters = {
                'group_id': grp,
                'fields': ",".join(group_data_fields),
                'access_token': self.config.get("VK_ACCESS_TOKEN"), 'v': str(vk_api_version)
            }
            subscribers_count = self._api_request_sync(api_method="groups.getById", parameters=groups_info_parameters)
            N = subscribers_count[0]['members_count']

            for n in range(0, N, batch_size * max_simultaneous_requests):
                requests = [
                    self._api_request_fixed_params(
                        group_id=grp, 
                        offset=n+1000*i,
                        user_data_fields=user_data_fields,
                        batch_size=batch_size,
                        vk_api_version=vk_api_version,
                        verbose=True,
                    )
                    for i in range(max_simultaneous_requests)
                ]
                responses = await asyncio.gather(*requests)

                for r in responses:
                    if 'items' not in r:
                        logging.error("No 'items' field in server response")
                        continue
                    lines = self._validate_user_data(r['items'], group_id=grp)
                    for line in lines:
                        print(line, file=fh)
                
                await asyncio.sleep(vk_request_timeout)

            await asyncio.sleep(vk_request_timeout/2)
            fh.close()


        elapsed = perf_counter() - start_time
        logging.info(f"Groups {groups_list} loaded in {elapsed:0.2f} secs")
        await self.close()


    async def close(self):
        if self.client_session:
            await self.client_session.close()
            self.client_session = None


    async def _api_request_fixed_params(
        self, 
        group_id: str, 
        offset: int,
        user_data_fields: list[str],
        batch_size: int,
        vk_api_version: float,
        verbose=True
    ):
        parameters = {
            'group_id': group_id,
            'fields': ",".join(user_data_fields),
            'access_token': self.config.get("VK_ACCESS_TOKEN"), 
            'v': str(vk_api_version),
            'sort': 'id_asc', 
            'count': batch_size, 
            'offset': offset
        }
        result = await self._api_request(api_method='groups.getMembers', parameters=parameters)
        if verbose:
            logging.info(f"Group {group_id}, batch {offset}")
        return result

    async def _api_request(self, api_method: str, parameters, method_url="https://api.vk.com/method/") -> list[dict]:
        """ Wrapper for https://vk.com/dev/methods """
        request = await self.client_session.post(url=method_url+api_method, params=parameters)
        data = await request.json()
        return data['response']

    def _api_request_sync(self, api_method: str, parameters, method_url="https://api.vk.com/method/") -> list[dict]:
        """ Sync wrapper for https://vk.com/dev/methods """
        req = requests.post(method_url + api_method, data=parameters)
        return json.loads(req.text)['response']


    def _validate_user_data(self, user_data: list[dict], group_id, fields_to_validate=('first_name', 'last_name', 'city')):
        for user in user_data:
            if ('first_name' not in user or 'last_name' not in user):
                continue

            if user['first_name'] == 'DELETED' or user['last_name'] == 'DELETED': # no need for deleted accounts
                continue
            if 'city' not in user:
                user['city'] = 'NULL' # missing fields turn to 'NULL'
            else:
                user['city'] = user['city']['title']

            for field in fields_to_validate:
                if ';' in user[field]:
                    user[field] = user[field].replace(';','') # ; used as csv delimiter

            line = ";".join(
                (   str(user['id']),
                    user['first_name'],
                    user['last_name'],
                    user.get('bdate', 'NULL'),
                    user['city'],
                    group_id
                ))
            yield line


