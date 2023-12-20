import os
from typing import Union
import logging
import tarfile
from datetime import datetime

import pandas as pd

from libs.config_loader import Config
from libs.singleton import MetaSingleton


Tablename = Union[str, pd.DataFrame]


class DataProcessor(metaclass=MetaSingleton):
    def __init__(self, config: Config):
        self.config = config
    
    def process(self) -> pd.DataFrame:
        groups_list = list(self.config.get("GROUPS_LIST"))
        csv_path = self.config.get("CSV_PATH")
        groups_filenames = [
            f"{csv_path}{group}.csv"
            for group in groups_list
        ]
        
        current_table = pd.read_csv(csv_path + groups_list.pop() + '.csv', sep=";", header=0, dtype=str)
        while groups_list != []:
            next_group = groups_list.pop()
            current_table = self._merge_tables(current_table, next_group, verbose=True)
            logging.info(f"{next_group} processed")
        
        # tar.gz csv files to backup them and remove csv
        archive_filename = f"groups_csv_{int(datetime.utcnow().timestamp())}.tar.gz" 
        with tarfile.open(archive_filename, "w:gz") as tar:
            tar.add(csv_path)
        logging.info(f"Archive saved: {archive_filename}")
        for file in groups_filenames:
            os.unlink(file)
        
        return current_table
    

    def _merge_tables(self, current_table: Tablename, next_group: Tablename, verbose=True) -> pd.DataFrame:
        csv_path = self.config.get("CSV_PATH")

        if isinstance(current_table, pd.DataFrame):
            table1 = current_table
        else:
            table1 = pd.read_csv(csv_path + current_table + '.csv', sep=";", header=0, dtype=str)
        if isinstance(next_group, pd.DataFrame):
            table2 = next_group
        else:
            table2 = pd.read_csv(csv_path + next_group + '.csv', sep=";", header=0, dtype=str)

        concatenated = pd.concat([table1, table2,], ignore_index=True)

        # Check whether user is subscribed to several groups
        # gs_x != gs_x is a check whether value is NaN
        outer_joined = pd.merge(table1[['id', 'group_subscribed']],
                                table2[['id', 'group_subscribed']],
                                on='id', how='outer')
                                
        outer_joined['groups'] = outer_joined['group_subscribed_x'] + ',' + outer_joined['group_subscribed_y']
        
        outer_joined.loc[
            outer_joined.group_subscribed_x != outer_joined.group_subscribed_x, 'groups'
        ] = outer_joined.group_subscribed_y
        outer_joined.loc[
            outer_joined.group_subscribed_y != outer_joined.group_subscribed_y, 'groups'
        ] = outer_joined.group_subscribed_x

        # Merging and cleaning
        left_joined = pd.merge(concatenated, outer_joined[['id', 'groups']], on='id', how='left')
        left_joined['group_subscribed'] = left_joined['groups']
        L = left_joined.drop_duplicates('id')

        if verbose:
            logging.info(f"{str(current_table)} and {str(next_group)} processed")
        return L[L.columns[0:6]]
