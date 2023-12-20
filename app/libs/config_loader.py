import os

import tomli

from libs.singleton import MetaSingleton
from configloader import ConfigLoader


config_abs_path = "/".join(os.path.abspath(__file__).split("/")[:-2])
deployment_files = {
    "local": os.path.join(config_abs_path, "config.yml"),
    "test": os.path.join(config_abs_path, "config_test.yml"),
}
SERVICE_NAME = "VKGROUPS"
ENV_CONFIG_PATH = os.getenv(f"{SERVICE_NAME}_CONFIG_PATH")
DEPLOYMENT_MODE = os.getenv(f"DEPLOYMENT_MODE")


class Config(metaclass=MetaSingleton):
    def __init__(self, config_file_path=deployment_files["local"]):
        self.config_loader = ConfigLoader()

        if ENV_CONFIG_PATH:
            print(f"Use config file from env: {ENV_CONFIG_PATH=}")
            self.config_loader.update_from_yaml_file(ENV_CONFIG_PATH)
        elif DEPLOYMENT_MODE:
            config_file = deployment_files.get(DEPLOYMENT_MODE, "local")
            print(f"Use config via {DEPLOYMENT_MODE=} {config_file=}")
            self.config_loader.update_from_yaml_file(config_file)
        else:
            # print(f"Use default config file: {config_file_path=}")
            self.config_loader.update_from_yaml_file(config_file_path)

        self.config_loader.update_from_env_namespace(f"{SERVICE_NAME}")

    def get(self, setting_name):
        return self.config_loader.get(setting_name, None)

    def to_dict(self):
        loader = self.config_loader
        return {key: loader.get(key) for key in loader.keys()}


def get_package_info():
    with open("../pyproject.toml", mode="rb") as fp:
        pyproject = tomli.load(fp)
    return {
        "version": pyproject["tool"]["poetry"]["version"],
        "description": pyproject["tool"]["poetry"]["description"],
        "title": pyproject["tool"]["poetry"]["name"],
    }
