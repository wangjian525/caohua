# -*- coding:utf-8 -*-

import os
import yaml
import logging
import json


class Yaml:
    _config = None

    def __new__(cls, *args, **kw):
        if not hasattr(cls, '_instance'):
            cls._instance = object.__new__(cls)
        return cls._instance

    def __init__(self, file_name="config.yml"): 
        config_temp = None
        try:
            cur_path = os.path.dirname(os.path.realpath(__file__))
            yaml_path = os.path.join(cur_path, file_name)

            f = open(yaml_path, 'r', encoding='utf-8')
            config_temp = f.read()
        except Exception as e:
            logging.info("计划配置文件加载失败！", e)
        finally:
            f.close()
        self._config = yaml.safe_load(config_temp)


    def __str__(self):
        return json.dumps(self._config)

    def __del__(self):
        self._config = None
        self = None

def get_config(file_name="../config/confing.yml"):
    return Yaml(file_name)._config

def refresh_config(cls, file_name="../config/confing.yml"):
    del cls
    return Yaml(file_name)._config
