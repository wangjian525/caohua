import pandas as pd
import numpy as np
import json
from sklearn.model_selection import train_test_split
from sklearn.decomposition import PCA
from sklearn.preprocessing import LabelEncoder, MinMaxScaler
import gc
import warnings
import requests

warnings.filterwarnings('ignore')
import lightgbm as lgb
from impala.dbapi import connect
from impala.util import as_pandas
import pymysql
import logging

warnings.filterwarnings('ignore')
logger = logging.getLogger('CreatePlan')


# 打包接口
#
def get_ad_create(plan_result):
    ad_info = []
    for i in range(plan_result.shape[0]):
        ad_info.append(json.loads(plan_result.iloc[i].to_json()))
    # print(ad_info)
    open_api_url_prefix = "https://ptom.caohua.com/"
    uri = "model/generationPlanBatchTask"
    url = open_api_url_prefix + uri
    params = {
        "secretkey": "abc2018!@**@888"
    }
    rsp = requests.post(url, json=ad_info, params=params)
    rsp_data = rsp.json()
    print('结束....')
    return rsp_data


def main_model():
    plan_result = pd.read_csv('./plan_result.csv')  # 保存创建日志
    # print(plan_result.head())
    plan_result = plan_result.sample(2)
    # print(plan_result.shape)
    rsp_data = get_ad_create(plan_result)
    print(rsp_data)


if __name__ == '__main__':
    main_model()
