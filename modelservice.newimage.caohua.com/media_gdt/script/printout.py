# -*- coding:utf-8 -*-
"""
   File Name：     printout.py
   Description :   脚本：新素材新建计划的格式化输出（广点通）
   Author :        royce.mao
   date：          2021/6/16 15:54
"""

import json
import requests


def get_plan_online(plan_result):
    """[上线测试的格式化输出]

    Args:
        plan_result ([dataframe]): [过滤后的新建计划集]]

    Returns:
        [json]: []
    """
    ad_info = []
    for i in range(plan_result.shape[0]):
        ad_info.append(json.loads(plan_result.iloc[i].to_json()))
    
    open_api_url_prefix = "https://ptom.caohua.com/"  ## "http://192.168.0.60:8085/"
    uri = "model/generationPlanBatchTask"
    url = open_api_url_prefix + uri
    params = {
        "secretkey": "abc2018!@**@888",
        "mediaId": 16
    }
    rsp = requests.post(url, json=ad_info, params=params)
    rsp_data = rsp.json()
    print("[INFO]:结束！")
    return rsp_data