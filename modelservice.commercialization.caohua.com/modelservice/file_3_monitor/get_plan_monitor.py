# -*- coding:utf-8 -*-

import web
import time
import json
import logging
import datetime
import traceback
import pandas as pd
import numpy as np
import warnings
from pandas.core.common import SettingWithCopyWarning
warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)
from modelservice.file_3_monitor.utils.monitor_utils import *

# import sys
# sys.path.append("..") 
import modelservice.yaml_conf as yaml_conf
logger = logging.getLogger('PlanMonitor')


confs = yaml_conf.get_config()  # !!! 一对一计划参数YAML全局配置


class PlanMonitor:
    """ 计划监控 """
    def POST(self, ):
        ret = self.main()
        return ret

    def __init__(self, ):
        pass

    def main(self, ):
        try:
            print("[INFO-MONITORING]:计划监控中...")
            data = web.data()
            json_data = json.loads(data)
            assert set(json_data.keys()) == set(["action","mediaId","mgameId","modelType","data","columns"]) or \
                   set(json_data.keys()) == set(["mediaId","mgameId","modelType","data","columns"]), "请求参数格式不对!"
            # 监控
            data = self.source_predict_state(json_data)
            print("[INFO-MONITORING]:计划监控完毕！")
        except Exception as e:
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logging.error("plan monitor error! at {}".format(timestamp))
            logging.error(e)
            logging.error("\n" + traceback.format_exc())
            return json.dumps({"code": 500, "msg": str(e), "datetime": timestamp})
        
        ret = json.dumps({"code": 200, "msg": "success!", "data": data})
        return ret

    @staticmethod
    def source_predict_state(json_data):
        """ 监控标准 """
        def source_predict_state_1(json_data):
            # TODO:模型1-游戏包、媒体通用
            return model_1(json_data)

        def source_predict_state_2(json_data):
            # TODO:模型2-游戏包、媒体通用
            return model_2(json_data)
        
        switch_map = {
                      1:"source_predict_state_1(json_data)",
                      2:"source_predict_state_2(json_data)"
                      }
        
        data = eval(switch_map[int(json_data['modelType'])])

        return data
        

def model_1(jsondata):
    """ 模型1 """
    mgame_id = int(jsondata['mgameId'])
    media_id = int(jsondata['mediaId'])
    k = '{},{}'.format(mgame_id, media_id)
    data = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])
    # data = data[(data['mgame_id'] == mgame_id) & (data['media_id'] == media_id)]

    data = data[data['source_run_date_amount'] > 0]

    data_not_ignore = data[data['source_run_date_amount'] >= 300]

    data_win_0 = data_not_ignore[data_not_ignore['platform'] == 1]
    data_win_0_ios = data_not_ignore[data_not_ignore['platform'] == 2]

    result_df = pd.DataFrame()

    cost_an, pay_an = get_an_cost(media_id, mgame_id, confs[k])
    cost_ios, pay_ios = get_ios_cost(media_id, mgame_id, confs[k])

    if data_win_0.shape[0] != 0:
        data_win_0['label'] = data_win_0.apply(
            lambda x: 1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount <= 1000) & (x.create_role_cost >= 3 * cost_an)
            else (1 if (x.source_run_date_amount > 1000) & (x.create_role_cost >= 2 * cost_an) & (x.create_role_roi <= 0.5 * confs[k]['roi_an'])
                  else (1 if (x.source_run_date_amount > 2000) & (x.create_role_roi > 0) & (x.create_role_roi <= confs[k]['roi_an'])
                        else (1 if x.create_role_cost >= 5 * cost_an else 0))), axis=1)
        result_df = result_df.append(data_win_0)

    if data_win_0_ios.shape[0] != 0:
        data_win_0_ios['label'] = data_win_0_ios.apply(
            lambda x: 1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount <= 1000) & (x.create_role_cost >= 3.5 * cost_ios)
            else (1 if (x.source_run_date_amount > 1000) & (x.create_role_cost >= 2.5 * cost_an) & (x.create_role_roi <= 0.4 * confs[k]['roi_ios'])
                  else (1 if (x.source_run_date_amount > 2000) & (x.create_role_roi > 0) & (x.create_role_roi <= confs[k]['roi_ios'])
                        else (1 if x.create_role_cost >= 5 * cost_ios else 0))), axis=1)
        result_df = result_df.append(data_win_0_ios)

    if result_df.shape[0] != 0:
        result_df_an = result_df[result_df['platform'] == 1]
        result_df_ios = result_df[result_df['platform'] == 2]
        result_df = pd.DataFrame()

        data_1 = result_df_an[result_df_an['source_run_date_amount'] <= 1.5 * confs[k]['budget_an']]
        data_2 = result_df_an[(result_df_an['source_run_date_amount'] > 1.5 * confs[k]['budget_an']) & (result_df_an['source_run_date_amount'] <= 3.5 * confs[k]['budget_an'])]
        data_3 = result_df_an[(result_df_an['source_run_date_amount'] > 3.5 * confs[k]['budget_an']) & (result_df_an['source_run_date_amount'] <= 6 * confs[k]['budget_an'])]
        data_4 = result_df_an[(result_df_an['source_run_date_amount'] > 6 * confs[k]['budget_an']) & (result_df_an['source_run_date_amount'] <= 12 * confs[k]['budget_an'])]
        data_5 = result_df_an[(result_df_an['source_run_date_amount'] > 12 * confs[k]['budget_an']) & (result_df_an['source_run_date_amount'] <= 26 * confs[k]['budget_an'])]
        data_6 = result_df_an[(result_df_an['source_run_date_amount'] > 26 * confs[k]['budget_an']) & (result_df_an['source_run_date_amount'] <= 38 * confs[k]['budget_an'])]
        data_7 = result_df_an[(result_df_an['source_run_date_amount'] > 38 * confs[k]['budget_an']) & (result_df_an['source_run_date_amount'] <= 53 * confs[k]['budget_an'])]
        data_8 = result_df_an[(result_df_an['source_run_date_amount'] > 53 * confs[k]['budget_an']) & (result_df_an['source_run_date_amount'] <= 68 * confs[k]['budget_an'])]
        data_9 = result_df_an[(result_df_an['source_run_date_amount'] > 68 * confs[k]['budget_an']) & (result_df_an['source_run_date_amount'] <= 83 * confs[k]['budget_an'])]
        data_10 = result_df_an[result_df_an['source_run_date_amount'] > 83 * confs[k]['budget_an']]

        if data_1.shape[0] != 0:
            data_1['budget'] = data_1.apply(
                lambda x: 2 * confs[k]['budget_an'] if (x.create_role_pay_sum >= confs[k]['budget_an'] * confs[k]['roi_an'] * 0.8) & (x.create_role_pay_num >= 2) else
                (2 * confs[k]['budget_an'] if x.create_role_pay_sum >= confs[k]['budget_an'] * confs[k]['roi_an'] else np.nan), axis=1)
            result_df = result_df.append(data_1)
        if data_2.shape[0] != 0:
            data_2['budget'] = data_2.apply(
                lambda x: 4 * confs[k]['budget_an'] if (x.create_role_roi >= 0.8 * confs[k]['roi_an']) & (x.create_role_pay_cost <= 0.8 * pay_an) else np.nan, axis=1)
            result_df = result_df.append(data_2)
        if data_3.shape[0] != 0:
            data_3['budget'] = data_3.apply(
                lambda x: 7 * confs[k]['budget_an'] if (x.create_role_roi >= 0.8 * confs[k]['roi_an']) & (x.create_role_pay_cost <= 0.8 * pay_an) else
                (7 * confs[k]['budget_an'] if (x.create_role_roi >= 1.1 * confs[k]['roi_an']) & (x.create_role_pay_cost <= pay_an) else np.nan), axis=1)
            result_df = result_df.append(data_3)
        if data_4.shape[0] != 0:
            data_4['budget'] = data_4.apply(
                lambda x: 14 * confs[k]['budget_an'] if (x.create_role_roi >= 0.9 * confs[k]['roi_an']) & (x.create_role_pay_cost <= 0.7 * pay_an) else
                (14 * confs[k]['budget_an'] if (x.create_role_roi >= 1.2 * confs[k]['roi_an']) & (x.create_role_pay_cost <= 0.9 * pay_an) else np.nan), axis=1)
            result_df = result_df.append(data_4)
        if data_5.shape[0] != 0:
            data_5['budget'] = data_5.apply(
                lambda x: 28 * confs[k]['budget_an'] if (x.create_role_roi >= confs[k]['roi_an']) & (x.create_role_pay_cost <= 0.6 * pay_an) else
                (28 * confs[k]['budget_an'] if (x.create_role_roi >= 1.3 * confs[k]['roi_an']) & (x.create_role_pay_cost <= 0.8 * pay_an) else
                 np.nan), axis=1)
            result_df = result_df.append(data_5)
        if data_6.shape[0] != 0:
            data_6['budget'] = data_6.apply(
                lambda x: 40 * confs[k]['budget_an'] if (x.create_role_roi >= 1.1 * confs[k]['roi_an']) & (x.create_role_pay_cost <= 0.6 * pay_an) else
                (40 * confs[k]['budget_an'] if (x.create_role_roi >= 1.3 * confs[k]['roi_an']) & (x.create_role_pay_cost <= 0.8 * pay_an) else
                 np.nan), axis=1)
            result_df = result_df.append(data_6)
        if data_7.shape[0] != 0:
            data_7['budget'] = data_7.apply(
                lambda x: 55 * confs[k]['budget_an'] if (x.create_role_roi >= 1.1 * confs[k]['roi_an']) & (x.create_role_pay_cost <= 0.5 * pay_an) else
                (55 * confs[k]['budget_an'] if (x.create_role_roi >= 1.3 * confs[k]['roi_an']) & (x.create_role_pay_cost <= 0.7 * pay_an) else
                 np.nan), axis=1)
            result_df = result_df.append(data_7)
        if data_8.shape[0] != 0:
            data_8['budget'] = data_8.apply(
                lambda x: 70 * confs[k]['budget_an'] if (x.create_role_roi >= 1.1 * confs[k]['roi_an']) & (x.create_role_pay_cost <= 0.5 * pay_an) else
                (70 * confs[k]['budget_an'] if (x.create_role_roi >= 1.3 * confs[k]['roi_an']) & (x.create_role_pay_cost <= 0.7 * pay_an) else
                 np.nan), axis=1)
            result_df = result_df.append(data_8)
        if data_9.shape[0] != 0:
            data_9['budget'] = data_9.apply(
                lambda x: 85 * confs[k]['budget_an'] if (x.create_role_roi >= 1.1 * confs[k]['roi_an']) & (x.create_role_pay_cost <= 0.5 * pay_an) else
                (85 * confs[k]['budget_an'] if (x.create_role_roi >= 1.3 * confs[k]['roi_an']) & (x.create_role_pay_cost <= 0.7 * pay_an) else
                 np.nan), axis=1)
            result_df = result_df.append(data_9)
        if data_10.shape[0] != 0:
            data_10['budget'] = data_10.apply(
                lambda x: 100 * confs[k]['budget_an'] if (x.create_role_roi >= 1.1 * confs[k]['roi_an']) & (x.create_role_pay_cost <= 0.5 * pay_an) else
                (100 * confs[k]['budget_an'] if (x.create_role_roi >= 1.3 * confs[k]['roi_an']) & (x.create_role_pay_cost <= 0.7 * pay_an) else
                 np.nan), axis=1)
            result_df = result_df.append(data_10)

        data_1_ios = result_df_ios[result_df_ios['source_run_date_amount'] <= 1.5 * confs[k]['budget_ios']]
        data_2_ios = result_df_ios[(result_df_ios['source_run_date_amount'] > 1.5 * confs[k]['budget_ios']) & (
                result_df_ios['source_run_date_amount'] <= 3.5 * confs[k]['budget_ios'])]
        data_3_ios = result_df_ios[(result_df_ios['source_run_date_amount'] > 3.5 * confs[k]['budget_ios']) & (
                result_df_ios['source_run_date_amount'] <= 6 * confs[k]['budget_ios'])]
        data_4_ios = result_df_ios[(result_df_ios['source_run_date_amount'] > 6 * confs[k]['budget_ios']) & (
                result_df_ios['source_run_date_amount'] <= 12 * confs[k]['budget_ios'])]
        data_5_ios = result_df_ios[(result_df_ios['source_run_date_amount'] > 12 * confs[k]['budget_ios']) & (
                result_df_ios['source_run_date_amount'] <= 26 * confs[k]['budget_ios'])]
        data_6_ios = result_df_ios[(result_df_ios['source_run_date_amount'] > 26 * confs[k]['budget_ios']) & (
                result_df_ios['source_run_date_amount'] <= 38 * confs[k]['budget_ios'])]
        data_7_ios = result_df_ios[(result_df_ios['source_run_date_amount'] > 38 * confs[k]['budget_ios']) & (
                result_df_ios['source_run_date_amount'] <= 53 * confs[k]['budget_ios'])]
        data_8_ios = result_df_ios[(result_df_ios['source_run_date_amount'] > 53 * confs[k]['budget_ios']) & (
                result_df_ios['source_run_date_amount'] <= 68 * confs[k]['budget_ios'])]
        data_9_ios = result_df_ios[(result_df_ios['source_run_date_amount'] > 68 * confs[k]['budget_ios']) & (
                result_df_ios['source_run_date_amount'] <= 83 * confs[k]['budget_ios'])]
        data_10_ios = result_df_ios[result_df_ios['source_run_date_amount'] > 83 * confs[k]['budget_ios']]

        if data_1_ios.shape[0] != 0:
            data_1_ios['budget'] = data_1_ios.apply(
                lambda x: 2 * confs[k]['budget_ios'] if (x.create_role_pay_sum >= confs[k]['budget_ios'] * confs[k]['roi_ios'] * 0.8) & (x.create_role_pay_num >= 2)
                else (2 * confs[k]['budget_ios'] if x.create_role_pay_sum >= confs[k]['budget_ios'] * confs[k]['roi_ios'] else np.nan), axis=1)
            result_df = result_df.append(data_1_ios)
        if data_2_ios.shape[0] != 0:
            data_2_ios['budget'] = data_2_ios.apply(
                lambda x: 4 * confs[k]['budget_ios'] if (x.create_role_roi >= 0.8 * confs[k]['roi_ios']) & (x.create_role_pay_cost <= 0.8 * pay_ios) else np.nan, axis=1)
            result_df = result_df.append(data_2_ios)
        if data_3_ios.shape[0] != 0:
            data_3_ios['budget'] = data_3_ios.apply(
                lambda x: 7 * confs[k]['budget_ios'] if (x.create_role_roi >= 0.8 * confs[k]['roi_ios']) & (x.create_role_pay_cost <= 0.8 * pay_ios)
                else (7 * confs[k]['budget_ios'] if (x.create_role_roi >= 1.1 * confs[k]['roi_ios']) & (x.create_role_pay_cost <= pay_ios) else np.nan), axis=1)
            result_df = result_df.append(data_3_ios)
        if data_4_ios.shape[0] != 0:
            data_4_ios['budget'] = data_4_ios.apply(
                lambda x: 14 * confs[k]['budget_ios'] if (x.create_role_roi >= 0.9 * confs[k]['roi_ios']) & (x.create_role_pay_cost <= 0.7 * pay_ios)
                else (14 * confs[k]['budget_ios'] if (x.create_role_roi >= 1.2 * confs[k]['roi_ios']) & (x.create_role_pay_cost <= 0.9 * pay_ios) else np.nan), axis=1)
            result_df = result_df.append(data_4_ios)
        if data_5_ios.shape[0] != 0:
            data_5_ios['budget'] = data_5_ios.apply(
                lambda x: 28 * confs[k]['budget_ios'] if (x.create_role_roi >= confs[k]['roi_ios']) & (x.create_role_pay_cost <= 0.6 * pay_ios)
                else (28 * confs[k]['budget_ios'] if (x.create_role_roi >= 1.3 * confs[k]['roi_ios']) & (x.create_role_pay_cost <= 0.8 * pay_ios) else np.nan), axis=1)
            result_df = result_df.append(data_5_ios)
        if data_6_ios.shape[0] != 0:
            data_6_ios['budget'] = data_6_ios.apply(
                lambda x: 40 * confs[k]['budget_ios'] if (x.create_role_roi >= 1.1 * confs[k]['roi_ios']) & (x.create_role_pay_cost <= 0.6 * pay_ios)
                else (40 * confs[k]['budget_ios'] if (x.create_role_roi >= 1.3 * confs[k]['roi_ios']) & (x.create_role_pay_cost <= 0.8 * pay_ios) else np.nan), axis=1)
            result_df = result_df.append(data_6_ios)
        if data_7_ios.shape[0] != 0:
            data_7_ios['budget'] = data_7_ios.apply(
                lambda x: 55 * confs[k]['budget_ios'] if (x.create_role_roi >= 1.1 * confs[k]['roi_ios']) & (x.create_role_pay_cost <= 0.5 * pay_ios)
                else (55 * confs[k]['budget_ios'] if (x.create_role_roi >= 1.3 * confs[k]['roi_ios']) & (x.create_role_pay_cost <= 0.7 * pay_ios) else np.nan), axis=1)
            result_df = result_df.append(data_7_ios)
        if data_8_ios.shape[0] != 0:
            data_8_ios['budget'] = data_8_ios.apply(
                lambda x: 70 * confs[k]['budget_ios'] if (x.create_role_roi >= 1.1 * confs[k]['roi_ios']) & (x.create_role_pay_cost <= 0.5 * pay_ios)
                else (70 * confs[k]['budget_ios'] if (x.create_role_roi >= 1.3 * confs[k]['roi_ios']) & (x.create_role_pay_cost <= 0.7 * pay_ios) else np.nan), axis=1)
            result_df = result_df.append(data_8_ios)
        if data_9_ios.shape[0] != 0:
            data_9_ios['budget'] = data_9_ios.apply(
                lambda x: 85 * confs[k]['budget_ios'] if (x.create_role_roi >= 1.1 * confs[k]['roi_ios']) & (x.create_role_pay_cost <= 0.5 * pay_ios)
                else (85 * confs[k]['budget_ios'] if (x.create_role_roi >= 1.3 * confs[k]['roi_ios']) & (x.create_role_pay_cost <= 0.7 * pay_ios) else np.nan), axis=1)
            result_df = result_df.append(data_9_ios)
        if data_10_ios.shape[0] != 0:
            data_10_ios['budget'] = data_10_ios.apply(
                lambda x: 100 * confs[k]['budget_ios'] if (x.create_role_roi >= 1.1 * confs[k]['roi_ios']) & (x.create_role_pay_cost <= 0.5 * pay_ios)
                else (100 * confs[k]['budget_ios'] if (x.create_role_roi >= 1.3 * confs[k]['roi_ios']) & (x.create_role_pay_cost <= 0.7 * pay_ios) else np.nan), axis=1)
            result_df = result_df.append(data_10_ios)

        result_df['budget'] = result_df['budget'].replace(np.nan, 'None')
        result_df['label'] = result_df['label'].astype(int)
        result_df = result_df.fillna('null')
    else:
        result_df = pd.DataFrame(columns=["channel_id", "source_id", "plan_name", "model_run_datetime", "create_time",
                                          "media_id", "game_id", "platform", "source_run_date_amount",
                                          "create_role_num", "create_role_cost",
                                          "create_role_pay_num", "create_role_pay_cost", "create_role_pay_sum",
                                          "create_role_roi", "create_role_pay_rate", "create_role_pay_num_cum",
                                          "learning_type", "learning_time_dt", "learning_time_hr", "deep_bid_type",
                                          "label", "budget"], data=[])
    return {"columns": result_df.columns.values.tolist(), "list": result_df.values.tolist()}


def model_2(jsondata):
    """ 模型2:纯规则 """
    mgame_id = int(jsondata['mgameId'])
    media_id = int(jsondata['mediaId'])
    k = '{},{}'.format(mgame_id, media_id)
    data = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])
    # data = data[(data['mgame_id'] == mgame_id) & (data['media_id'] == media_id)]

    data = data[data['source_run_date_amount'] > 0]
    data.sort_values(by='data_win', inplace=True)
    data.drop_duplicates(subset=['channel_id', 'source_id', 'source_run_date_amount', 'create_role_num'], keep='first'
                         , inplace=True)

    cost_an, pay_an = get_an_cost(media_id, mgame_id, confs[k])
    cost_ios, pay_ios = get_ios_cost(media_id, mgame_id, confs[k])

    data_0 = data[(data['cum_amount'] >= 100) & (data['cum_amount'] <= confs[k]['budget_an']) & (data['cum_day'] <= 7) & (
            data['cum_role_cost'] <= 3 * cost_an) & (data['platform'] == 1)]
    data_1 = data[(data['cum_amount'] >= 100) & (data['cum_amount'] <= confs[k]['budget_ios']) & (data['cum_day'] <= 7) & (
            data['cum_role_cost'] <= 3 * cost_ios) & (data['platform'] == 2)]
    data_0 = data_0.append(data_1)
    data_0['label'] = data_0['cum_amount'].apply(lambda x: 0)

    data = data.drop(data_0.index).reset_index(drop=True)
    data_not_ignore = data[(data['source_run_date_amount'] >= 200) | (data['data_win'] == 1)]
    data_ignore = data.drop(data_not_ignore.index)

    data_2 = data_not_ignore[
        ((data_not_ignore['data_win'] <= 2) & (data_not_ignore['source_run_date_amount'] >= 2 * confs[k]['budget_an']) & (data_not_ignore['create_role_roi'] == 0) & (data_not_ignore['platform'] == 1))|
        ((data_not_ignore['data_win'] <= 2) & (data_not_ignore['source_run_date_amount'] >= 2 * confs[k]['budget_ios']) & (data_not_ignore['create_role_roi'] == 0) & (data_not_ignore['platform'] == 2))]
    data_not_ignore = data_not_ignore.drop(data_2.index)

    data_win_1 = data_not_ignore[(data_not_ignore['data_win'] == 1) & (data_not_ignore['platform'] == 1)]
    data_win_2 = data_not_ignore[(data_not_ignore['data_win'] == 2) & (data_not_ignore['platform'] == 1)]
    data_win_3 = data_not_ignore[(data_not_ignore['data_win'] == 3) & (data_not_ignore['platform'] == 1)]
    data_win_1_ios = data_not_ignore[(data_not_ignore['data_win'] == 1) & (data_not_ignore['platform'] == 2)]
    data_win_2_ios = data_not_ignore[(data_not_ignore['data_win'] == 2) & (data_not_ignore['platform'] == 2)]
    data_win_3_ios = data_not_ignore[(data_not_ignore['data_win'] == 3) & (data_not_ignore['platform'] == 2)]

    result_win1 = pd.DataFrame()

    if data_win_1.shape[0] != 0:
        data_win_1['label'] = data_win_1.apply(lambda x: 1 if x.create_role_num == 0
        else (1 if (x.create_role_pay_cost > 1.7 * pay_an) | (x.create_role_cost > 4 * cost_an)
              else (1 if x.create_role_roi < 0.8 * confs[k]['roi_an']
                    else (0 if x.create_role_roi >= 0.8 * confs[k]['roi_an'] else 1))), axis=1)

        result_win1 = result_win1.append(data_win_1)

    if data_win_1_ios.shape[0] != 0:
        data_win_1_ios['label'] = data_win_1_ios.apply(lambda x: 1 if x.create_role_num == 0
        else (1 if (x.create_role_pay_cost > 1.7 * pay_ios) | (x.create_role_cost > 3 * cost_ios)
              else (1 if x.create_role_roi <= 0.7 * confs[k]['roi_ios']
                    else (0 if x.create_role_roi >= 0.7 * confs[k]['roi_ios'] else 1))), axis=1)

        result_win1 = result_win1.append(data_win_1_ios)

    result_win2 = pd.DataFrame()
    if data_win_2.shape[0] != 0:
        data_win_2['label'] = data_win_2.apply(lambda x: 1 if x.create_role_num == 0
        else (1 if (x.create_role_pay_cost > 1.7 * pay_an) | (x.create_role_cost > 4 * cost_an)
              else (3 if (x.create_role_roi == 0) & (x.source_run_date_amount >= 2 * confs[k]['budget_an'])
                    else (1 if x.create_role_roi < 1.2 * confs[k]['roi_an']
                          else (0 if x.create_role_roi >= 1.2 * confs[k]['roi_an'] else 1)))), axis=1)
        result_win2 = result_win2.append(data_win_2)

    if data_win_2_ios.shape[0] != 0:
        data_win_2_ios['label'] = data_win_2_ios.apply(lambda x: 1 if x.create_role_num == 0
        else (1 if (x.create_role_pay_cost > 1.5 * pay_ios) | (x.create_role_cost > 3 * cost_ios)
              else (3 if (x.create_role_roi == 0) & (x.source_run_date_amount >= 2 * confs[k]['budget_ios'])
                    else (1 if x.create_role_roi < 1.2 * confs[k]['roi_ios']
                          else (0 if x.create_role_roi >= 1.2 * confs[k]['roi_ios'] else 1)))), axis=1)
        result_win2 = result_win2.append(data_win_2_ios)

    result_win3 = pd.DataFrame()
    if data_win_3.shape[0] != 0:
        data_win_3['label'] = data_win_3.apply(lambda x: 1 if x.create_role_num == 0
        else (1 if (x.create_role_pay_cost > 1.5 * pay_an) | (x.create_role_cost > 3 * cost_an)
                    else (0 if x.create_role_roi >= 0.8 * confs[k]['roi_an_3']
                          else (1 if x.create_role_roi < 0.8 * confs[k]['roi_an_3'] else 1))), axis=1)

        result_win3 = result_win3.append(data_win_3)

    if data_win_3_ios.shape[0] != 0:
        data_win_3_ios['label'] = data_win_3_ios.apply(lambda x: 1 if x.create_role_num == 0
        else (1 if (x.create_role_pay_cost > 1.5 * pay_ios) | (x.create_role_cost > 2.5 * cost_ios)
                    else (0 if x.create_role_roi >= 0.7 * confs[k]['roi_ios_3']
                          else (1 if x.create_role_roi <= 0.7 * confs[k]['roi_ios_3'] else 2))), axis=1)

        result_win3 = result_win3.append(data_win_3_ios)

    result = pd.concat([result_win1, result_win2, result_win3], axis=0)
    if result.shape[0] != 0:
        result.sort_values('data_win', ascending=True, inplace=True)

    if result.shape[0] != 0:
        if (result[result['data_win'] == 3].shape[0] != 0) & (result[result['data_win'] == 2].shape[0] != 0) & (
                result[result['data_win'] == 1].shape[0] != 0):
            result_1_2_3 = result[(result['data_win'] == 1) | (result['data_win'] == 2) | (result['data_win'] == 3)]
            result_1_2_3_label = result_1_2_3[['channel_id', 'source_id', 'model_run_datetime', 'data_win', 'label']]
            result_1_2_3_label['data_win'] = result_1_2_3_label['data_win'].astype(int)
            result_1_2_3_piv = pd.pivot_table(result_1_2_3_label,
                                              index=['channel_id', 'source_id', 'model_run_datetime'],
                                              columns='data_win')
            result_1_2_3_piv.columns = result_1_2_3_piv.columns.droplevel()
            result_1_2_3_piv = result_1_2_3_piv.rename(columns={1: 'label_1', 2: 'label_2', 3: 'label_3'})

            result_1_2_3_piv = result_1_2_3_piv.reset_index()

            result_1_2_3_piv['label'] = result_1_2_3_piv.apply(lambda x: 0 if x.label_1 == 0
                                                        else (0 if x.label_2 == 0
                                                              else (1 if x.label_2 == 3
                                                                    else (0 if x.label_3 == 0 else 1))), axis=1)
            result_1_2_3 = result_1_2_3.drop('label', axis=1)
            result_1_2_3 = result_1_2_3.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='last')

            result_1_2_3_piv.drop(['label_1', 'label_2', 'label_3'], axis=1, inplace=True)
            source_predict = pd.merge(result_1_2_3, result_1_2_3_piv,
                                      on=['channel_id', 'source_id', 'model_run_datetime'],
                                      how='left')

        elif (result[result['data_win'] == 2].shape[0] != 0) & (result[result['data_win'] == 1].shape[0] != 0):
            result_1_2 = result[(result['data_win'] == 1) | (result['data_win'] == 2)]
            result_1_2_label = result_1_2[['channel_id', 'source_id', 'model_run_datetime', 'data_win', 'label']]
            result_1_2_label['data_win'] = result_1_2_label['data_win'].astype(int)
            result_1_2_piv = pd.pivot_table(result_1_2_label, index=['channel_id', 'source_id', 'model_run_datetime'],
                                            columns='data_win')
            result_1_2_piv.columns = result_1_2_piv.columns.droplevel()
            result_1_2_piv = result_1_2_piv.rename(columns={1: 'label_1', 2: 'label_2'})

            result_1_2_piv = result_1_2_piv.reset_index()
            result_1_2_piv['label'] = result_1_2_piv.apply(lambda x: 0 if x.label_1 == 0
                                                                else (0 if x.label_2 == 0 else 1), axis=1)
            result_1_2 = result_1_2.drop('label', axis=1)
            result_1_2 = result_1_2.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='last')

            result_1_2_piv.drop(['label_1', 'label_2'], axis=1, inplace=True)
            source_predict = pd.merge(result_1_2, result_1_2_piv, on=['channel_id', 'source_id', 'model_run_datetime'],
                                      how='left')
        else:
            source_predict = result

    else:
        source_predict = pd.DataFrame(
            columns=["channel_id", "source_id", "plan_name", "model_run_datetime", "create_time",
                     "media_id", "game_id", "platform", "data_win", "source_run_date_amount", "create_role_num",
                     "create_role_cost", "create_role_pay_num", "create_role_pay_cost", "create_role_pay_sum",
                     "create_role_roi",
                     "create_role_retain_1d", "create_role_pay_rate", "create_role_pay_num_cum", "learning_type",
                     "learning_time_dt",
                     "learning_time_hr", "deep_bid_type", "cum_amount", "cum_day", "cum_role_cost", "label"])

    if data_ignore.shape[0] != 0:
        data_ignore['label'] = 1
        data_ignore.sort_values('data_win', ascending=False, inplace=True)
        data_ignore = data_ignore.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
        data_ignore['data_win'] = 9 

    if data_2.shape[0] != 0:
        data_2['label'] = 1
        data_2.sort_values('data_win', ascending=False, inplace=True)
        data_2 = data_2.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
        data_2['data_win'] = -1

    if data_0.shape[0] != 0:
        data_0.sort_values('data_win', ascending=False, inplace=True)
        data_0 = data_0.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
        data_0['data_win'] = -2

    source_predict = pd.concat([source_predict, data_0], axis=0)
    source_predict = pd.concat([source_predict, data_2], axis=0)
    source_predict = pd.concat([source_predict, data_ignore], axis=0)

    source_predict.sort_values('data_win', ascending=True, inplace=True)
    source_predict = source_predict.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
    source_predict['label'] = source_predict['label'].astype(int)
    source_predict = source_predict.fillna('null')

    return {"columns": source_predict.columns.values.tolist(), "list": source_predict.values.tolist()}


if __name__ == '__main__':
    # 测试
    monitor_plan = PlanMonitor()  # !!! 输入形式3
    data = monitor_plan.main()
    print(data)
