# -- coding: utf-8 --

import web
import time
import logging
import json
import pandas as pd
import numpy as np
import joblib
import datetime

# from utils.utils import plan_jp_gdt
# from utils.utils import get_jp

logger = logging.getLogger('PtomPredict')
model_path = "./aimodel/"
lgb_r_1d = joblib.load(model_path + 'lgb_r_1d.pkl')
lgb_r_2d = joblib.load(model_path + 'lgb_r_2d.pkl')
lgb_r_3d = joblib.load(model_path + 'lgb_r_3d.pkl')
lgb_r_4d = joblib.load(model_path + 'lgb_r_4d.pkl')
lgb_r_5d = joblib.load(model_path + 'lgb_r_5d.pkl')
lgb_r_6d = joblib.load(model_path + 'lgb_r_6d.pkl')
lgb_r_7d = joblib.load(model_path + 'lgb_r_7d.pkl')
source_lgb_b = joblib.load(model_path + 'source_lgb_b.pkl')
source_lgb_b_cv_0 = joblib.load(model_path + 'lgb_b_cv_0.pkl')
source_lgb_b_cv = joblib.load(model_path + 'lgb_b_cv.pkl')
gbdt = joblib.load(model_path + 'gbdt.pkl')
model_for_tank = joblib.load(model_path + 'model_for_tank.pkl')
gbdt_win0 = joblib.load(model_path + 'gbdt_win0.pkl')
gbdt_b_win1 = joblib.load(model_path + 'gbdt_b_win1.pkl')
gbdt_b_win2 = joblib.load(model_path + 'gbdt_b_win2.pkl')
gbdt_b_win3 = joblib.load(model_path + 'gbdt_b_win3.pkl')


#
# 投放系统预测模型
#
class PtomPredict:
    def __init__(self, ):
        self.start_time = datetime.date.today()  ## 服务启动时间
        # self.df = self.GetData()  ## 提取一次数据

    def GET(self):
        # 处理POST请求
        logging.info("do PtomPredict service")
        timestamp = "%d" % int(round(time.time() * 1000))
        return json.dumps({"code": 500, "msg": "Not allow GET request!", "timestamp": timestamp, "data": ""})

    def POST(self):
        # 处理POST请求
        logging.info("do PtomPredict post service")
        # req_time = datetime.date.today()  ## 服务请求时间
        # self.df = self.GetData() if (req_time - self.start_time).days >= 7 else self.df  ## 距离上次提取数据时间，超过7天，则重新提取数据
        # self.start_time = req_time if (req_time - self.start_time).days >= 7 else self.start_time  ## 距离上次提取数据时间，超过7天，则重置当前计算时间
        try:
            ret = self.Process()
            logging.info(ret)
            return ret
        except Exception as e:
            logging.error(e)
            timestamp = "%d" % int(round(time.time() * 1000))
            ret = json.dumps({"code": 500, "msg": str(e), "timestamp": timestamp})
            return ret

    """
    任务处理函数
    数据格式：
    {"action":"role_info","columns":["user_id","cp_server_no","mgame_id","cp_role_id","role_id","create_role_time","create_role_date","channel_id","source_id","game_id",
    "media_id","role_data_type","role_created_login_num","role_created_active","role_created_online","max_role_level","ip_num","pay_num","pay_sum","active_0-8","active_8-12",
    "active_12-14","active_14-18","active_18-24","pay_grade_1","pay_grade_2","pay_grade_3","pay_grade_4","pay_grade_5","pay_grade_6","pay_rate","pay_avg"],
    "data":[[120234147,"S1164",1056,"19764109",14274099,"2020-08-18 10:04:43","2020/8/18",20638,331512,1000993,16,0,7,1,0,8,1,0,0.00,0,3,0,0,0,0,0,0,0,0,0,0.00,0.00],
    [120234151,"661164",1056,"19764088",156437737,"2020-08-18 10:02:39","2020/8/18",20646,342975,1000960,6,0,1,1,0,1,1,0,0.00,0,1,0,0,0,0,0,0,0,0,0,0.00,0.00]]
    }
    -----------------------------------------------

     { "action": "source_predict",
      "columns": ["channel_id","source_id","data_win","source_run_datetime","create_time","media_id","game_id","platform","source_run_date_amount","create_role_num","create_role_cost","create_role_pay_num","create_role_pay_cost",
      "create_role_pay_sum","create_role_roi","create_role_retain_1d"],
      "data":[[1,1,1,"2020-03-30","2020-03-30",10,80024,1,500,5,100,2,250,20,0.25,0.2],
      [2,3,4,"2020-03-31","2020-03-30",10,80023,2,1000,10,200,4,500,40,0.5,0.2]
      ]
    }
    
    { "action": "source_info_1",
      "columns": ["channel_id","source_id","media_id","game_id","source_run_date","source_run_date_amount","type_0_num","type_1_num","type_2_num","source_30_roi_predict"],
      "data":[[1,1,1,1,"2020-03-30",1000,0.5,0.3,0.2,0.3],
            [2,3,1,"2020-03-30",1000,0.4,0.3,0.3,0.3]
      ]
    }
    
    ----------------------------------------------
    { "action": "source_predict",
      "columns": ["channel_id","source_id","data_win","source_run_datetime","create_time","media_id","game_id","platform","source_run_date_amount","create_role_num","create_role_cost","create_role_pay_num","create_role_pay_cost",
      "create_role_pay_sum","create_role_roi","create_role_retain_1d"],
      "data":[[1,1,1,"2020-03-30","2020-03-30",10,80024,1,500,5,100,2,250,20,0.25,0.2],
      [2,3,4,"2020-03-31","2020-03-30",10,80023,2,1000,10,200,4,500,40,0.5,0.2]
      ]
    }
    ----------------------------------------------
    { "action": "source_predict_for_tank",
      "columns": ["channel_id","source_id","data_win","source_run_datetime","create_time","media_id","game_id","platform","source_run_date_amount","create_role_num","create_role_cost","create_role_pay_num","create_role_pay_cost",
      "create_role_pay_sum","create_role_roi","create_role_retain_1d"],
      "data":[[1,1,1,"2020-03-30","2020-03-30",10,80024,1,500,5,100,2,250,20,0.25,0.2],
      [2,3,4,"2020-03-31","2020-03-30",10,80023,2,1000,10,200,4,500,40,0.5,0.2]
      ]
    }
    ----------------------------------------------
    { "action": "source_predict_state_1",
      "columns": ["channel_id","source_id","plan_name", "model_run_datetime","create_time","media_id",
    "game_id","platform","source_run_date_amount","create_role_num","create_role_cost","create_role_pay_num",
    "create_role_pay_cost","create_role_pay_sum","create_role_roi","create_role_pay_rate"],
      "data":[[1,1,"末日","2020-03-30","2020-03-30",10,80024,1,400,5,90,1,3500,32,0.02,0.2],
      [1,1,"末日_2","2020-03-30","2020-03-30",10,80023,2,2000,10,300,0,0,0,0,0]]
      }
    ----------------------------------------------
    { "action": "source_predict_state_2",
      "columns": ["channel_id","source_id","plan_name","model_run_datetime","create_time",
    "media_id","game_id","platform","data_win","source_run_date_amount","create_role_num",
    "create_role_cost","create_role_pay_num","create_role_pay_cost","create_role_pay_sum","create_role_roi",
    "create_role_retain_1d","create_role_pay_rate"],
      "data":[[1,1,"末日","2020-03-30","2020-03-30",10,80024,1,3,400,5,90,1,3500,32,0.1,0.02,0.2],
      [1,2,"末日_2","2020-03-30","2020-03-30",10,80023,2,1,2000,10,300,0,0,0,0,0.02,0]]
      }
    ----------------------------------------------
    { "action": "reload_model"
    }
    
    
        { "action": "source_predict_state_2",
      "columns": ["channel_id","source_id","plan_name","model_run_datetime","create_time",
    "media_id","game_id","platform","data_win","source_run_date_amount","create_role_num",
    "create_role_cost","create_role_pay_num","create_role_pay_cost","create_role_pay_sum","create_role_roi",
    "create_role_retain_1d","create_role_pay_rate","create_role_pay_num_cum","learning_type","learning_time_dt","learning_time_hr"],
      "data":[[1,1,"末日m","2020-03-30","2020-03-30",10,80024,1,3,5000,5,62,3,3600,32,0.03,0.02,0.2,23,1,"2020-12-22","15"],
      [1,2,"ddd","2020-03-30","2020-03-30",10,80023,2,1,2000,10,300,0,0,0,0,0.02,16,0,0,,]]
      }
    """

    def Process(self):
        data = web.data()
        logging.info("PtomPredict post==> %s" % data)
        json_data = json.loads(data)
        action = json_data['action']
        data = {}
        if "role_info" == action:
            data = do_role_info(json_data)
        elif "source_info_1" == action:
            data = do_source_info_1(json_data)
        elif "source_predict" == action:
            data = source_predict(json_data)
        elif "reload_model" == action:
            data = reload_model()
        elif "source_predict_for_tank" == action:
            data = source_predict_for_tank(json_data)
        elif "source_predict_state_1" == action:
            data = source_predict_state_1(json_data)
        elif "source_predict_state_2" == action:
            data = source_predict_state_2(json_data)
        elif "dg_source_predict_state_1" == action:
            data = dg_source_predict_state_1(json_data)
        elif "dg_source_predict_state_2" == action:
            data = dg_source_predict_state_2(json_data)
        elif "jp_source_predict_state_2" == action:  ## TODO：金牌模型2（末日广点通可用）
            data = jp_source_predict_state_2(json_data)  ## TODO
        elif "source_predict_state_1_gdt" == action:
            data = source_predict_state_1_gdt(json_data)
        elif "source_predict_state_1_dg_gdt" == action:
            data = source_predict_state_1_dg_gdt(json_data)
        elif "source_predict_state_1_jp_gdt" == action:  ## TODO：金牌模型1（广点通单独使用）
            data = source_predict_state_1_jp_gdt(json_data)  ## TODO
        else:
            data = {}
        ret = json.dumps({"code": 200, "msg": "success!", "timestamp": "", "data": data})
        return ret


"""
输出模型1~7结果
"""


def do_role_info(jsondata):
    src_role_info = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])
    # 根据角色创建时间类型分组
    role_info_predict = None
    for role_data_type in [0, 1, 2, 3, 4, 5, 6, 7]:
        role_info = src_role_info[src_role_info['role_data_type'] == role_data_type]
        if role_info.size == 0:
            continue
        # 特征选择
        select_features = ["user_id", "cp_server_no", "mgame_id", "cp_role_id", "role_id", "create_role_time",
                           "create_role_date", "channel_id", "source_id", "game_id", "media_id", "role_data_type",
                           "role_created_login_num", "role_created_active", "role_created_online", "max_role_level",
                           "ip_num", "pay_num", "pay_sum", "active_0-8", "active_8-12", "active_12-14", "active_14-18",
                           "active_18-24", "pay_grade_1", "pay_grade_2", "pay_grade_3", "pay_grade_4",
                           "pay_grade_5", "pay_grade_6", "pay_rate", "pay_avg"]
        role_info = role_info[select_features]
        # 根据角色行为分类
        role_info_pay = role_info[role_info['pay_sum'] != 0]
        role_info_nopay = role_info[role_info['pay_sum'] == 0]
        role_info_nopay_online_n = role_info_nopay[(role_info_nopay['role_created_online'] <= 600) &
                                                   (role_info_nopay['role_created_login_num'] <= 2) & (
                                                           role_info_nopay['max_role_level'] <= 10)]
        role_info_nopay_online_y = role_info_nopay[~((role_info_nopay['role_created_online'] <= 600) &
                                                     (role_info_nopay['role_created_login_num'] <= 2) & (
                                                             role_info_nopay['max_role_level'] <= 10))]

        # 标记角户质量不付费不在线0;不付费在线1;付费2
        role_info_nopay_online_n['role_type'] = 0
        role_info_nopay_online_y['role_type'] = 1
        role_info_pay['role_type'] = 2

        # 付费金额预测
        role_info_nopay_online_n['predict_30_pay'] = 0
        role_info_nopay_online_y['predict_30_pay'] = 0
        drop_features = ['user_id', 'cp_server_no', 'mgame_id', 'cp_role_id', 'role_id', 'create_role_time',
                         'create_role_date',
                         'role_type', "channel_id", "source_id", "game_id", "media_id", "role_data_type"]
        if role_info_pay.size != 0:
            if role_data_type == 0 or role_data_type == 1:
                role_info_pay['predict_30_pay'] = np.expm1(
                    lgb_r_1d.predict(role_info_pay.drop(drop_features, axis=1))) * 2.1
            elif role_data_type == 2:
                role_info_pay['predict_30_pay'] = np.expm1(
                    lgb_r_2d.predict(role_info_pay.drop(drop_features, axis=1))) * 1.64
            elif role_data_type == 3:
                role_info_pay['predict_30_pay'] = np.expm1(
                    lgb_r_3d.predict(role_info_pay.drop(drop_features, axis=1))) * 1.54
            elif role_data_type == 4:
                role_info_pay['predict_30_pay'] = np.expm1(
                    lgb_r_4d.predict(role_info_pay.drop(drop_features, axis=1))) * 1.39
            elif role_data_type == 5:
                role_info_pay['predict_30_pay'] = np.expm1(
                    lgb_r_5d.predict(role_info_pay.drop(drop_features, axis=1))) * 1.32
            elif role_data_type == 6:
                role_info_pay['predict_30_pay'] = np.expm1(
                    lgb_r_6d.predict(role_info_pay.drop(drop_features, axis=1))) * 1.29
            elif role_data_type == 7:
                role_info_pay['predict_30_pay'] = np.expm1(
                    lgb_r_7d.predict(role_info_pay.drop(drop_features, axis=1))) * 1.255
        # 数据输出
        if role_info_predict is None:
            role_info_predict = pd.concat([role_info_pay, role_info_nopay_online_n, role_info_nopay_online_y], axis=0)
        else:
            role_info_predict = pd.concat([role_info_predict, role_info_pay, role_info_nopay_online_n,
                                           role_info_nopay_online_y], axis=0)
        role_info_predict = role_info_predict[
            ['user_id', 'cp_server_no', 'mgame_id', 'cp_role_id', 'create_role_time', 'create_role_date',
             'role_type', "channel_id", "source_id", "game_id", "media_id", "role_data_type", 'predict_30_pay']]
    return {"columns": role_info_predict.columns.values.tolist(), "list": role_info_predict.values.tolist()}


"""
输出模型source_info_1结果
"""


def do_source_info_1(jsondata):
    '''
    :param jsondata: {columns : 'channel_id',"media_id","game_id",'source_id','source_run_date','source_run_date_amount','type_0_rate',
                      'type_1_rate','type_2_rate','source_30_roi_predict'}
    :return:
    '''
    source_info_1 = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])
    source_info_1 = source_info_1[source_info_1['source_run_date_amount'] >= 500]
    # 实时模型预测：差计划source_type=0；观望source_type=1
    drop_features = ['channel_id', 'source_id', 'media_id', 'game_id', 'source_run_date', 'source_run_date_amount']
    source_info_1['source_type'] = source_lgb_b.predict(source_info_1.drop(drop_features, axis=1))
    source_info_1['source_type'] = source_info_1['source_type'].apply(lambda x: 0 if x < 0.3 else 1)
    # 数据输出
    source_info_1 = source_info_1[['channel_id', 'source_id', 'media_id', 'game_id', 'source_run_date',
                                   'source_run_date_amount', 'source_30_roi_predict', 'source_type']]

    return {"columns": source_info_1.columns.values.tolist(), "list": source_info_1.values.tolist()}


# def source_predict(jsondata):
#     '''
#     :param jsondata: {"columns": ["channel_id","source_id","data_win","source_run_datetime","create_time","media_id","game_id","platform","source_run_date_amount","create_role_num",
#     "create_role_cost","create_role_pay_num","create_role_pay_cost","create_role_pay_sum","create_role_roi","create_role_retain_1d"]}
#     :return:
#     '''
#     src_source_info = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])
#     # 根据角色创建时间类型分组
#     columns = ["channel_id","source_id","data_win","source_run_datetime","create_time","media_id","game_id","platform","source_run_date_amount","create_role_num","create_role_cost",
#         "create_role_pay_num","create_role_pay_cost","create_role_pay_sum","create_role_roi","create_role_retain_1d","label"]
#     source_predict = pd.DataFrame(columns=columns)
#
#     src_source_info_0 = src_source_info[src_source_info['data_win'] == 0]
#     src_source_info_1_7 = src_source_info[src_source_info['data_win'] != 0]
#     drop_features_0 = ["channel_id","source_id","source_run_datetime","create_time","media_id","game_id","create_role_pay_num","create_role_pay_sum","create_role_retain_1d"]
#     drop_features_1_7 = ["channel_id", "source_id", "source_run_datetime", "create_time", "media_id", "game_id","create_role_pay_num", "create_role_pay_sum"]
#     if src_source_info_0.shape[0] != 0:
#         src_source_info_0['label'] = source_lgb_b_cv_0.predict(src_source_info_0.drop(drop_features_0, axis=1))
#         src_source_info_0['label'] = src_source_info_0['label'].apply(lambda x: 1 if x >= 0.7 else 0)
#     if src_source_info_1_7.shape[0] != 0:
#         src_source_info_1_7['label'] = source_lgb_b_cv.predict(src_source_info_1_7.drop(drop_features_1_7, axis=1))
#         src_source_info_1_7['label'] = src_source_info_1_7['label'].apply(lambda x: 1 if x >= 0.5 else 0)
#
#     # 数据输出
#     source_predict = source_predict.append(src_source_info_0).append(src_source_info_1_7)
#     source_predict['label'] = source_predict['label'].astype(int)
#
#     return {"columns": source_predict.columns.values.tolist(), "list": source_predict.values.tolist()}

"""
输出末日计划监控模型source_predict结果
"""


def source_predict(jsondata):
    '''
    :param jsondata: {"columns": ["channel_id","source_id","data_win","source_run_datetime","create_time","media_id","game_id","platform","source_run_date_amount","create_role_num",
    "create_role_cost","create_role_pay_num","create_role_pay_cost","create_role_pay_sum","create_role_roi","create_role_retain_1d"]}
    :return:
    '''
    data = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])
    # 数据预处理
    data = data[data['source_run_date_amount'] > 0]  # 删除消耗为0的数据
    # 去重，处理某天没有消耗的情况
    data.sort_values(by='data_win', inplace=True)
    data.drop_duplicates(subset=['channel_id', 'source_id', 'source_run_date_amount', 'create_role_num'], keep='first'
                         , inplace=True)
    # 对消耗500元以下的不作判断，对数据按平台、win进行分类处理
    data_ignore = data[data['source_run_date_amount'] < 500]
    data_not_ignore = data[data['source_run_date_amount'] >= 500]

    data_an = data_not_ignore[data_not_ignore['platform'] == 1]
    data_ios = data_not_ignore[data_not_ignore['platform'] == 2]

    data_win_0 = data_an[data_an['data_win'] == 0]
    data_win_1 = data_an[data_an['data_win'] == 1]
    data_win_2 = data_an[data_an['data_win'] == 2]
    data_win_3 = data_an[data_an['data_win'] == 3]
    data_win_7 = data_an[data_an['data_win'] == 7]

    data_win_0_ios = data_ios[data_ios['data_win'] == 0]
    data_win_1_ios = data_ios[data_ios['data_win'] == 1]
    data_win_2_ios = data_ios[data_ios['data_win'] == 2]
    data_win_3_ios = data_ios[data_ios['data_win'] == 3]
    data_win_7_ios = data_ios[data_ios['data_win'] == 7]

    result_df = pd.DataFrame()
    temp_df = pd.DataFrame()
    # win=0预判
    if data_win_0.shape[0] != 0:
        data_win_0['label'] = data_win_0.apply(
            lambda x: 1 if x.create_role_num == 0 else (
                1 if (x.create_role_cost >= 20) & (x.create_role_pay_num == 0) else (
                    0 if (x.create_role_cost <= 70) & (x.create_role_roi >= 0.02) else
                    (1 if x.create_role_cost >= 50 else 2))), axis=1)
        result_df = result_df.append(data_win_0[(data_win_0['label'] == 1) | (data_win_0['label'] == 0)])
        temp_df = temp_df.append(data_win_0[data_win_0['label'] == 2])
    if data_win_0_ios.shape[0] != 0:
        data_win_0_ios['label'] = data_win_0_ios.apply(lambda x: 1 if x.create_role_num == 0 else (
            1 if (x.create_role_cost >= 40) & (x.create_role_pay_num == 0) else (
                0 if (x.create_role_cost <= 300) & (x.create_role_roi >= 0.02) else (
                    1 if x.create_role_cost >= 120 else 2))), axis=1)
        result_df = result_df.append(data_win_0_ios[(data_win_0_ios['label'] == 1) | (data_win_0_ios['label'] == 0)])
        temp_df = temp_df.append(data_win_0_ios[data_win_0_ios['label'] == 2])

    # win=1预判
    if data_win_1.shape[0] != 0:
        data_win_1['label'] = data_win_1.apply(
            lambda x: 1 if x.create_role_num == 0 else (0 if (x.create_role_cost <= 70) & (x.create_role_roi >= 0.02)
                                                        else (
                1 if x.create_role_cost >= 50 else (1 if x.create_role_pay_sum == 0 else 2))), axis=1)
        result_df = result_df.append(data_win_1[(data_win_1['label'] == 1) | (data_win_1['label'] == 0)])
        temp_df = temp_df.append(data_win_1[data_win_1['label'] == 2])
    if data_win_1_ios.shape[0] != 0:
        data_win_1_ios['label'] = data_win_1_ios.apply(
            lambda x: 1 if x.create_role_num == 0 else (0 if (x.create_role_cost <= 300) & (x.create_role_roi >= 0.02)
                                                        else (
                1 if x.create_role_cost >= 120 else (1 if x.create_role_pay_sum == 0 else 2))), axis=1)
        result_df = result_df.append(data_win_1_ios[(data_win_1_ios['label'] == 1) | (data_win_1_ios['label'] == 0)])
        temp_df = temp_df.append(data_win_1_ios[data_win_1_ios['label'] == 2])

    # win=2预判
    if data_win_2.shape[0] != 0:
        data_win_2['label'] = data_win_2.apply(
            lambda x: 1 if x.create_role_num == 0 else (0 if (x.create_role_cost <= 70) & (x.create_role_roi >= 0.045)
                                                        else (
                1 if x.create_role_cost >= 50 else (1 if x.create_role_roi <= 0.03 else 2))), axis=1)
        result_df = result_df.append(data_win_2[(data_win_2['label'] == 1) | (data_win_2['label'] == 0)])
        temp_df = temp_df.append(data_win_2[data_win_2['label'] == 2])

    if data_win_2_ios.shape[0] != 0:
        data_win_2_ios['label'] = data_win_2_ios.apply(
            lambda x: 1 if x.create_role_num == 0 else (0 if (x.create_role_cost <= 250) & (x.create_role_roi >= 0.045)
                                                        else (
                1 if x.create_role_cost >= 120 else (1 if x.create_role_roi <= 0.02 else 2))), axis=1)
        result_df = result_df.append(data_win_2_ios[(data_win_2_ios['label'] == 1) | (data_win_2_ios['label'] == 0)])
        temp_df = temp_df.append(data_win_2_ios[data_win_2_ios['label'] == 2])

    # win=3预判
    if data_win_3.shape[0] != 0:
        data_win_3['label'] = data_win_3.apply(lambda x: 1 if x.create_role_num == 0 else (
            1 if x.create_role_pay_num == 0 else (0 if (x.create_role_cost <= 70) & (x.create_role_roi >= 0.07)
                                                  else (1 if x.create_role_pay_cost > 5000 else (
                1 if x.create_role_retain_1d < 0.05 else (1 if x.create_role_roi < 0.04 else 2))))), axis=1)
        result_df = result_df.append(data_win_3[(data_win_3['label'] == 1) | (data_win_3['label'] == 0)])
        temp_df = temp_df.append(data_win_3[data_win_3['label'] == 2])
    if data_win_3_ios.shape[0] != 0:
        data_win_3_ios['label'] = data_win_3_ios.apply(lambda x: 1 if x.create_role_num == 0 else (
            1 if x.create_role_pay_num == 0 else (0 if (x.create_role_cost <= 250) & (x.create_role_roi >= 0.07)
                                                  else (1 if x.create_role_pay_cost > 10000 else (
                1 if x.create_role_retain_1d < 0.05 else (1 if x.create_role_roi < 0.04 else 2))))), axis=1)
        result_df = result_df.append(data_win_3_ios[(data_win_3_ios['label'] == 1) | (data_win_3_ios['label'] == 0)])
        temp_df = temp_df.append(data_win_3_ios[data_win_3_ios['label'] == 2])

    # 模型预测
    if temp_df.shape[0] != 0:
        temp_df['label'] = gbdt.predict(
            temp_df[['data_win', 'platform', 'create_role_cost', 'create_role_pay_cost', 'create_role_roi']])
    # win=7
    if data_win_7.shape[0] != 0:
        data_win_7['label'] = data_win_7.apply(lambda x: 1 if x.create_role_num == 0 else (
            0 if (x.create_role_cost <= 50) & (x.create_role_roi >= 0.125) & (x.create_role_pay_cost <= 4000) else 1),
                                               axis=1)
        result_df = result_df.append(data_win_7[(data_win_7['label'] == 1) | (data_win_7['label'] == 0)])
        temp_df = temp_df.append(data_win_7[data_win_7['label'] == 2])
    if data_win_7_ios.shape[0] != 0:
        data_win_7_ios['label'] = data_win_7_ios.apply(lambda x: 1 if x.create_role_num == 0 else (
            0 if (x.create_role_cost <= 100) & (x.create_role_roi >= 0.11) & (x.create_role_pay_cost <= 8000) else 1),
                                                       axis=1)
        result_df = result_df.append(data_win_7_ios[(data_win_7_ios['label'] == 1) | (data_win_7_ios['label'] == 0)])
        temp_df = temp_df.append(data_win_7_ios[data_win_7_ios['label'] == 2])

    result_temp = result_df.append(temp_df)

    # 结果输出
    if result_temp.shape[0] != 0:
        part_1 = result_temp[result_temp['data_win'] == 0]
        part_2 = result_temp[result_temp['data_win'] == 7]
        if part_1.shape[0] != 0:
            temp = result_temp.drop(part_1.index, axis=0)
        else:
            temp = result_temp
        if temp.shape[0] != 0:
            if part_2.shape[0] != 0:
                temp = temp.drop(part_2.index, axis=0)
            # win1,2,3按优先级3,2,1进行去重
            temp.sort_values('data_win', ascending=False, inplace=True)
            temp.drop_duplicates(['channel_id', 'source_id', 'source_run_datetime'], keep='first', inplace=True)
            # win1,2,3去重后，与win0进行去重，优先win=0
            if part_1.shape[0] != 0:
                temp = pd.concat([temp, part_1], axis=0)
                temp.sort_values('data_win', inplace=True)
                temp.drop_duplicates(['channel_id', 'source_id', 'source_run_datetime'], keep='first', inplace=True)
            # 再去win=7进行去重，win=7优先级小
            if part_2.shape[0] != 0:
                temp = pd.concat([temp, part_2], axis=0)
                temp.sort_values('data_win', inplace=True)
                temp.drop_duplicates(['channel_id', 'source_id', 'source_run_datetime'], keep='first', inplace=True)
            source_predict = temp

        elif part_1.shape[0] != 0:
            temp = part_1
            if part_2.shape[0] != 0:
                temp = pd.concat([temp, part_2], axis=0)
                temp.sort_values('data_win', inplace=True)
                temp.drop_duplicates(['channel_id', 'source_id', 'source_run_datetime'], keep='first', inplace=True)
            source_predict = temp

        else:
            source_predict = part_2
    else:
        source_predict = pd.DataFrame(
            columns=["channel_id", "source_id", "data_win", "source_run_datetime", "create_time",
                     "media_id", "game_id", "platform", "source_run_date_amount", "create_role_num", "create_role_cost",
                     "create_role_pay_num", "create_role_pay_cost", "create_role_pay_sum", "create_role_roi",
                     "create_role_retain_1d", "label"])
    source_predict['label'] = source_predict['label'].astype(int)

    return {"columns": source_predict.columns.values.tolist(), "list": source_predict.values.tolist()}


"""
输出坦克计划监控模型source_predict-for_tank结果
"""


def source_predict_for_tank(jsondata):
    '''
    :param jsondata: {"columns": ["channel_id","source_id","data_win","source_run_datetime","create_time","media_id","game_id","platform","source_run_date_amount","create_role_num",
    "create_role_cost","create_role_pay_num","create_role_pay_cost","create_role_pay_sum","create_role_roi","create_role_retain_1d"]}
    :return:
    '''
    data = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])
    # 数据预处理
    data = data[data['source_run_date_amount'] > 0]  # 删除消耗为0的数据
    # 去重，处理某天没有消耗的情况
    data.sort_values(by='data_win', inplace=True)
    data.drop_duplicates(subset=['channel_id', 'source_id', 'source_run_date_amount', 'create_role_num'], keep='first'
                         , inplace=True)
    # 对消耗500元以下的不作判断，对数据按平台、win进行分类处理
    data_ignore = data[data['source_run_date_amount'] < 500]
    data_not_ignore = data[data['source_run_date_amount'] >= 500]

    data_an = data_not_ignore[data_not_ignore['platform'] == 1]
    data_ios = data_not_ignore[data_not_ignore['platform'] == 2]

    data_win_0 = data_an[data_an['data_win'] == 0]
    data_win_1 = data_an[data_an['data_win'] == 1]
    data_win_2 = data_an[data_an['data_win'] == 2]
    data_win_3 = data_an[data_an['data_win'] == 3]
    data_win_7 = data_an[data_an['data_win'] == 7]

    data_win_0_ios = data_ios[data_ios['data_win'] == 0]
    data_win_1_ios = data_ios[data_ios['data_win'] == 1]
    data_win_2_ios = data_ios[data_ios['data_win'] == 2]
    data_win_3_ios = data_ios[data_ios['data_win'] == 3]
    data_win_7_ios = data_ios[data_ios['data_win'] == 7]

    result_df = pd.DataFrame()
    temp_df = pd.DataFrame()
    # win=0预判
    if data_win_0.shape[0] != 0:
        data_win_0['label'] = data_win_0.apply(
            lambda x: 1 if x.create_role_num == 0 else (1 if (x.create_role_cost >= 35)
                                                             & (x.create_role_pay_num == 0) else (
                0 if (x.create_role_cost <= 150) & (x.create_role_roi >= 0.025)
                else (1 if x.create_role_cost >= 80 else (1 if x.create_role_pay_cost > 2000 else 2)))), axis=1)
        result_df = result_df.append(data_win_0[(data_win_0['label'] == 1) | (data_win_0['label'] == 0)])
        temp_df = temp_df.append(data_win_0[data_win_0['label'] == 2])
    if data_win_0_ios.shape[0] != 0:
        data_win_0_ios['label'] = data_win_0_ios.apply(
            lambda x: 1 if x.create_role_num == 0 else (1 if (x.create_role_cost >= 180)
                                                             & (x.create_role_pay_num == 0) else (
                0 if (x.create_role_cost <= 600) & (x.create_role_roi >= 0.02) else
                (1 if x.create_role_cost >= 300 else (1 if x.create_role_pay_cost > 4000 else 2)))), axis=1)
        result_df = result_df.append(data_win_0_ios[(data_win_0_ios['label'] == 1) | (data_win_0_ios['label'] == 0)])
        temp_df = temp_df.append(data_win_0_ios[data_win_0_ios['label'] == 2])

    # win=1预判
    if data_win_1.shape[0] != 0:
        data_win_1['label'] = data_win_1.apply(
            lambda x: 1 if x.create_role_num == 0 else (0 if (x.create_role_cost <= 150) & (x.create_role_roi >= 0.025)
                                                        else (
                1 if x.create_role_cost >= 80 else (1 if x.create_role_pay_sum == 0 else 2))), axis=1)
        result_df = result_df.append(data_win_1[(data_win_1['label'] == 1) | (data_win_1['label'] == 0)])
        temp_df = temp_df.append(data_win_1[data_win_1['label'] == 2])
    if data_win_1_ios.shape[0] != 0:
        data_win_1_ios['label'] = data_win_1_ios.apply(
            lambda x: 1 if x.create_role_num == 0 else (0 if (x.create_role_cost <= 600) & (x.create_role_roi >= 0.02)
                                                        else (
                1 if x.create_role_cost >= 300 else (1 if x.create_role_pay_sum == 0 else 2))), axis=1)
        result_df = result_df.append(data_win_1_ios[(data_win_1_ios['label'] == 1) | (data_win_1_ios['label'] == 0)])
        temp_df = temp_df.append(data_win_1_ios[data_win_1_ios['label'] == 2])

    # win=2预判
    if data_win_2.shape[0] != 0:
        data_win_2['label'] = data_win_2.apply(
            lambda x: 1 if x.create_role_num == 0 else (0 if (x.create_role_cost <= 130) & (x.create_role_roi >= 0.05)
                                                        else (1 if x.create_role_cost >= 70 else (
                1 if x.create_role_roi <= 0.025 else (1 if x.create_role_pay_cost > 2000 else 2)))), axis=1)
        result_df = result_df.append(data_win_2[(data_win_2['label'] == 1) | (data_win_2['label'] == 0)])
        temp_df = temp_df.append(data_win_2[data_win_2['label'] == 2])

    if data_win_2_ios.shape[0] != 0:
        data_win_2_ios['label'] = data_win_2_ios.apply(
            lambda x: 1 if x.create_role_num == 0 else (0 if (x.create_role_cost <= 500) & (x.create_role_roi >= 0.045)
                                                        else (1 if x.create_role_cost >= 260 else (
                1 if x.create_role_roi <= 0.02 else (1 if x.create_role_pay_cost > 4000 else 2)))), axis=1)
        result_df = result_df.append(data_win_2_ios[(data_win_2_ios['label'] == 1) | (data_win_2_ios['label'] == 0)])
        temp_df = temp_df.append(data_win_2_ios[data_win_2_ios['label'] == 2])

    # win=3预判
    if data_win_3.shape[0] != 0:
        data_win_3['label'] = data_win_3.apply(lambda x: 1 if x.create_role_num == 0 else (
            1 if x.create_role_pay_num == 0 else (0 if (x.create_role_cost <= 120) & (x.create_role_roi >= 0.08)
                                                  else (1 if x.create_role_pay_cost > 2000 else (
                1 if x.create_role_retain_1d < 0.08 else (1 if x.create_role_roi < 0.04 else 2))))), axis=1)
        result_df = result_df.append(data_win_3[(data_win_3['label'] == 1) | (data_win_3['label'] == 0)])
        temp_df = temp_df.append(data_win_3[data_win_3['label'] == 2])
    if data_win_3_ios.shape[0] != 0:
        data_win_3_ios['label'] = data_win_3_ios.apply(lambda x: 1 if x.create_role_num == 0 else (
            1 if x.create_role_pay_num == 0 else (0 if (x.create_role_cost <= 400) & (x.create_role_roi >= 0.07)
                                                  else (1 if x.create_role_pay_cost > 4000 else (
                1 if x.create_role_retain_1d < 0.08 else (1 if x.create_role_roi < 0.04 else 2))))), axis=1)
        result_df = result_df.append(data_win_3_ios[(data_win_3_ios['label'] == 1) | (data_win_3_ios['label'] == 0)])
        temp_df = temp_df.append(data_win_3_ios[data_win_3_ios['label'] == 2])

    # 模型预测
    if temp_df.shape[0] != 0:
        temp_df['label'] = model_for_tank.predict(
            temp_df[['data_win', 'platform', 'create_role_cost', 'create_role_pay_cost', 'create_role_roi']])
    # win=7
    if data_win_7.shape[0] != 0:
        data_win_7['label'] = data_win_7.apply(lambda x: 1 if x.create_role_num == 0 else (
            0 if (x.create_role_cost <= 50) & (x.create_role_roi >= 0.14) & (x.create_role_pay_cost <= 2000) else 1),
                                               axis=1)
        result_df = result_df.append(data_win_7[(data_win_7['label'] == 1) | (data_win_7['label'] == 0)])
        temp_df = temp_df.append(data_win_7[data_win_7['label'] == 2])
    if data_win_7_ios.shape[0] != 0:
        data_win_7_ios['label'] = data_win_7_ios.apply(lambda x: 1 if x.create_role_num == 0 else (
            0 if (x.create_role_cost <= 250) & (x.create_role_roi >= 0.13) & (x.create_role_pay_cost <= 4000) else 1),
                                                       axis=1)
        result_df = result_df.append(data_win_7_ios[(data_win_7_ios['label'] == 1) | (data_win_7_ios['label'] == 0)])
        temp_df = temp_df.append(data_win_7_ios[data_win_7_ios['label'] == 2])

    result_temp = result_df.append(temp_df)

    # 结果输出
    if result_temp.shape[0] != 0:
        part_1 = result_temp[result_temp['data_win'] == 0]
        part_2 = result_temp[result_temp['data_win'] == 7]
        if part_1.shape[0] != 0:
            temp = result_temp.drop(part_1.index, axis=0)
        else:
            temp = result_temp
        if temp.shape[0] != 0:
            if part_2.shape[0] != 0:
                temp = temp.drop(part_2.index, axis=0)
            # win1,2,3按优先级3,2,1进行去重
            temp.sort_values('data_win', ascending=False, inplace=True)
            temp.drop_duplicates(['channel_id', 'source_id', 'source_run_datetime'], keep='first', inplace=True)
            # win1,2,3去重后，与win0进行去重，优先win=0
            if part_1.shape[0] != 0:
                temp = pd.concat([temp, part_1], axis=0)
                temp.sort_values('data_win', inplace=True)
                temp.drop_duplicates(['channel_id', 'source_id', 'source_run_datetime'], keep='first', inplace=True)
            # 再去win=7进行去重，win=7优先级小
            if part_2.shape[0] != 0:
                temp = pd.concat([temp, part_2], axis=0)
                temp.sort_values('data_win', inplace=True)
                temp.drop_duplicates(['channel_id', 'source_id', 'source_run_datetime'], keep='first', inplace=True)
            source_predict = temp

        elif part_1.shape[0] != 0:
            temp = part_1
            if part_2.shape[0] != 0:
                temp = pd.concat([temp, part_2], axis=0)
                temp.sort_values('data_win', inplace=True)
                temp.drop_duplicates(['channel_id', 'source_id', 'source_run_datetime'], keep='first', inplace=True)
            source_predict = temp

        else:
            source_predict = part_2
    else:
        source_predict = pd.DataFrame(
            columns=["channel_id", "source_id", "data_win", "source_run_datetime", "create_time",
                     "media_id", "game_id", "platform", "source_run_date_amount", "create_role_num", "create_role_cost",
                     "create_role_pay_num", "create_role_pay_cost", "create_role_pay_sum", "create_role_roi",
                     "create_role_retain_1d", "label"])
    source_predict['label'] = source_predict['label'].astype(int)

    return {"columns": source_predict.columns.values.tolist(), "list": source_predict.values.tolist()}


# 2020-11-12:双状况计划监控模型——末日
# def source_predict_state_1(jsondata):
#     '''
#     :param jsondata: {"columns": ["channel_id","source_id","plan_name", "model_run_datetime","create_time","media_id",
#     "game_id","platform","source_run_date_amount","create_role_num","create_role_cost","create_role_pay_num",
#     "create_role_pay_cost","create_role_pay_sum","create_role_roi","create_role_pay_rate","create_role_pay_num_cum",
#     "learning_type","learning_time_dt","learning_time_hr","deep_bid_type"]}
#     :return:
#     '''
#     data = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])
#     data['learning_time'] = data['learning_time_dt'] + ' ' + data['learning_time_hr']
#     data['learning_time'] = pd.to_datetime(data['learning_time'], errors='coerce')
#     data['learning_time_interval'] = (pd.datetime.now() - data['learning_time']).astype('timedelta64[h]')
#     # 数据预处理
#     data = data[data['source_run_date_amount'] > 0]  # 删除消耗为0的数据
#     # 去重，处理某天没有消耗的情况
#     data.drop_duplicates(subset=['channel_id', 'source_id', 'source_run_date_amount', 'create_role_num'], keep='first'
#                          , inplace=True)
#     # 对消耗300元以下的不作判断，对数据按平台、win进行分类处理
#     #   data_ignore = data[data['source_run_date_amount'] < 300]
#     data_not_ignore = data[data['source_run_date_amount'] >= 300]
#
#     # 增加每次付费跑法的判断
#     data_pre_action = data_not_ignore[data_not_ignore['deep_bid_type'] == 'BID_PER_ACTION']
#     data_not_ignore = data_not_ignore[data_not_ignore['deep_bid_type'] != 'BID_PER_ACTION']
#     # 已过学习期二天内和冲刺学习期的计划
#     data_learning = data_not_ignore[
#         ((data_not_ignore['learning_type'] == 0) & (data_not_ignore['create_role_pay_num_cum'] >= 15)) | (
#                 (data_not_ignore['learning_type'] == 1) & (data_not_ignore['learning_time_interval'] <= 48))]
#     data_not_ignore = data_not_ignore.drop(data_learning.index, axis=0)
#
#     data_win_0 = data_not_ignore[data_not_ignore['platform'] == 1]
#     data_win_0_ios = data_not_ignore[data_not_ignore['platform'] == 2]
#
#     data_win_0_learning = data_learning[data_learning['platform'] == 1]
#     data_win_0_ios_learning = data_learning[data_learning['platform'] == 2]
#
#     data_win_0_pre = data_pre_action[data_pre_action['platform'] == 1]
#     data_win_0_ios_pre = data_pre_action[data_pre_action['platform'] == 2]
#
#     result_df = pd.DataFrame()
#     temp_df = pd.DataFrame()
#     # win=0预判
#     # 每次付费安卓
#     if data_win_0_pre.shape[0] != 0:
#         data_win_0_pre['label'] = data_win_0_pre.apply(
#             lambda x: 1 if x.create_role_num == 0 else (1 if (x.create_role_pay_sum == 0) &
#                                                              (x.create_role_cost >= 200) else (
#                 2 if (x.create_role_pay_sum == 0) & (x.create_role_cost < 200) else
#                 (2 if (x.create_role_pay_sum != 0) & (x.create_role_pay_cost >= 8000) else (
#                     2 if (x.create_role_pay_sum != 0) &
#                          (x.create_role_pay_cost < 8000) & (x.create_role_cost >= 300) else 0)))), axis=1)
#         result_df = result_df.append(data_win_0_pre[(data_win_0_pre['label'] == 1) | (data_win_0_pre['label'] == 2)])
#         temp_df = temp_df.append(data_win_0_pre[data_win_0_pre['label'] == 0])
#
#     # 每次付费ISO
#     if data_win_0_ios_pre.shape[0] != 0:
#         data_win_0_ios_pre['label'] = data_win_0_ios_pre.apply(
#             lambda x: 1 if x.create_role_num == 0 else (1 if (x.create_role_pay_sum == 0) &
#                                                              (x.create_role_cost >= 300) else (
#                 2 if (x.create_role_pay_sum == 0) & (x.create_role_cost < 300) else
#                 (2 if (x.create_role_pay_sum != 0) & (x.create_role_pay_cost >= 14000) else (
#                     2 if (x.create_role_pay_sum != 0) &
#                          (x.create_role_pay_cost < 14000) & (x.create_role_cost >= 400) else 0)))), axis=1)
#         result_df = result_df.append(data_win_0_ios_pre[(data_win_0_ios_pre['label'] == 1) | (data_win_0_ios_pre['label'] == 2)])
#         temp_df = temp_df.append(data_win_0_ios_pre[data_win_0_ios_pre['label'] == 0])
#
#     if data_win_0.shape[0] != 0:
#         data_win_0['label'] = data_win_0.apply(
#             lambda x: 1 if x.create_role_num == 0 else (1 if (x.create_role_pay_sum == 0) &
#                                                              (x.create_role_cost >= 80) else (
#                 2 if (x.create_role_pay_sum == 0) & (x.create_role_cost < 80) else
#                 (2 if (x.create_role_pay_sum != 0) & (x.create_role_pay_cost >= 7000) else (
#                     2 if (x.create_role_pay_sum != 0) &
#                          (x.create_role_pay_cost < 7000) & (x.create_role_cost >= 140) else 0)))), axis=1)
#         result_df = result_df.append(data_win_0[(data_win_0['label'] == 1) | (data_win_0['label'] == 2)])
#         temp_df = temp_df.append(data_win_0[data_win_0['label'] == 0])
#
#     if data_win_0_learning.shape[0] != 0:
#         data_win_0_learning['label'] = data_win_0_learning.apply(
#             lambda x: 1 if x.create_role_num == 0 else (1 if (x.create_role_pay_sum == 0) &
#                                                              (x.create_role_cost >= 100) else (
#                 2 if (x.create_role_pay_sum == 0) & (x.create_role_cost < 100) else
#                 (2 if (x.create_role_pay_sum != 0) & (x.create_role_pay_cost >= 9000) else (
#                     2 if (x.create_role_pay_sum != 0) &
#                          (x.create_role_pay_cost < 9000) & (x.create_role_cost >= 160) else 0)))), axis=1)
#         result_df = result_df.append(
#             data_win_0_learning[(data_win_0_learning['label'] == 1) | (data_win_0_learning['label'] == 2)])
#         temp_df = temp_df.append(data_win_0_learning[data_win_0_learning['label'] == 0])
#
#     if data_win_0_ios.shape[0] != 0:
#         data_win_0_ios['label'] = data_win_0_ios.apply(
#             lambda x: 1 if x.create_role_num == 0 else (1 if (x.create_role_pay_sum == 0) &
#                                                              (x.create_role_cost >= 200) else (
#                 2 if (x.create_role_pay_sum == 0) & (x.create_role_cost < 200) else
#                 (2 if (x.create_role_pay_sum != 0) & (x.create_role_pay_cost >= 12000) else (
#                     2 if (x.create_role_pay_sum != 0) &
#                          (x.create_role_pay_cost < 12000) & (x.create_role_cost >= 300) else 0)))), axis=1)
#         result_df = result_df.append(data_win_0_ios[(data_win_0_ios['label'] == 1) | (data_win_0_ios['label'] == 2)])
#         temp_df = temp_df.append(data_win_0_ios[data_win_0_ios['label'] == 0])
#
#     if data_win_0_ios_learning.shape[0] != 0:
#         data_win_0_ios_learning['label'] = data_win_0_ios_learning.apply(
#             lambda x: 1 if x.create_role_num == 0 else (1 if (x.create_role_pay_sum == 0) &
#                                                              (x.create_role_cost >= 250) else (
#                 2 if (x.create_role_pay_sum == 0) & (x.create_role_cost < 250) else
#                 (2 if (x.create_role_pay_sum != 0) & (x.create_role_pay_cost >= 14000) else (
#                     2 if (x.create_role_pay_sum != 0) &
#                          (x.create_role_pay_cost < 14000) & (x.create_role_cost >= 350) else 0)))), axis=1)
#         result_df = result_df.append(
#             data_win_0_ios_learning[(data_win_0_ios_learning['label'] == 1) | (data_win_0_ios_learning['label'] == 2)])
#         temp_df = temp_df.append(data_win_0_ios_learning[data_win_0_ios_learning['label'] == 0])
#
#     # 模型预测
#     if temp_df.shape[0] != 0:
#         temp_df['label'] = gbdt_win0.predict(temp_df[['source_run_date_amount',
#                                                       'create_role_num', 'create_role_cost', 'create_role_pay_num',
#                                                       'create_role_pay_cost', 'create_role_pay_sum', 'create_role_roi',
#                                                       'create_role_pay_rate']])
#         temp_df['label'] = temp_df['label'].apply(lambda x: 3 if x == 1 else 4)
#
#     source_predict = pd.concat([result_df, temp_df], axis=0)
#
#     source_predict['label'] = source_predict['label'].astype(int)
#     source_predict.drop(['learning_time', 'learning_time_interval'], axis=1, inplace=True)
#
#     return {"columns": source_predict.columns.values.tolist(), "list": source_predict.values.tolist()}

# 2021-6-21:双状况计划监控模型——末日广点通
def source_predict_state_1_gdt(jsondata):
    '''
    :param jsondata: {"columns": ["channel_id","source_id","plan_name", "model_run_datetime","create_time","media_id",
    "game_id","platform","source_run_date_amount","create_role_num","create_role_cost","create_role_pay_num",
    "create_role_pay_cost","create_role_pay_sum","create_role_roi","create_role_pay_rate","create_role_pay_num_cum",
    "learning_type","learning_time_dt","learning_time_hr","deep_bid_type"]}
    :return:'label','budget'
    '''

    # 该模型只能接受计划状态plan_run_state=1且data_win=0 且media_id=16 且 model_run_datetime 等于当日的数据，即只接受当日广点通实时数据
    # label为1时关计划，为0时开计划，如果模型输出label结果与计划当前状况（开/关）相同，则不操作。例如，计划当前是关停状况，当模型输出为1时，不操作

    # 早上7：55分，将所有在运行的计划状态plan_run_state改为2。调用模型2进行判断，是否达标。达标则状态改为1，开计划，预算由模型1计算决定。不达标，则为关停状态，且plan_run_state为2

    data = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])
    data = data[data['media_id'] == 16]
    # 数据预处理
    data = data[data['source_run_date_amount'] > 0]  # 删除消耗为0的数据

    # 对消耗300元以下的不作判断，对数据按平台、win进行分类处理
    data_not_ignore = data[data['source_run_date_amount'] >= 300]

    # 增加每次付费跑法的判断
    #     data_pre_action = data_not_ignore[data_not_ignore['deep_bid_type'] == 'BID_PER_ACTION']
    #     data_not_ignore = data_not_ignore[data_not_ignore['deep_bid_type'] != 'BID_PER_ACTION']

    data_win_0 = data_not_ignore[data_not_ignore['platform'] == 1]
    data_win_0_ios = data_not_ignore[data_not_ignore['platform'] == 2]

    #     data_win_0_pre = data_pre_action[data_pre_action['platform'] == 1]
    #     data_win_0_ios_pre = data_pre_action[data_pre_action['platform'] == 2]

    result_df = pd.DataFrame()

    # 1:关; 0:开； 2：保持原状
    # win=0预判
    if data_win_0.shape[0] != 0:
        data_win_0_1 = data_win_0[data_win_0['source_run_date_amount'] <= 7000]
        data_win_0_2 = data_win_0[
            (data_win_0['source_run_date_amount'] > 7000) & (data_win_0['source_run_date_amount'] <= 13000)]
        data_win_0_3 = data_win_0[
            (data_win_0['source_run_date_amount'] > 13000) & (data_win_0['source_run_date_amount'] <= 25000)]
        data_win_0_4 = data_win_0[
            (data_win_0['source_run_date_amount'] > 25000) & (data_win_0['source_run_date_amount'] <= 50000)]
        data_win_0_5 = data_win_0[data_win_0['source_run_date_amount'] > 50000]
        if data_win_0_1.shape[0] != 0:
            # data_win_0_1['label'] = data_win_0_1.apply(
            #     lambda x: 1 if (x.create_role_cost >= 200) & (x.deep_bid_type == 'BID_PER_ACTION') |
            #                    (x.create_role_cost >= 100) & (x.deep_bid_type != 'BID_PER_ACTION') else (
            #         1 if (x.create_role_roi <= 0.02)
            #              & (x.source_run_date_amount >= 3500) else (0 if (x.create_role_roi >= 0.03) else 2)), axis=1)

            data_win_0_1['label'] = data_win_0_1.apply(
                lambda x: 1 if (((x.create_role_cost >= 200) & (x.source_run_date_amount <= 1000) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 300) & (x.source_run_date_amount <= 1000) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_cost >= 120) & (x.source_run_date_amount > 1000) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 180) & (x.source_run_date_amount > 1000) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_roi < 0.016) & (x.source_run_date_amount >= 3500))) else (0
                          if (((x.create_role_cost <= 180) & (x.source_run_date_amount <= 1000) & (x.create_role_pay_sum == 0)) |
                              ((x.create_role_cost <= 280) & (x.source_run_date_amount <= 1000) & (x.create_role_pay_sum != 0)) |
                              ((x.create_role_cost <= 100) & (x.source_run_date_amount > 1000) & (x.create_role_pay_sum == 0)) |
                              ((x.create_role_cost <= 160) & (x.source_run_date_amount > 1000) & (x.create_role_pay_sum != 0)) |
                              (x.create_role_roi >= 0.016)) else 2), axis=1)

            result_df = result_df.append(data_win_0_1)

        if data_win_0_2.shape[0] != 0:
            data_win_0_2['label'] = data_win_0_2.apply(
                lambda x: 1 if (x.create_role_cost >= 140) else (1 if (x.create_role_roi <= 0.019)
                                                                      | (x.create_role_pay_cost >= 6000) else (
                    0 if (x.create_role_roi >= 0.02) else 2)), axis=1)
            result_df = result_df.append(data_win_0_2)

        if data_win_0_3.shape[0] != 0:
            data_win_0_3['label'] = data_win_0_3.apply(
                lambda x: 1 if (x.create_role_cost >= 130) else (1 if (x.create_role_roi <= 0.02)
                                                                      | (x.create_role_pay_cost >= 6000) else (
                    0 if (x.create_role_roi >= 0.022) else 2)), axis=1)
            result_df = result_df.append(data_win_0_3)

        if data_win_0_4.shape[0] != 0:
            data_win_0_4['label'] = data_win_0_4.apply(
                lambda x: 1 if (x.create_role_cost >= 130) else (1 if (x.create_role_roi <= 0.022)
                                                                      | (x.create_role_pay_cost >= 5000) else (
                    0 if (x.create_role_roi >= 0.024) else 2)), axis=1)
            result_df = result_df.append(data_win_0_4)

        if data_win_0_5.shape[0] != 0:
            data_win_0_5['label'] = data_win_0_5.apply(
                lambda x: 1 if (x.create_role_cost >= 130) else (1 if (x.create_role_roi <= 0.024)
                                                                      | (x.create_role_pay_cost >= 4500) else (
                    0 if (x.create_role_roi >= 0.026) else 2)), axis=1)
            result_df = result_df.append(data_win_0_5)

    # ISO
    if data_win_0_ios.shape[0] != 0:
        data_win_0_ios_1 = data_win_0_ios[data_win_0_ios['source_run_date_amount'] <= 7000]
        data_win_0_ios_2 = data_win_0_ios[
            (data_win_0_ios['source_run_date_amount'] > 7000) & (data_win_0_ios['source_run_date_amount'] <= 13000)]
        data_win_0_ios_3 = data_win_0_ios[
            (data_win_0_ios['source_run_date_amount'] > 13000) & (data_win_0_ios['source_run_date_amount'] <= 25000)]
        data_win_0_ios_4 = data_win_0_ios[
            (data_win_0_ios['source_run_date_amount'] > 25000) & (data_win_0_ios['source_run_date_amount'] <= 50000)]
        data_win_0_ios_5 = data_win_0_ios[data_win_0_ios['source_run_date_amount'] > 50000]
        if data_win_0_ios_1.shape[0] != 0:
            data_win_0_ios_1['label'] = data_win_0_ios_1.apply(
                lambda x: 1 if (((x.create_role_cost >= 300) & (x.source_run_date_amount <= 1000) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 400) & (x.source_run_date_amount <= 1000) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_cost >= 200) & (x.source_run_date_amount > 1000) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 300) & (x.source_run_date_amount > 1000) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_roi < 0.014) & (x.source_run_date_amount >= 4500))) else (0
                          if (((x.create_role_cost <= 280) & (x.source_run_date_amount <= 1000) & (x.create_role_pay_sum == 0)) |
                              ((x.create_role_cost <= 380) & (x.source_run_date_amount <= 1000) & (x.create_role_pay_sum != 0)) |
                              ((x.create_role_cost <= 180) & (x.source_run_date_amount > 1000) & (x.create_role_pay_sum == 0)) |
                              ((x.create_role_cost <= 280) & (x.source_run_date_amount > 1000) & (x.create_role_pay_sum != 0)) |
                              (x.create_role_roi >= 0.014)) else 2), axis=1)
            result_df = result_df.append(data_win_0_ios_1)

        if data_win_0_ios_2.shape[0] != 0:
            data_win_0_ios_2['label'] = data_win_0_ios_2.apply(
                lambda x: 1 if (x.create_role_cost >= 200) else (1 if (x.create_role_roi <= 0.017)
                                                                      | (x.create_role_pay_cost >= 7000) else (
                    0 if (x.create_role_roi >= 0.018) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_2)

        if data_win_0_ios_3.shape[0] != 0:
            data_win_0_ios_3['label'] = data_win_0_ios_3.apply(
                lambda x: 1 if (x.create_role_cost >= 200) else (1 if (x.create_role_roi <= 0.018)
                                                                      | (x.create_role_pay_cost >= 7000) else (
                    0 if (x.create_role_roi >= 0.02) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_3)

        if data_win_0_ios_4.shape[0] != 0:
            data_win_0_ios_4['label'] = data_win_0_ios_4.apply(
                lambda x: 1 if (x.create_role_cost >= 200) else (1 if (x.create_role_roi <= 0.02)
                                                                      | (x.create_role_pay_cost >= 6000) else (
                    0 if (x.create_role_roi >= 0.022) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_4)

        if data_win_0_ios_5.shape[0] != 0:
            data_win_0_ios_5['label'] = data_win_0_ios_5.apply(
                lambda x: 1 if (x.create_role_cost >= 200) else (1 if (x.create_role_roi <= 0.022)
                                                                      | (x.create_role_pay_cost >= 6000) else (
                    0 if (x.create_role_roi >= 0.024) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_5)
    if result_df.shape[0] != 0:
        result_df['label'] = result_df['label'].astype(int)
        result_df['budget'] = 'None'
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


def source_predict_state_1(jsondata):
    '''
    :param jsondata: {"columns": ["channel_id","source_id","plan_name", "model_run_datetime","create_time","media_id",
    "game_id","platform","source_run_date_amount","create_role_num","create_role_cost","create_role_pay_num",
    "create_role_pay_cost","create_role_pay_sum","create_role_roi","create_role_pay_rate","create_role_pay_num_cum",
    "learning_type","learning_time_dt","learning_time_hr","deep_bid_type"]}
    :return:'label','budget'
    '''

    # 该模型只能接受计划状态plan_run_state=1且data_win=0 且 model_run_datetime 等于当日的数据，即只接受当日实时数据
    # label为1时关计划，为0时开计划，如果模型输出label结果与计划当前状况（开/关）相同，则不操作。例如，计划当前是关停状况，当模型输出为1时，不操作
    # budget输出值为该计划预算需要增加至的数值，如果budget小于等于当前计划预算，则不操作。例如budget=6000，则将该计划的预算增加至6000元
    # 加预算操作与开关操作相互独立，例如可以存在加了预算，但不开计划的情况
    # 晚上12点至第二天早上8：00不执行加预算操作，但开关指令正常执行
    # 早上7：55分，将所有在运行的计划状态plan_run_state改为2。调用模型2进行判断，是否达标。达标则状态改为1，开计划，预算由模型1计算决定。不达标，则为关停状态，且plan_run_state为2

    data = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])

    # 数据预处理
    data = data[data['source_run_date_amount'] > 0]  # 删除消耗为0的数据
    # 去重，对当天运行进行监控情况
    # 对消耗300元以下的不作判断，对数据按平台、win进行分类处理
    data_not_ignore = data[data['source_run_date_amount'] >= 300]

    # 增加每次付费跑法的判断
    data_pre_action = data_not_ignore[data_not_ignore['deep_bid_type'] == 'BID_PER_ACTION']
    data_not_ignore = data_not_ignore[data_not_ignore['deep_bid_type'] != 'BID_PER_ACTION']

    data_win_0 = data_not_ignore[data_not_ignore['platform'] == 1]
    data_win_0_ios = data_not_ignore[data_not_ignore['platform'] == 2]

    data_win_0_pre = data_pre_action[data_pre_action['platform'] == 1]
    data_win_0_ios_pre = data_pre_action[data_pre_action['platform'] == 2]

    result_df = pd.DataFrame()

    # 1:关; 0:开； 3：加预算
    # win=0预判
    # 每次付费安卓
    if data_win_0_pre.shape[0] != 0:
        # data_win_0_pre['label'] = data_win_0_pre.apply(lambda x: 1 if (x.create_role_pay_sum == 0) &
        #                                                               (x.create_role_cost >= 350) else (
        #     1 if (x.create_role_pay_sum <= 24) & (x.create_role_cost >= 400) else
        #     (1 if (x.create_role_roi <= 0.018) & (x.create_role_cost >= 450) else (
        #         1 if x.create_role_cost >= 500 else 0))), axis=1)
        # 跑录屏素材，成本高
        data_win_0_pre['label'] = data_win_0_pre.apply(
            lambda x: 1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount <= 1000) &
                           (x.create_role_cost >= 300) else (
                1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount > 1000) &
                     (x.create_role_cost >= 200) else (
                    1 if (x.create_role_roi > 0) & (x.create_role_roi <= 0.02) & (x.create_role_cost >= 300) else (
                        1 if x.create_role_cost >= 400 else 0))), axis=1)
        # 跑小素材，成本低
        # data_win_0_pre['label'] = data_win_0_pre.apply(
        #     lambda x: 1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount <= 1000) &
        #                    (x.create_role_cost >= 150) else (
        #         1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount > 1000) &
        #              (x.create_role_cost >= 100) else (
        #             1 if (x.create_role_roi > 0) & (x.create_role_roi <= 0.02) & (x.create_role_cost >= 150) else (
        #                 1 if x.create_role_cost >= 200 else 0))), axis=1)

        result_df = result_df.append(data_win_0_pre)

    # 每次付费ISO
    if data_win_0_ios_pre.shape[0] != 0:
        # data_win_0_ios_pre['label'] = data_win_0_ios_pre.apply(lambda x: 1 if (x.create_role_pay_sum == 0) &
        #                                                                       (x.create_role_cost >= 400) else (
        #     1 if (x.create_role_pay_sum <= 24) & (x.create_role_cost >= 450) else
        #     (1 if (x.create_role_roi <= 0.014) & (x.create_role_cost >= 500) else (
        #         1 if x.create_role_cost >= 550 else 0))), axis=1)
        data_win_0_ios_pre['label'] = data_win_0_ios_pre.apply(
            lambda x: 1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount <= 1000) &
                           (x.create_role_cost >= 250) else (
                1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount > 1000) &
                     (x.create_role_cost >= 200) else (
                    1 if (x.create_role_roi <= 0.02) & (x.create_role_cost >= 300) else (
                        1 if x.create_role_cost >= 300 else 0))), axis=1)
        result_df = result_df.append(data_win_0_ios_pre)

    # 其它付费方式安卓
    if data_win_0.shape[0] != 0:
        # data_win_0['label'] = data_win_0.apply(lambda x: 1 if (x.create_role_pay_sum == 0) &
        #                                                       (x.create_role_cost >= 160) else (
        #     1 if (x.create_role_pay_sum <= 24) & (x.create_role_cost >= 200) else
        #     (1 if (x.create_role_roi <= 0.018) & (x.create_role_cost >= 240) else (
        #         1 if x.create_role_cost >= 300 else 0))), axis=1)
        # 跑录屏素材，成本高
        data_win_0['label'] = data_win_0.apply(
            lambda x: 1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount <= 1000) &
                           (x.create_role_cost >= 240) else (
                1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount > 1000) &
                     (x.create_role_cost >= 140) else (
                    1 if (x.create_role_roi > 0) & (x.create_role_roi <= 0.02) & (x.create_role_cost >= 200) else (
                        1 if x.create_role_cost >= 260 else 0))), axis=1)
        # 跑小素材，成本低
        # data_win_0['label'] = data_win_0.apply(
        #     lambda x: 1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount <= 1000) &
        #                    (x.create_role_cost >= 120) else (
        #         1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount > 1000) &
        #              (x.create_role_cost >= 70) else (
        #             1 if (x.create_role_roi > 0) & (x.create_role_roi <= 0.02) & (x.create_role_cost >= 100) else (
        #                 1 if x.create_role_cost >= 130 else 0))), axis=1)
        result_df = result_df.append(data_win_0)

    # 其它付费方式IOS
    if data_win_0_ios.shape[0] != 0:
        # data_win_0_ios['label'] = data_win_0_ios.apply(lambda x: 1 if (x.create_role_pay_sum == 0) &
        #                                                               (x.create_role_cost >= 180) else (
        #     1 if (x.create_role_pay_sum < 24) & (x.create_role_cost >= 200) else
        #     (1 if (x.create_role_roi <= 0.014) & (x.create_role_cost >= 250) else (
        #         1 if x.create_role_cost >= 300 else 0))), axis=1)
        data_win_0_ios['label'] = data_win_0_ios.apply(
            lambda x: 1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount <= 1000) &
                           (x.create_role_cost >= 200) else (
                1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount > 1000) &
                     (x.create_role_cost >= 150) else (
                    1 if (x.create_role_roi <= 0.02) & (x.create_role_cost >= 250) else (
                        1 if x.create_role_cost >= 250 else 0))), axis=1)
        result_df = result_df.append(data_win_0_ios)
    # 凌晨前关闭消耗大于1500且ROI<1.8%的计划
    # d_time = datetime.datetime.strptime(str(datetime.datetime.now().date()) + '23:40', '%Y-%m-%d%H:%M')
    # d_time1 = datetime.datetime.strptime(str((datetime.datetime.now() + datetime.timedelta(days=1)).date()) + '00:01',
    #                                      '%Y-%m-%d%H:%M')
    # n_time = datetime.datetime.now()
    # if n_time > d_time and n_time < d_time1 and result_df.shape[0] != 0:
    #     result_df['label'] = result_df.apply(
    #         lambda x: 1 if (x.source_run_date_amount >= 800) & (x.create_role_roi <= 0.018) else x.label, axis=1)
    if result_df.shape[0] != 0:
        result_df_an = result_df[result_df['platform'] == 1]
        result_df_ios = result_df[result_df['platform'] == 2]
        result_df = pd.DataFrame()

        # 安卓加预算
        data_1 = result_df_an[result_df_an['source_run_date_amount'] <= 4500]
        data_2 = result_df_an[
            (result_df_an['source_run_date_amount'] > 4500) & (result_df_an['source_run_date_amount'] <= 9200)]
        data_3 = result_df_an[
            (result_df_an['source_run_date_amount'] > 9200) & (result_df_an['source_run_date_amount'] <= 18000)]
        data_4 = result_df_an[
            (result_df_an['source_run_date_amount'] > 18000) & (result_df_an['source_run_date_amount'] <= 40000)]
        data_5 = result_df_an[
            (result_df_an['source_run_date_amount'] > 40000) & (result_df_an['source_run_date_amount'] <= 76000)]
        data_6 = result_df_an[
            (result_df_an['source_run_date_amount'] > 76000) & (result_df_an['source_run_date_amount'] <= 120000)]
        data_7 = result_df_an[
            (result_df_an['source_run_date_amount'] > 120000) & (result_df_an['source_run_date_amount'] <= 190000)]
        data_8 = result_df_an[
            (result_df_an['source_run_date_amount'] > 190000) & (result_df_an['source_run_date_amount'] <= 240000)]
        data_9 = result_df_an[
            (result_df_an['source_run_date_amount'] > 240000) & (result_df_an['source_run_date_amount'] <= 290000)]
        data_10 = result_df_an[result_df_an['source_run_date_amount'] > 290000]

        if data_1.shape[0] != 0:
            data_1['budget'] = data_1.apply(
                lambda x: 6600 if (x.create_role_pay_sum >= 60) & (x.create_role_pay_num >= 2) else
                (6600 if x.create_role_pay_sum >= 98 else np.nan), axis=1)
            result_df = result_df.append(data_1)
        if data_2.shape[0] != 0:
            # data_2['budget'] = data_2.apply(
            #     lambda x: 13000 if (x.create_role_roi >= 0.028) & (x.create_role_pay_num >= 2) else
            #     (13000 if x.create_role_roi >= 0.038 else np.nan), axis=1)
            data_2['budget'] = data_2.apply(
                lambda x: 13000 if (x.create_role_roi >= 0.028) & (x.create_role_pay_num >= 2) else np.nan, axis=1)
            result_df = result_df.append(data_2)
        if data_3.shape[0] != 0:
            data_3['budget'] = data_3.apply(
                lambda x: 23000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_num >= 3) else
                (23000 if x.create_role_roi >= 0.05 else np.nan), axis=1)
            # data_3['budget'] = data_3.apply(
            #     lambda x: 23000 if (x.create_role_roi >= 0.035) & (x.create_role_pay_num >= 2) else
            #     np.nan, axis=1)
            result_df = result_df.append(data_3)
        if data_4.shape[0] != 0:
            data_4['budget'] = data_4.apply(
                lambda x: 50000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_cost <= 4500) else
                (50000 if (x.create_role_roi >= 0.035) & (x.create_role_pay_cost <= 6000) else
                 np.nan), axis=1)
            result_df = result_df.append(data_4)
        if data_5.shape[0] != 0:
            data_5['budget'] = data_5.apply(
                lambda x: 95000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_cost <= 4200) else
                (95000 if (x.create_role_roi >= 0.035) & (x.create_role_pay_cost <= 5500) else
                 np.nan), axis=1)
            result_df = result_df.append(data_5)
        if data_6.shape[0] != 0:
            data_6['budget'] = data_6.apply(
                lambda x: 148000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_cost <= 4000) else
                (148000 if (x.create_role_roi >= 0.035) & (x.create_role_pay_cost <= 3500) else
                 np.nan), axis=1)
            result_df = result_df.append(data_6)
        if data_7.shape[0] != 0:
            data_7['budget'] = data_7.apply(
                lambda x: 220000 if (x.create_role_roi >= 0.031) & (x.create_role_pay_cost <= 4000) else
                (220000 if (x.create_role_roi >= 0.036) & (x.create_role_pay_cost <= 3500) else
                 np.nan), axis=1)
            result_df = result_df.append(data_7)
        if data_8.shape[0] != 0:
            data_8['budget'] = data_8.apply(
                lambda x: 275000 if (x.create_role_roi >= 0.032) & (x.create_role_pay_cost <= 3800) else
                (275000 if (x.create_role_roi >= 0.037) & (x.create_role_pay_cost <= 3300) else
                 np.nan), axis=1)
            result_df = result_df.append(data_8)
        if data_9.shape[0] != 0:
            data_9['budget'] = data_9.apply(
                lambda x: 320000 if (x.create_role_roi >= 0.032) & (x.create_role_pay_cost <= 3600) else
                (320000 if (x.create_role_roi >= 0.037) & (x.create_role_pay_cost <= 3100) else
                 np.nan), axis=1)
            result_df = result_df.append(data_9)
        if data_10.shape[0] != 0:
            data_10['budget'] = data_10.apply(
                lambda x: 450000 if (x.create_role_roi >= 0.033) & (x.create_role_pay_cost <= 3500) else
                (450000 if (x.create_role_roi >= 0.038) & (x.create_role_pay_cost <= 3000) else
                 np.nan), axis=1)
            result_df = result_df.append(data_10)

        # ios加预算
        data_1_ios = result_df_ios[result_df_ios['source_run_date_amount'] <= 5600]
        data_2_ios = result_df_ios[
            (result_df_ios['source_run_date_amount'] > 5600) & (result_df_ios['source_run_date_amount'] <= 9200)]
        data_3_ios = result_df_ios[
            (result_df_ios['source_run_date_amount'] > 9200) & (result_df_ios['source_run_date_amount'] <= 18000)]
        data_4_ios = result_df_ios[
            (result_df_ios['source_run_date_amount'] > 18000) & (result_df_ios['source_run_date_amount'] <= 40000)]
        data_5_ios = result_df_ios[
            (result_df_ios['source_run_date_amount'] > 40000) & (result_df_ios['source_run_date_amount'] <= 76000)]
        data_6_ios = result_df_ios[
            (result_df_ios['source_run_date_amount'] > 76000) & (result_df_ios['source_run_date_amount'] <= 120000)]
        data_7_ios = result_df_ios[
            (result_df_ios['source_run_date_amount'] > 120000) & (result_df_ios['source_run_date_amount'] <= 190000)]
        data_8_ios = result_df_ios[
            (result_df_ios['source_run_date_amount'] > 190000) & (result_df_ios['source_run_date_amount'] <= 240000)]
        data_9_ios = result_df_ios[
            (result_df_ios['source_run_date_amount'] > 240000) & (result_df_ios['source_run_date_amount'] <= 290000)]
        data_10_ios = result_df_ios[result_df_ios['source_run_date_amount'] > 290000]

        if data_1_ios.shape[0] != 0:
            data_1_ios['budget'] = data_1_ios.apply(
                lambda x: 8000 if (x.create_role_pay_sum >= 60) & (x.create_role_pay_num >= 2) else
                (8000 if x.create_role_pay_sum >= 98 else np.nan), axis=1)
            result_df = result_df.append(data_1_ios)
        if data_2_ios.shape[0] != 0:
            # data_2_ios['budget'] = data_2_ios.apply(
            #     lambda x: 13000 if (x.create_role_roi >= 0.028) & (x.create_role_pay_num >= 2) else
            #     (13000 if x.create_role_roi >= 0.04 else np.nan), axis=1)
            data_2_ios['budget'] = data_2_ios.apply(
                lambda x: 13000 if (x.create_role_roi >= 0.024) & (x.create_role_pay_num >= 2) else np.nan, axis=1)
            result_df = result_df.append(data_2_ios)
        if data_3_ios.shape[0] != 0:
            # data_3_ios['budget'] = data_3_ios.apply(
            #     lambda x: 23000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_num >= 2) else
            #     np.nan, axis=1)
            data_3_ios['budget'] = data_3_ios.apply(
                lambda x: 23000 if (x.create_role_roi >= 0.026) & (x.create_role_pay_num >= 3) else
                (23000 if x.create_role_roi >= 0.035 else np.nan), axis=1)
            result_df = result_df.append(data_3_ios)
        if data_4_ios.shape[0] != 0:
            data_4_ios['budget'] = data_4_ios.apply(
                lambda x: 50000 if (x.create_role_roi >= 0.028) & (x.create_role_pay_cost <= 6000) else
                (50000 if (x.create_role_roi >= 0.038) & (x.create_role_pay_cost <= 7500) else
                 np.nan), axis=1)
            result_df = result_df.append(data_4_ios)
        if data_5_ios.shape[0] != 0:
            data_5_ios['budget'] = data_5_ios.apply(
                lambda x: 95000 if (x.create_role_roi >= 0.029) & (x.create_role_pay_cost <= 5500) else
                (95000 if (x.create_role_roi >= 0.035) & (x.create_role_pay_cost <= 7000) else
                 np.nan), axis=1)
            result_df = result_df.append(data_5_ios)
        if data_6_ios.shape[0] != 0:
            data_6_ios['budget'] = data_6_ios.apply(
                lambda x: 148000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_cost <= 5000) else
                (148000 if (x.create_role_roi >= 0.035) & (x.create_role_pay_cost <= 6500) else
                 np.nan), axis=1)
            result_df = result_df.append(data_6_ios)
        if data_7_ios.shape[0] != 0:
            data_7_ios['budget'] = data_7_ios.apply(
                lambda x: 220000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_cost <= 4500) else
                (220000 if (x.create_role_roi >= 0.035) & (x.create_role_pay_cost <= 6000) else
                 np.nan), axis=1)
            result_df = result_df.append(data_7_ios)
        if data_8_ios.shape[0] != 0:
            data_8_ios['budget'] = data_8_ios.apply(
                lambda x: 275000 if (x.create_role_roi >= 0.031) & (x.create_role_pay_cost <= 4500) else
                (275000 if (x.create_role_roi >= 0.035) & (x.create_role_pay_cost <= 6000) else
                 np.nan), axis=1)
            result_df = result_df.append(data_8_ios)
        if data_9_ios.shape[0] != 0:
            data_9_ios['budget'] = data_9_ios.apply(
                lambda x: 320000 if (x.create_role_roi >= 0.031) & (x.create_role_pay_cost <= 4500) else
                (320000 if (x.create_role_roi >= 0.035) & (x.create_role_pay_cost <= 6000) else
                 np.nan), axis=1)
            result_df = result_df.append(data_9_ios)
        if data_10_ios.shape[0] != 0:
            data_10_ios['budget'] = data_10_ios.apply(
                lambda x: 450000 if (x.create_role_roi >= 0.031) & (x.create_role_pay_cost <= 4000) else
                (450000 if (x.create_role_roi >= 0.035) & (x.create_role_pay_cost <= 5500) else
                 np.nan), axis=1)
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


def source_predict_state_2(jsondata):
    '''
    :param jsondata: {"columns": ["channel_id","source_id","plan_name","model_run_datetime","create_time",
    "media_id","game_id","platform","data_win","source_run_date_amount","create_role_num",
    "create_role_cost","create_role_pay_num","create_role_pay_cost","create_role_pay_sum","create_role_roi",
    "create_role_retain_1d","create_role_pay_rate","create_role_pay_num_cum","learning_type","learning_time_dt",
    "learning_time_hr","deep_bid_type"]}
    :return:
    '''
    data = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])

    # 数据预处理
    data = data[data['source_run_date_amount'] > 0]  # 删除消耗为0的数据
    # 去重，处理某天没有消耗的情况
    data.sort_values(by='data_win', inplace=True)
    data.drop_duplicates(subset=['channel_id', 'source_id', 'source_run_date_amount', 'create_role_num'], keep='first'
                         , inplace=True)
    # 对消耗300元以下的不作判断，对数据按平台、win进行分类处理
    data_not_ignore = data[(data['source_run_date_amount'] >= 300) | (data['data_win'] == 1)]
    data_ignore = data.drop(data_not_ignore.index)

    data_win_1 = data_not_ignore[(data_not_ignore['data_win'] == 1) & (data_not_ignore['platform'] == 1)]
    data_win_2 = data_not_ignore[(data_not_ignore['data_win'] == 2) & (data_not_ignore['platform'] == 1)]
    data_win_3 = data_not_ignore[(data_not_ignore['data_win'] == 3) & (data_not_ignore['platform'] == 1)]

    data_win_1_ios = data_not_ignore[(data_not_ignore['data_win'] == 1) & (data_not_ignore['platform'] == 2)]
    data_win_2_ios = data_not_ignore[(data_not_ignore['data_win'] == 2) & (data_not_ignore['platform'] == 2)]
    data_win_3_ios = data_not_ignore[(data_not_ignore['data_win'] == 3) & (data_not_ignore['platform'] == 2)]

    # win=1预判
    result_win1 = pd.DataFrame()
    temp_win1 = pd.DataFrame()

    # data_win_1中再分deep_bid_type
    data_win_1_pre_action = data_win_1[data_win_1['deep_bid_type'] == 'BID_PER_ACTION']
    data_win_1 = data_win_1[data_win_1['deep_bid_type'] != 'BID_PER_ACTION']

    if data_win_1_pre_action.shape[0] != 0:
        data_win_1_pre_action['label'] = data_win_1_pre_action.apply(lambda x: 1 if x.create_role_num == 0 else (1 if
                                                                                                                 x.create_role_roi < 0.018 else (
            0 if (x.create_role_pay_num > 1) & (
                    x.create_role_roi >= 0.02) & (x.source_run_date_amount >= 500) else (
                0 if (x.create_role_roi >= 0.022) & (x.source_run_date_amount >= 500) else 1 if
                (x.create_role_pay_cost > 8000) | (x.create_role_cost > 220) else 2))), axis=1)

        result_win1 = result_win1.append(
            data_win_1_pre_action[(data_win_1_pre_action['label'] == 1) | (data_win_1_pre_action['label'] == 0)])
        temp_win1 = temp_win1.append(data_win_1_pre_action[data_win_1_pre_action['label'] == 2])

    if data_win_1.shape[0] != 0:
        data_win_1['label'] = data_win_1.apply(lambda x: 1 if x.create_role_num == 0 else (1 if
                                                                                           x.create_role_roi <= 0.018 else (
            0 if (x.create_role_pay_num > 1) & (
                    x.create_role_roi >= 0.02) & (x.source_run_date_amount >= 500) else (
                0 if (x.create_role_roi >= 0.022) & (x.source_run_date_amount >= 500) else 1 if
                (x.create_role_pay_cost > 8000) | (x.create_role_cost > 140) else 2))), axis=1)

        result_win1 = result_win1.append(data_win_1[(data_win_1['label'] == 1) | (data_win_1['label'] == 0)])
        temp_win1 = temp_win1.append(data_win_1[data_win_1['label'] == 2])

    data_win_1_ios_pre_action = data_win_1_ios[data_win_1_ios['deep_bid_type'] == 'BID_PER_ACTION']
    data_win_1_ios = data_win_1_ios[data_win_1_ios['deep_bid_type'] != 'BID_PER_ACTION']

    if data_win_1_ios_pre_action.shape[0] != 0:
        data_win_1_ios_pre_action['label'] = data_win_1_ios_pre_action.apply(
            lambda x: 1 if x.create_role_num == 0 else (1 if
                                                        x.create_role_roi <= 0.016 else (
                0 if (x.create_role_pay_num > 1) & (
                        x.create_role_roi >= 0.018) & (x.source_run_date_amount >= 500) else (
                    0 if (x.create_role_roi >= 0.02) & (x.source_run_date_amount >= 500) else 1 if
                    (x.create_role_pay_cost > 12000) | (x.create_role_cost > 300) else 2))), axis=1)

        result_win1 = result_win1.append(
            data_win_1_ios_pre_action[
                (data_win_1_ios_pre_action['label'] == 1) | (data_win_1_ios_pre_action['label'] == 0)])
        temp_win1 = temp_win1.append(data_win_1_ios_pre_action[data_win_1_ios_pre_action['label'] == 2])

    if data_win_1_ios.shape[0] != 0:
        data_win_1_ios['label'] = data_win_1_ios.apply(lambda x: 1 if x.create_role_num == 0 else (1 if
                                                                                                   x.create_role_roi <= 0.016 else (
            0 if (x.create_role_pay_num > 1) & (
                    x.create_role_roi >= 0.018) & (x.source_run_date_amount >= 500) else (
                0 if (x.create_role_roi >= 0.02) & (x.source_run_date_amount >= 500) else 1 if
                (x.create_role_pay_cost > 12000) | (x.create_role_cost > 200) else 2))), axis=1)

        result_win1 = result_win1.append(
            data_win_1_ios[(data_win_1_ios['label'] == 1) | (data_win_1_ios['label'] == 0)])
        temp_win1 = temp_win1.append(data_win_1_ios[data_win_1_ios['label'] == 2])

    # win=1 模型预测
    if temp_win1.shape[0] != 0:
        temp_win1['label'] = gbdt_b_win1.predict(temp_win1[['create_role_cost',
                                                            'create_role_pay_cost', 'create_role_roi',
                                                            'create_role_pay_rate']])
    result_win1_data = pd.concat([result_win1, temp_win1], axis=0)

    # win=2预判
    result_win2 = pd.DataFrame()
    temp_win2 = pd.DataFrame()
    if data_win_2.shape[0] != 0:
        data_win_2['label'] = data_win_2.apply(lambda x: 1 if x.create_role_num == 0 else (1 if
                                                                                           x.create_role_roi <= 0.033 else (
            0 if (x.create_role_pay_num > 1) & (
                    x.create_role_roi >= 0.035) & (x.source_run_date_amount >= 500) else (
                0 if (x.create_role_roi >= 0.038) & (x.source_run_date_amount >= 500) else 1 if
                (x.create_role_pay_cost > 7000) | (x.create_role_cost > 150) else 2))), axis=1)

        result_win2 = result_win2.append(data_win_2[(data_win_2['label'] == 1) | (data_win_2['label'] == 0)])
        temp_win2 = temp_win2.append(data_win_2[data_win_2['label'] == 2])

    if data_win_2_ios.shape[0] != 0:
        data_win_2_ios['label'] = data_win_2_ios.apply(lambda x: 1 if x.create_role_num == 0 else (1 if
                                                                                                   x.create_role_roi <= 0.033 else (
            0 if (x.create_role_pay_num > 1) & (
                    x.create_role_roi >= 0.035) & (x.source_run_date_amount >= 500) else (
                0 if (x.create_role_roi >= 0.038) & (x.source_run_date_amount >= 500) else 1 if
                (x.create_role_pay_cost > 10000) | (x.create_role_cost > 250) else 2))), axis=1)

        result_win2 = result_win2.append(
            data_win_2_ios[(data_win_2_ios['label'] == 1) | (data_win_2_ios['label'] == 0)])
        temp_win2 = temp_win2.append(data_win_2_ios[data_win_2_ios['label'] == 2])

    # win=2 模型预测
    if temp_win2.shape[0] != 0:
        temp_win2['label'] = gbdt_b_win2.predict(temp_win2[['create_role_cost',
                                                            'create_role_pay_cost', 'create_role_roi',
                                                            'create_role_pay_rate', 'create_role_retain_1d']])
    result_win2_data = pd.concat([result_win2, temp_win2], axis=0)

    # win=3预判
    result_win3 = pd.DataFrame()
    temp_win3 = pd.DataFrame()
    if data_win_3.shape[0] != 0:
        data_win_3['label'] = data_win_3.apply(lambda x: 1 if x.create_role_num == 0 else (1 if
                                                                                           x.create_role_roi <= 0.05 else (
            0 if (x.create_role_pay_num > 1) & (
                    x.create_role_roi >= 0.06) & (x.source_run_date_amount >= 500) else (
                0 if (x.create_role_roi >= 0.07) & (x.source_run_date_amount >= 500) else 1 if
                (x.create_role_pay_cost > 6000) | (x.create_role_cost > 120) else 2))), axis=1)

        result_win3 = result_win3.append(data_win_3[(data_win_3['label'] == 1) | (data_win_3['label'] == 0)])
        temp_win3 = temp_win3.append(data_win_3[data_win_3['label'] == 2])

    if data_win_3_ios.shape[0] != 0:
        data_win_3_ios['label'] = data_win_3_ios.apply(lambda x: 1 if x.create_role_num == 0 else (1 if
                                                                                                   x.create_role_roi <= 0.05 else (
            0 if (x.create_role_pay_num > 1) & (
                    x.create_role_roi >= 0.06) & (x.source_run_date_amount >= 500) else (
                0 if (x.create_role_roi >= 0.07) & (x.source_run_date_amount >= 500) else 1 if
                (x.create_role_pay_cost > 9000) | (x.create_role_cost > 200) else 2))), axis=1)

        result_win3 = result_win3.append(
            data_win_3_ios[(data_win_3_ios['label'] == 1) | (data_win_3_ios['label'] == 0)])
        temp_win3 = temp_win3.append(data_win_3_ios[data_win_3_ios['label'] == 2])

    # win=3 模型预测
    if temp_win3.shape[0] != 0:
        temp_win3['label'] = gbdt_b_win3.predict(temp_win3[['create_role_cost',
                                                            'create_role_pay_cost', 'create_role_roi',
                                                            'create_role_pay_rate', 'create_role_retain_1d']])
    result_win3_data = pd.concat([result_win3, temp_win3], axis=0)

    result = pd.concat([result_win1_data, result_win2_data, result_win3_data], axis=0)

    # 结果输出
    if result.shape[0] != 0:
        # 1\2有一个为开，则开
        if (result[result['data_win'] == 2].shape[0] != 0) & (result[result['data_win'] == 1].shape[0] != 0):
            result_1_2 = result[(result['data_win'] == 1) | (result['data_win'] == 2)]
            result_1_2_label = result_1_2[['channel_id', 'source_id', 'model_run_datetime', 'data_win', 'label']]
            result_1_2_label['data_win'] = result_1_2_label['data_win'].astype(int)
            result_1_2_piv = pd.pivot_table(result_1_2_label, index=['channel_id', 'source_id', 'model_run_datetime'],
                                            columns='data_win')
            result_1_2_piv.columns = result_1_2_piv.columns.droplevel()
            result_1_2_piv = result_1_2_piv.rename(columns={1: 'label_1', 2: 'label_2'})

            result_1_2_piv = result_1_2_piv.reset_index()
            result_1_2_piv['label'] = result_1_2_piv.apply(
                lambda x: 0 if x.label_1 == 0 else (0 if x.label_2 == 0 else 1),
                axis=1)
            result_1_2 = result_1_2.drop('label', axis=1)
            # result_1_2 = result_1_2[result_1_2['data_win'] == 1]
            result_1_2 = result_1_2.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
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
                     "learning_time_hr", "deep_bid_type", "label"])

    if data_ignore.shape[0] != 0:
        data_ignore['label'] = 1
        data_ignore.sort_values('data_win', ascending=False, inplace=True)
        data_ignore = data_ignore.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
        data_ignore['data_win'] = -1

    source_predict = pd.concat([source_predict, data_ignore], axis=0)
    source_predict.sort_values('data_win', ascending=False, inplace=True)
    source_predict = source_predict.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')

    source_predict['label'] = source_predict['label'].astype(int)
    # print(source_predict.columns.values.tolist())
    source_predict = source_predict.fillna('null')
    return {"columns": source_predict.columns.values.tolist(), "list": source_predict.values.tolist()}


"""
帝国模型
"""


# 2021-8-5:双状况计划监控模型——帝国广点通


def source_predict_state_1_dg_gdt(jsondata):
    '''
    :param jsondata: {"columns": ["channel_id","source_id","plan_name", "model_run_datetime","create_time","media_id",
    "game_id","platform","source_run_date_amount","create_role_num","create_role_cost","create_role_pay_num",
    "create_role_pay_cost","create_role_pay_sum","create_role_roi","create_role_pay_rate","create_role_pay_num_cum",
    "learning_type","learning_time_dt","learning_time_hr","deep_bid_type"]}
    :return:'label','budget'
    '''

    # 该模型只能接受计划状态plan_run_state=1且data_win=0 且media_id=16 且 model_run_datetime 等于当日的数据，即只接受当日广点通实时数据
    # label为1时关计划，为0时开计划，如果模型输出label结果与计划当前状况（开/关）相同，则不操作。例如，计划当前是关停状况，当模型输出为1时，不操作

    # 早上7：55分，将所有在运行的计划状态plan_run_state改为2。调用模型2进行判断，是否达标。达标则状态改为1，开计划，预算由模型1计算决定。不达标，则为关停状态，且plan_run_state为2

    data = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])
    data = data[data['media_id'] == 16]
    # 数据预处理
    data = data[data['source_run_date_amount'] > 0]  # 删除消耗为0的数据

    # 对消耗300元以下的不作判断，对数据按平台、win进行分类处理
    data_not_ignore = data[data['source_run_date_amount'] >= 300]

    # 增加每次付费跑法的判断
    #     data_pre_action = data_not_ignore[data_not_ignore['deep_bid_type'] == 'BID_PER_ACTION']
    #     data_not_ignore = data_not_ignore[data_not_ignore['deep_bid_type'] != 'BID_PER_ACTION']

    data_win_0 = data_not_ignore[data_not_ignore['platform'] == 1]
    data_win_0_ios = data_not_ignore[data_not_ignore['platform'] == 2]

    #     data_win_0_pre = data_pre_action[data_pre_action['platform'] == 1]
    #     data_win_0_ios_pre = data_pre_action[data_pre_action['platform'] == 2]

    result_df = pd.DataFrame()

    # 1:关; 0:开； 2：保持原状
    # win=0预判
    if data_win_0.shape[0] != 0:
        data_win_0_1 = data_win_0[data_win_0['source_run_date_amount'] <= 7000]
        data_win_0_2 = data_win_0[
            (data_win_0['source_run_date_amount'] > 7000) & (data_win_0['source_run_date_amount'] <= 13000)]
        data_win_0_3 = data_win_0[
            (data_win_0['source_run_date_amount'] > 13000) & (data_win_0['source_run_date_amount'] <= 25000)]
        data_win_0_4 = data_win_0[
            (data_win_0['source_run_date_amount'] > 25000) & (data_win_0['source_run_date_amount'] <= 50000)]
        data_win_0_5 = data_win_0[data_win_0['source_run_date_amount'] > 50000]
        if data_win_0_1.shape[0] != 0:
            # data_win_0_1['label'] = data_win_0_1.apply(
            #     lambda x: 1 if (x.create_role_cost >= 200) & (x.deep_bid_type == 'BID_PER_ACTION') |
            #                    (x.create_role_cost >= 100) & (x.deep_bid_type != 'BID_PER_ACTION') else (
            #         1 if (x.create_role_roi <= 0.02)
            #              & (x.source_run_date_amount >= 3500) else (0 if (x.create_role_roi >= 0.03) else 2)), axis=1)

            data_win_0_1['label'] = data_win_0_1.apply(
                lambda x: 1 if (((x.create_role_cost >= 500) & (x.source_run_date_amount <= 1300) & (
                            x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 750) & (x.source_run_date_amount <= 1300) & (
                                            x.create_role_pay_sum != 0)) |
                                ((x.create_role_cost >= 400) & (x.source_run_date_amount > 1300) & (
                                            x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 550) & (x.source_run_date_amount > 1300) & (
                                            x.create_role_pay_sum != 0)) |
                                ((x.create_role_roi < 0.01) & (x.source_run_date_amount >= 4500))) else (0
                             if ((( x.create_role_cost <= 480) & (x.source_run_date_amount <= 1300) & (x.create_role_pay_sum == 0)) |
                                 (( x.create_role_cost <= 730) & (x.source_run_date_amount <= 1300) & (x.create_role_pay_sum != 0)) |
                                 (( x.create_role_cost <= 390) & (x.source_run_date_amount > 1300) & (x.create_role_pay_sum == 0)) |
                                 (( x.create_role_cost <= 540) & (x.source_run_date_amount > 1300) & (x.create_role_pay_sum != 0)) |
                                 (x.create_role_roi >= 0.01)) else 2),axis=1)

            # data_win_0_1['label'] = data_win_0_1.apply(
            #     lambda x: 1 if (((x.create_role_cost >= 350) & (x.source_run_date_amount <= 1300) & (x.create_role_pay_sum == 0)) |
            #                     ((x.create_role_cost >= 450) & (x.source_run_date_amount <= 1300) & (x.create_role_pay_sum != 0)) |
            #                     ((x.create_role_cost >= 200) & (x.source_run_date_amount > 1300) & (x.create_role_pay_sum == 0)) |
            #                     ((x.create_role_cost >= 300) & (x.source_run_date_amount > 1300) & (x.create_role_pay_sum != 0)) |
            #                     ((x.create_role_roi < 0.014) & (x.source_run_date_amount >= 4500))) else (0
            #                 if (((x.create_role_cost <= 330) & (x.source_run_date_amount <= 1300) & (x.create_role_pay_sum == 0)) |
            #                     ((x.create_role_cost <= 430) & (x.source_run_date_amount <= 1300) & (x.create_role_pay_sum != 0)) |
            #                     ((x.create_role_cost <= 190) & (x.source_run_date_amount > 1300) & (x.create_role_pay_sum == 0)) |
            #                     ((x.create_role_cost <= 290) & (x.source_run_date_amount > 1300) & (x.create_role_pay_sum != 0)) |
            #                     (x.create_role_roi >= 0.014)) else 2), axis=1)

            result_df = result_df.append(data_win_0_1)

        if data_win_0_2.shape[0] != 0:
            data_win_0_2['label'] = data_win_0_2.apply(
                lambda x: 1 if (x.create_role_cost >= 300) else (1 if (x.create_role_roi <= 0.016)
                                                                      | (x.create_role_pay_cost >= 4000) else (
                    0 if (x.create_role_roi >= 0.017) else 2)), axis=1)
            result_df = result_df.append(data_win_0_2)

        if data_win_0_3.shape[0] != 0:
            data_win_0_3['label'] = data_win_0_3.apply(
                lambda x: 1 if (x.create_role_cost >= 290) else (1 if (x.create_role_roi <= 0.017)
                                                                      | (x.create_role_pay_cost >= 4000) else (
                    0 if (x.create_role_roi >= 0.018) else 2)), axis=1)
            result_df = result_df.append(data_win_0_3)

        if data_win_0_4.shape[0] != 0:
            data_win_0_4['label'] = data_win_0_4.apply(
                lambda x: 1 if (x.create_role_cost >= 280) else (1 if (x.create_role_roi <= 0.018)
                                                                      | (x.create_role_pay_cost >= 3500) else (
                    0 if (x.create_role_roi >= 0.019) else 2)), axis=1)
            result_df = result_df.append(data_win_0_4)

        if data_win_0_5.shape[0] != 0:
            data_win_0_5['label'] = data_win_0_5.apply(
                lambda x: 1 if (x.create_role_cost >= 270) else (1 if (x.create_role_roi <= 0.019)
                                                                      | (x.create_role_pay_cost >= 3500) else (
                    0 if (x.create_role_roi >= 0.02) else 2)), axis=1)
            result_df = result_df.append(data_win_0_5)

    # ISO
    if data_win_0_ios.shape[0] != 0:
        data_win_0_ios_1 = data_win_0_ios[data_win_0_ios['source_run_date_amount'] <= 7000]
        data_win_0_ios_2 = data_win_0_ios[
            (data_win_0_ios['source_run_date_amount'] > 7000) & (data_win_0_ios['source_run_date_amount'] <= 13000)]
        data_win_0_ios_3 = data_win_0_ios[
            (data_win_0_ios['source_run_date_amount'] > 13000) & (data_win_0_ios['source_run_date_amount'] <= 25000)]
        data_win_0_ios_4 = data_win_0_ios[
            (data_win_0_ios['source_run_date_amount'] > 25000) & (data_win_0_ios['source_run_date_amount'] <= 50000)]
        data_win_0_ios_5 = data_win_0_ios[data_win_0_ios['source_run_date_amount'] > 50000]
        if data_win_0_ios_1.shape[0] != 0:
            # data_win_0_ios_1['label'] = data_win_0_ios_1.apply(
            #     lambda x: 1 if (((x.create_role_cost >= 400) & (x.source_run_date_amount <= 1300) & (x.create_role_pay_sum == 0)) |
            #                     ((x.create_role_cost >= 600) & (x.source_run_date_amount <= 1300) & (x.create_role_pay_sum != 0)) |
            #                     ((x.create_role_cost >= 300) & (x.source_run_date_amount > 1300) & (x.create_role_pay_sum == 0)) |
            #                     ((x.create_role_cost >= 500) & (x.source_run_date_amount > 1300) & (x.create_role_pay_sum != 0)) |
            #                     ((x.create_role_roi < 0.013) & (x.source_run_date_amount >= 5000))) else (0
            #               if (((x.create_role_cost <= 380) & (x.source_run_date_amount <= 1300) & (x.create_role_pay_sum == 0)) |
            #                   ((x.create_role_cost <= 580) & (x.source_run_date_amount <= 1300) & (x.create_role_pay_sum != 0)) |
            #                   ((x.create_role_cost <= 280) & (x.source_run_date_amount > 1300) & (x.create_role_pay_sum == 0)) |
            #                   ((x.create_role_cost <= 480) & (x.source_run_date_amount > 1300) & (x.create_role_pay_sum != 0)) |
            #                   (x.create_role_roi >= 0.013)) else 2), axis=1)
            data_win_0_ios_1['label'] = data_win_0_ios_1.apply(
                lambda x: 1 if (((x.create_role_cost >= 600) & (x.source_run_date_amount <= 1300) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 800) & (x.source_run_date_amount <= 1300) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_cost >= 500) & (x.source_run_date_amount > 1300) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 700) & (x.source_run_date_amount > 1300) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_roi < 0.013) & (x.source_run_date_amount >= 5000))) else (0
                          if (((x.create_role_cost <= 580) & (x.source_run_date_amount <= 1300) & (x.create_role_pay_sum == 0)) |
                              ((x.create_role_cost <= 780) & (x.source_run_date_amount <= 1300) & (x.create_role_pay_sum != 0)) |
                              ((x.create_role_cost <= 480) & (x.source_run_date_amount > 1300) & (x.create_role_pay_sum == 0)) |
                              ((x.create_role_cost <= 680) & (x.source_run_date_amount > 1300) & (x.create_role_pay_sum != 0)) |
                              (x.create_role_roi >= 0.013)) else 2), axis=1)

            result_df = result_df.append(data_win_0_ios_1)

        if data_win_0_ios_2.shape[0] != 0:
            data_win_0_ios_2['label'] = data_win_0_ios_2.apply(
                lambda x: 1 if (x.create_role_cost >= 350) else (1 if (x.create_role_roi <= 0.015)| (x.create_role_pay_cost >= 5000) else (
                    0 if (x.create_role_roi >= 0.016) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_2)

        if data_win_0_ios_3.shape[0] != 0:
            data_win_0_ios_3['label'] = data_win_0_ios_3.apply(
                lambda x: 1 if (x.create_role_cost >= 340) else (1 if (x.create_role_roi <= 0.016)
                                                                      | (x.create_role_pay_cost >= 5000) else (
                    0 if (x.create_role_roi >= 0.017) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_3)

        if data_win_0_ios_4.shape[0] != 0:
            data_win_0_ios_4['label'] = data_win_0_ios_4.apply(
                lambda x: 1 if (x.create_role_cost >= 330) else (1 if (x.create_role_roi <= 0.017)
                                                                      | (x.create_role_pay_cost >= 5000) else (
                    0 if (x.create_role_roi >= 0.018) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_4)

        if data_win_0_ios_5.shape[0] != 0:
            data_win_0_ios_5['label'] = data_win_0_ios_5.apply(
                lambda x: 1 if (x.create_role_cost >= 320) else (1 if (x.create_role_roi <= 0.018)
                                                                      | (x.create_role_pay_cost >= 5000) else (
                    0 if (x.create_role_roi >= 0.019) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_5)
    if result_df.shape[0] != 0:
        result_df['label'] = result_df['label'].astype(int)
        result_df['budget'] = 'None'
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


def dg_source_predict_state_1(jsondata):
    '''
    :param jsondata: {"columns": ["channel_id","source_id","plan_name", "model_run_datetime","create_time","media_id",
    "game_id","platform","source_run_date_amount","create_role_num","create_role_cost","create_role_pay_num",
    "create_role_pay_cost","create_role_pay_sum","create_role_roi","create_role_pay_rate","create_role_pay_num_cum",
    "learning_type","learning_time_dt","learning_time_hr","deep_bid_type"]}
    :return:'label','budget'
    '''

    # 该模型只能接受计划状态plan_run_state=1且data_win=0 且 model_run_datetime 等于当日的数据，即只接受当日实时数据
    # label为1时关计划，为0时开计划，如果模型输出label结果与计划当前状况（开/关）相同，则不操作。例如，计划当前是关停状况，当模型输出为1时，不操作
    # budget输出值为该计划预算需要增加至的数值，如果budget小于等于当前计划预算，则不操作。例如budget=6000，则将该计划的预算增加至6000元
    # 加预算操作与开关操作相互独立，例如可以存在加了预算，但不开计划的情况
    # 晚上12点至第二天早上8：00不执行加预算操作，但开关指令正常执行
    # 早上7：55分，将所有在运行的计划状态plan_run_state改为2。调用模型2进行判断，是否达标。达标则状态改为1，开计划，预算由模型1计算决定。不达标，则为关停状态，且plan_run_state为2

    data = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])

    # 数据预处理
    data = data[data['source_run_date_amount'] > 0]  # 删除消耗为0的数据
    # 去重，对当天运行进行监控情况
    # 对消耗300元以下的不作判断，对数据按平台、win进行分类处理
    data_not_ignore = data[data['source_run_date_amount'] >= 300]

    # 增加每次付费跑法的判断
    data_pre_action = data_not_ignore[data_not_ignore['deep_bid_type'] == 'BID_PER_ACTION']
    data_not_ignore = data_not_ignore[data_not_ignore['deep_bid_type'] != 'BID_PER_ACTION']

    data_win_0 = data_not_ignore[data_not_ignore['platform'] == 1]
    data_win_0_ios = data_not_ignore[data_not_ignore['platform'] == 2]

    data_win_0_pre = data_pre_action[data_pre_action['platform'] == 1]
    data_win_0_ios_pre = data_pre_action[data_pre_action['platform'] == 2]

    result_df = pd.DataFrame()

    # 1:关; 0:开； 3：加预算
    # win=0预判
    # 每次付费安卓
    if data_win_0_pre.shape[0] != 0:
        data_win_0_pre['label'] = data_win_0_pre.apply(
            lambda x: 1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount <= 1500) &
                           (x.create_role_cost >= 300) else (
                1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount > 1500) &
                     (x.create_role_cost >= 200) else (
                    1 if (x.create_role_roi > 0) & (x.create_role_roi <= 0.015) & (x.create_role_cost >= 280) else (
                        1 if x.create_role_cost >= 350 else 0))), axis=1)

        result_df = result_df.append(data_win_0_pre)

    # 每次付费ISO
    if data_win_0_ios_pre.shape[0] != 0:
        data_win_0_ios_pre['label'] = data_win_0_ios_pre.apply(
            lambda x: 1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount <= 1500) &
                           (x.create_role_cost >= 800) else (
                1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount > 1500) &
                     (x.create_role_cost >= 650) else (
                    1 if (x.create_role_roi > 0) & (x.create_role_roi <= 0.014) & (x.create_role_cost >= 900) else (
                        1 if x.create_role_cost >= 1000 else 0))), axis=1)

        result_df = result_df.append(data_win_0_ios_pre)

    # 其它付费方式安卓
    if data_win_0.shape[0] != 0:
        data_win_0['label'] = data_win_0.apply(
            lambda x: 1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount <= 1500) &
                           (x.create_role_cost >= 220) else (
                1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount > 1500) &
                     (x.create_role_cost >= 130) else (
                    1 if (x.create_role_roi > 0) & (x.create_role_roi <= 0.015) & (x.create_role_cost >= 200) else (
                        1 if x.create_role_cost >= 300 else 0))), axis=1)

        result_df = result_df.append(data_win_0)

    # 其它付费方式IOS
    if data_win_0_ios.shape[0] != 0:
        data_win_0_ios['label'] = data_win_0_ios.apply(
            lambda x: 1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount <= 1500) &
                           (x.create_role_cost >= 700) else (
                1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount > 1500) &
                     (x.create_role_cost >= 500) else (
                    1 if (x.create_role_roi > 0) & (x.create_role_roi <= 0.014) & (x.create_role_cost >= 800) else (
                        1 if x.create_role_cost >= 900 else 0))), axis=1)

        result_df = result_df.append(data_win_0_ios)

    # 凌晨前关闭消耗大于2000且ROI<0.8%的计划
    d_time = datetime.datetime.strptime(str(datetime.datetime.now().date()) + '23:40', '%Y-%m-%d%H:%M')
    d_time1 = datetime.datetime.strptime(
        str((datetime.datetime.now() + datetime.timedelta(days=1)).date()) + '00:01',
        '%Y-%m-%d%H:%M')
    n_time = datetime.datetime.now()
    if n_time > d_time and n_time < d_time1 and result_df.shape[0] != 0:
        result_df['label'] = result_df.apply(
            lambda x: 1 if (x.source_run_date_amount >= 3000) & (x.create_role_roi <= 0.008) else x.label, axis=1)
    if result_df.shape[0] != 0:
        result_df_an = result_df[result_df['platform'] == 1]
        result_df_ios = result_df[result_df['platform'] == 2]
        result_df = pd.DataFrame()

        # 安卓加预算
        data_1 = result_df_an[result_df_an['source_run_date_amount'] <= 4500]
        data_2 = result_df_an[
            (result_df_an['source_run_date_amount'] > 4500) & (result_df_an['source_run_date_amount'] <= 9200)]
        data_3 = result_df_an[
            (result_df_an['source_run_date_amount'] > 9200) & (result_df_an['source_run_date_amount'] <= 18000)]
        data_4 = result_df_an[
            (result_df_an['source_run_date_amount'] > 18000) & (result_df_an['source_run_date_amount'] <= 40000)]
        data_5 = result_df_an[
            (result_df_an['source_run_date_amount'] > 40000) & (result_df_an['source_run_date_amount'] <= 76000)]
        data_6 = result_df_an[
            (result_df_an['source_run_date_amount'] > 76000) & (result_df_an['source_run_date_amount'] <= 120000)]
        data_7 = result_df_an[
            (result_df_an['source_run_date_amount'] > 120000) & (result_df_an['source_run_date_amount'] <= 190000)]
        data_8 = result_df_an[
            (result_df_an['source_run_date_amount'] > 190000) & (result_df_an['source_run_date_amount'] <= 240000)]
        data_9 = result_df_an[
            (result_df_an['source_run_date_amount'] > 240000) & (result_df_an['source_run_date_amount'] <= 290000)]
        data_10 = result_df_an[result_df_an['source_run_date_amount'] > 290000]

        if data_1.shape[0] != 0:
            data_1['budget'] = data_1.apply(
                lambda x: 6600 if (x.create_role_pay_sum >= 50) & (x.create_role_pay_num >= 2) else
                (6600 if x.create_role_pay_sum >= 78 else np.nan), axis=1)
            result_df = result_df.append(data_1)
        if data_2.shape[0] != 0:
            data_2['budget'] = data_2.apply(
                lambda x: 13000 if (x.create_role_roi >= 0.025) & (x.create_role_pay_num >= 2) else np.nan, axis=1)
            result_df = result_df.append(data_2)
        if data_3.shape[0] != 0:
            data_3['budget'] = data_3.apply(
                lambda x: 23000 if (x.create_role_roi >= 0.027) & (x.create_role_pay_num >= 3) else
                (23000 if x.create_role_roi >= 0.032 else np.nan), axis=1)
            result_df = result_df.append(data_3)
        if data_4.shape[0] != 0:
            data_4['budget'] = data_4.apply(
                lambda x: 50000 if (x.create_role_roi >= 0.029) & (x.create_role_pay_cost <= 4500) else
                (50000 if (x.create_role_roi >= 0.034) & (x.create_role_pay_cost <= 6000) else
                 np.nan), axis=1)
            result_df = result_df.append(data_4)
        if data_5.shape[0] != 0:
            data_5['budget'] = data_5.apply(
                lambda x: 95000 if (x.create_role_roi >= 0.031) & (x.create_role_pay_cost <= 4200) else
                (95000 if (x.create_role_roi >= 0.036) & (x.create_role_pay_cost <= 5500) else
                 np.nan), axis=1)
            result_df = result_df.append(data_5)
        if data_6.shape[0] != 0:
            data_6['budget'] = data_6.apply(
                lambda x: 148000 if (x.create_role_roi >= 0.034) & (x.create_role_pay_cost <= 4000) else
                (148000 if (x.create_role_roi >= 0.042) & (x.create_role_pay_cost <= 3500) else
                 np.nan), axis=1)
            result_df = result_df.append(data_6)
        if data_7.shape[0] != 0:
            data_7['budget'] = data_7.apply(
                lambda x: 220000 if (x.create_role_roi >= 0.035) & (x.create_role_pay_cost <= 4000) else
                (220000 if (x.create_role_roi >= 0.043) & (x.create_role_pay_cost <= 3500) else
                 np.nan), axis=1)
            result_df = result_df.append(data_7)
        if data_8.shape[0] != 0:
            data_8['budget'] = data_8.apply(
                lambda x: 275000 if (x.create_role_roi >= 0.037) & (x.create_role_pay_cost <= 3800) else
                (275000 if (x.create_role_roi >= 0.045) & (x.create_role_pay_cost <= 3300) else
                 np.nan), axis=1)
            result_df = result_df.append(data_8)
        if data_9.shape[0] != 0:
            data_9['budget'] = data_9.apply(
                lambda x: 320000 if (x.create_role_roi >= 0.037) & (x.create_role_pay_cost <= 3600) else
                (320000 if (x.create_role_roi >= 0.045) & (x.create_role_pay_cost <= 3100) else
                 np.nan), axis=1)
            result_df = result_df.append(data_9)
        if data_10.shape[0] != 0:
            data_10['budget'] = data_10.apply(
                lambda x: 450000 if (x.create_role_roi >= 0.037) & (x.create_role_pay_cost <= 3500) else
                (450000 if (x.create_role_roi >= 0.045) & (x.create_role_pay_cost <= 3000) else
                 np.nan), axis=1)
            result_df = result_df.append(data_10)

        # ios加预算
        data_1_ios = result_df_ios[result_df_ios['source_run_date_amount'] <= 5600]
        data_2_ios = result_df_ios[
            (result_df_ios['source_run_date_amount'] > 5600) & (result_df_ios['source_run_date_amount'] <= 9200)]
        data_3_ios = result_df_ios[
            (result_df_ios['source_run_date_amount'] > 9200) & (result_df_ios['source_run_date_amount'] <= 18000)]
        data_4_ios = result_df_ios[
            (result_df_ios['source_run_date_amount'] > 18000) & (result_df_ios['source_run_date_amount'] <= 40000)]
        data_5_ios = result_df_ios[
            (result_df_ios['source_run_date_amount'] > 40000) & (result_df_ios['source_run_date_amount'] <= 76000)]
        data_6_ios = result_df_ios[
            (result_df_ios['source_run_date_amount'] > 76000) & (result_df_ios['source_run_date_amount'] <= 120000)]
        data_7_ios = result_df_ios[
            (result_df_ios['source_run_date_amount'] > 120000) & (result_df_ios['source_run_date_amount'] <= 190000)]
        data_8_ios = result_df_ios[
            (result_df_ios['source_run_date_amount'] > 190000) & (result_df_ios['source_run_date_amount'] <= 240000)]
        data_9_ios = result_df_ios[
            (result_df_ios['source_run_date_amount'] > 240000) & (result_df_ios['source_run_date_amount'] <= 290000)]
        data_10_ios = result_df_ios[result_df_ios['source_run_date_amount'] > 290000]

        if data_1_ios.shape[0] != 0:
            data_1_ios['budget'] = data_1_ios.apply(
                lambda x: 8000 if (x.create_role_pay_sum >= 48) & (x.create_role_pay_num >= 2) else
                (8000 if x.create_role_pay_sum >= 68 else np.nan), axis=1)
            result_df = result_df.append(data_1_ios)
        if data_2_ios.shape[0] != 0:
            # data_2_ios['budget'] = data_2_ios.apply(
            #     lambda x: 13000 if (x.create_role_roi >= 0.028) & (x.create_role_pay_num >= 2) else
            #     (13000 if x.create_role_roi >= 0.04 else np.nan), axis=1)
            data_2_ios['budget'] = data_2_ios.apply(
                lambda x: 13000 if (x.create_role_roi >= 0.021) & (x.create_role_pay_num >= 2) else np.nan, axis=1)
            result_df = result_df.append(data_2_ios)
        if data_3_ios.shape[0] != 0:
            # data_3_ios['budget'] = data_3_ios.apply(
            #     lambda x: 23000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_num >= 2) else
            #     np.nan, axis=1)
            data_3_ios['budget'] = data_3_ios.apply(
                lambda x: 23000 if (x.create_role_roi >= 0.023) & (x.create_role_pay_num >= 3) else
                (23000 if x.create_role_roi >= 0.032 else np.nan), axis=1)
            result_df = result_df.append(data_3_ios)
        if data_4_ios.shape[0] != 0:
            data_4_ios['budget'] = data_4_ios.apply(
                lambda x: 50000 if (x.create_role_roi >= 0.024) & (x.create_role_pay_cost <= 6000) else
                (50000 if (x.create_role_roi >= 0.035) & (x.create_role_pay_cost <= 7500) else
                 np.nan), axis=1)
            result_df = result_df.append(data_4_ios)
        if data_5_ios.shape[0] != 0:
            data_5_ios['budget'] = data_5_ios.apply(
                lambda x: 95000 if (x.create_role_roi >= 0.026) & (x.create_role_pay_cost <= 5500) else
                (95000 if (x.create_role_roi >= 0.036) & (x.create_role_pay_cost <= 7000) else
                 np.nan), axis=1)
            result_df = result_df.append(data_5_ios)
        if data_6_ios.shape[0] != 0:
            data_6_ios['budget'] = data_6_ios.apply(
                lambda x: 148000 if (x.create_role_roi >= 0.027) & (x.create_role_pay_cost <= 5000) else
                (148000 if (x.create_role_roi >= 0.039) & (x.create_role_pay_cost <= 6500) else
                 np.nan), axis=1)
            result_df = result_df.append(data_6_ios)
        if data_7_ios.shape[0] != 0:
            data_7_ios['budget'] = data_7_ios.apply(
                lambda x: 220000 if (x.create_role_roi >= 0.029) & (x.create_role_pay_cost <= 4500) else
                (220000 if (x.create_role_roi >= 0.04) & (x.create_role_pay_cost <= 6000) else
                 np.nan), axis=1)
            result_df = result_df.append(data_7_ios)
        if data_8_ios.shape[0] != 0:
            data_8_ios['budget'] = data_8_ios.apply(
                lambda x: 275000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_cost <= 4500) else
                (275000 if (x.create_role_roi >= 0.04) & (x.create_role_pay_cost <= 6000) else
                 np.nan), axis=1)
            result_df = result_df.append(data_8_ios)
        if data_9_ios.shape[0] != 0:
            data_9_ios['budget'] = data_9_ios.apply(
                lambda x: 320000 if (x.create_role_roi >= 0.031) & (x.create_role_pay_cost <= 4500) else
                (320000 if (x.create_role_roi >= 0.041) & (x.create_role_pay_cost <= 6000) else
                 np.nan), axis=1)
            result_df = result_df.append(data_9_ios)
        if data_10_ios.shape[0] != 0:
            data_10_ios['budget'] = data_10_ios.apply(
                lambda x: 450000 if (x.create_role_roi >= 0.032) & (x.create_role_pay_cost <= 4000) else
                (450000 if (x.create_role_roi >= 0.042) & (x.create_role_pay_cost <= 5500) else
                 np.nan), axis=1)
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


def dg_source_predict_state_2(jsondata):
    '''
    :param jsondata: {"columns": ["channel_id","source_id","plan_name","model_run_datetime","create_time",
    "media_id","game_id","platform","data_win","source_run_date_amount","create_role_num",
    "create_role_cost","create_role_pay_num","create_role_pay_cost","create_role_pay_sum","create_role_roi",
    "create_role_retain_1d","create_role_pay_rate","create_role_pay_num_cum","learning_type","learning_time_dt",
    "learning_time_hr","deep_bid_type"]}
    :return:
    '''
    data = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])

    # 数据预处理
    data = data[data['source_run_date_amount'] > 0]  # 删除消耗为0的数据
    # 去重，处理某天没有消耗的情况
    data.sort_values(by='data_win', inplace=True)
    data.drop_duplicates(subset=['channel_id', 'source_id', 'source_run_date_amount', 'create_role_num'], keep='first'
                         , inplace=True)
    # 对消耗300元以下的不作判断，对数据按平台、win进行分类处理
    data_not_ignore = data[(data['source_run_date_amount'] >= 300) | (data['data_win'] == 1)]
    data_ignore = data.drop(data_not_ignore.index)

    data_win_1 = data_not_ignore[(data_not_ignore['data_win'] == 1) & (data_not_ignore['platform'] == 1)]
    data_win_2 = data_not_ignore[(data_not_ignore['data_win'] == 2) & (data_not_ignore['platform'] == 1)]
    data_win_3 = data_not_ignore[(data_not_ignore['data_win'] == 3) & (data_not_ignore['platform'] == 1)]

    data_win_1_ios = data_not_ignore[(data_not_ignore['data_win'] == 1) & (data_not_ignore['platform'] == 2)]
    data_win_2_ios = data_not_ignore[(data_not_ignore['data_win'] == 2) & (data_not_ignore['platform'] == 2)]
    data_win_3_ios = data_not_ignore[(data_not_ignore['data_win'] == 3) & (data_not_ignore['platform'] == 2)]

    # win=1预判
    result_win1 = pd.DataFrame()
    temp_win1 = pd.DataFrame()

    # data_win_1中再分deep_bid_type
    data_win_1_pre_action = data_win_1[data_win_1['deep_bid_type'] == 'BID_PER_ACTION']
    data_win_1 = data_win_1[data_win_1['deep_bid_type'] != 'BID_PER_ACTION']

    if data_win_1_pre_action.shape[0] != 0:
        data_win_1_pre_action['label'] = data_win_1_pre_action.apply(lambda x: 1 if x.create_role_num == 0 else (1 if
                                                                                                                 x.create_role_roi <= 0.01 else (
            0 if (x.create_role_pay_num > 1) & (
                    x.create_role_roi >= 0.012) & (x.source_run_date_amount >= 500) else (
                0 if (x.create_role_roi >= 0.013) & (x.source_run_date_amount >= 500) else 1 if
                (x.create_role_pay_cost > 6000) | (x.create_role_cost > 400) else 2))), axis=1)

        result_win1 = result_win1.append(
            data_win_1_pre_action[(data_win_1_pre_action['label'] == 1) | (data_win_1_pre_action['label'] == 0)])
        temp_win1 = temp_win1.append(data_win_1_pre_action[data_win_1_pre_action['label'] == 2])

    if data_win_1.shape[0] != 0:
        data_win_1['label'] = data_win_1.apply(lambda x: 1 if x.create_role_num == 0 else (1 if
                                                                                           x.create_role_roi <= 0.01 else (
            0 if (x.create_role_pay_num > 1) & (
                    x.create_role_roi >= 0.012) & (x.source_run_date_amount >= 500) else (
                0 if (x.create_role_roi >= 0.013) & (x.source_run_date_amount >= 500) else 1 if
                (x.create_role_pay_cost > 6000) | (x.create_role_cost > 280) else 2))), axis=1)

        result_win1 = result_win1.append(data_win_1[(data_win_1['label'] == 1) | (data_win_1['label'] == 0)])
        temp_win1 = temp_win1.append(data_win_1[data_win_1['label'] == 2])

    data_win_1_ios_pre_action = data_win_1_ios[data_win_1_ios['deep_bid_type'] == 'BID_PER_ACTION']
    data_win_1_ios = data_win_1_ios[data_win_1_ios['deep_bid_type'] != 'BID_PER_ACTION']

    if data_win_1_ios_pre_action.shape[0] != 0:
        data_win_1_ios_pre_action['label'] = data_win_1_ios_pre_action.apply(
            lambda x: 1 if x.create_role_num == 0 else (1 if
                                                        x.create_role_roi <= 0.01 else (
                0 if (x.create_role_pay_num > 1) & (
                        x.create_role_roi >= 0.012) & (x.source_run_date_amount >= 500) else (
                    0 if (x.create_role_roi >= 0.013) & (x.source_run_date_amount >= 500) else 1 if
                    (x.create_role_pay_cost > 8000) | (x.create_role_cost > 500) else 2))), axis=1)

        result_win1 = result_win1.append(
            data_win_1_ios_pre_action[
                (data_win_1_ios_pre_action['label'] == 1) | (data_win_1_ios_pre_action['label'] == 0)])
        temp_win1 = temp_win1.append(data_win_1_ios_pre_action[data_win_1_ios_pre_action['label'] == 2])

    if data_win_1_ios.shape[0] != 0:
        data_win_1_ios['label'] = data_win_1_ios.apply(lambda x: 1 if x.create_role_num == 0 else (1 if
                                                                                                   x.create_role_roi <= 0.01 else (
            0 if (x.create_role_pay_num > 1) & (
                    x.create_role_roi >= 0.012) & (x.source_run_date_amount >= 500) else (
                0 if (x.create_role_roi >= 0.013) & (x.source_run_date_amount >= 500) else 1 if
                (x.create_role_pay_cost > 8000) | (x.create_role_cost > 400) else 2))), axis=1)

        result_win1 = result_win1.append(
            data_win_1_ios[(data_win_1_ios['label'] == 1) | (data_win_1_ios['label'] == 0)])
        temp_win1 = temp_win1.append(data_win_1_ios[data_win_1_ios['label'] == 2])

    # win=1 模型预测
    if temp_win1.shape[0] != 0:
        temp_win1['label'] = gbdt_b_win1.predict(temp_win1[['create_role_cost',
                                                            'create_role_pay_cost', 'create_role_roi',
                                                            'create_role_pay_rate']])
    result_win1_data = pd.concat([result_win1, temp_win1], axis=0)

    # win=2预判
    result_win2 = pd.DataFrame()
    temp_win2 = pd.DataFrame()
    if data_win_2.shape[0] != 0:
        data_win_2['label'] = data_win_2.apply(lambda x: 1 if x.create_role_num == 0 else (1 if
                                                                                           x.create_role_roi <= 0.025 else (
            0 if (x.create_role_pay_num > 1) & (
                    x.create_role_roi >= 0.026) & (x.source_run_date_amount >= 500) else (
                0 if (x.create_role_roi >= 0.027) & (x.source_run_date_amount >= 500) else 1 if
                (x.create_role_pay_cost > 6000) | (x.create_role_cost > 300) else 2))), axis=1)

        result_win2 = result_win2.append(data_win_2[(data_win_2['label'] == 1) | (data_win_2['label'] == 0)])
        temp_win2 = temp_win2.append(data_win_2[data_win_2['label'] == 2])

    if data_win_2_ios.shape[0] != 0:
        data_win_2_ios['label'] = data_win_2_ios.apply(lambda x: 1 if x.create_role_num == 0 else (1 if
                                                                                                   x.create_role_roi <= 0.025 else (
            0 if (x.create_role_pay_num > 1) & (
                    x.create_role_roi >= 0.026) & (x.source_run_date_amount >= 500) else (
                0 if (x.create_role_roi >= 0.027) & (x.source_run_date_amount >= 500) else 1 if
                (x.create_role_pay_cost > 8000) | (x.create_role_cost > 400) else 2))), axis=1)

        result_win2 = result_win2.append(data_win_2_ios[(data_win_2_ios['label'] == 1) | (data_win_2_ios['label'] == 0)])
        temp_win2 = temp_win2.append(data_win_2_ios[data_win_2_ios['label'] == 2])

    # win=2 模型预测
    if temp_win2.shape[0] != 0:
        temp_win2['label'] = gbdt_b_win2.predict(temp_win2[['create_role_cost',
                                                            'create_role_pay_cost', 'create_role_roi',
                                                            'create_role_pay_rate', 'create_role_retain_1d']])
    result_win2_data = pd.concat([result_win2, temp_win2], axis=0)

    # win=3预判
    result_win3 = pd.DataFrame()
    temp_win3 = pd.DataFrame()
    if data_win_3.shape[0] != 0:
        data_win_3['label'] = data_win_3.apply(lambda x: 1 if x.create_role_num == 0 else (1 if
                                                                                           x.create_role_roi <= 0.036 else (
            0 if (x.create_role_pay_num > 1) & (
                    x.create_role_roi >= 0.037) & (x.source_run_date_amount >= 500) else (
                0 if (x.create_role_roi >= 0.038) & (x.source_run_date_amount >= 500) else 1 if
                (x.create_role_pay_cost > 6000) | (x.create_role_cost > 300) else 2))), axis=1)

        result_win3 = result_win3.append(data_win_3[(data_win_3['label'] == 1) | (data_win_3['label'] == 0)])
        temp_win3 = temp_win3.append(data_win_3[data_win_3['label'] == 2])

    if data_win_3_ios.shape[0] != 0:
        data_win_3_ios['label'] = data_win_3_ios.apply(lambda x: 1 if x.create_role_num == 0 else (1 if
                                                                                                   x.create_role_roi <= 0.036 else (
            0 if (x.create_role_pay_num > 1) & (
                    x.create_role_roi >= 0.037) & (x.source_run_date_amount >= 500) else (
                0 if (x.create_role_roi >= 0.038) & (x.source_run_date_amount >= 500) else 1 if
                (x.create_role_pay_cost > 8000) | (x.create_role_cost > 400) else 2))), axis=1)

        result_win3 = result_win3.append(
            data_win_3_ios[(data_win_3_ios['label'] == 1) | (data_win_3_ios['label'] == 0)])
        temp_win3 = temp_win3.append(data_win_3_ios[data_win_3_ios['label'] == 2])

    # win=3 模型预测
    if temp_win3.shape[0] != 0:
        temp_win3['label'] = gbdt_b_win3.predict(temp_win3[['create_role_cost',
                                                            'create_role_pay_cost', 'create_role_roi',
                                                            'create_role_pay_rate', 'create_role_retain_1d']])
    result_win3_data = pd.concat([result_win3, temp_win3], axis=0)

    result = pd.concat([result_win1_data, result_win2_data, result_win3_data], axis=0)

    # 结果输出
    if result.shape[0] != 0:
        result_1_2_3 = result[result['data_win'] != 7]
        # win1,2,3按优先级3,2,1进行去重
        if result_1_2_3.shape[0] != 0:
            result_1_2_3.sort_values('data_win', ascending=False, inplace=True)
            result_1_2_3.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first', inplace=True)

        # # 再与win=7进行去重，win=7优先级小
        # result_all = pd.concat([result_1_2_3, result[result['data_win'] == 7]], axis=0)
        #
        # result_all.sort_values('data_win', inplace=True)
        source_predict = result_1_2_3.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')

    else:
        source_predict = pd.DataFrame(
            columns=["channel_id", "source_id", "plan_name", "model_run_datetime", "create_time",
                     "media_id", "game_id", "platform", "data_win", "source_run_date_amount", "create_role_num",
                     "create_role_cost", "create_role_pay_num", "create_role_pay_cost", "create_role_pay_sum",
                     "create_role_roi",
                     "create_role_retain_1d", "create_role_pay_rate", "create_role_pay_num_cum", "learning_type",
                     "learning_time_dt",
                     "learning_time_hr", "deep_bid_type", "label"])

    if data_ignore.shape[0] != 0:
        data_ignore['label'] = 1
        data_ignore.sort_values('data_win', ascending=False, inplace=True)
        data_ignore = data_ignore.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
        data_ignore['data_win'] = -1

    source_predict = pd.concat([source_predict, data_ignore], axis=0)
    source_predict.sort_values('data_win', ascending=False, inplace=True)
    source_predict = source_predict.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')

    source_predict['label'] = source_predict['label'].astype(int)
    # print(source_predict.columns.values.tolist())
    source_predict = source_predict.fillna('null')
    return {"columns": source_predict.columns.values.tolist(), "list": source_predict.values.tolist()}


"""
金牌模型  ## TODO
"""


def source_predict_state_1_jp_gdt(jsondata, ):
    """ 金牌：模型1"""
    '''
    :param jsondata: {"columns": ["channel_id","source_id","plan_name", "model_run_datetime","create_time","media_id",
    "game_id","platform","source_run_date_amount","create_role_num","create_role_cost","create_role_pay_num",
    "create_role_pay_cost","create_role_pay_sum","create_role_roi","create_role_pay_rate","create_role_pay_num_cum",
    "learning_type","learning_time_dt","learning_time_hr","deep_bid_type"]}
    :return:'label','budget'
    '''
    data = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])
    data = data[data['media_id'] == 16]
    # 该模型只能接受计划状态plan_run_state=1且data_win=0 且media_id=16 且 model_run_datetime 等于当日的数据，即只接受当日广点通实时数据
    # label为1时关计划，为0时开计划，如果模型输出label结果与计划当前状况（开/关）相同，则不操作。例如，计划当前是关停状况，当模型输出为1时，不操作
    # 早上7：55分，将所有在运行的计划状态plan_run_state改为2。调用模型2进行判断，是否达标。达标则状态改为1，开计划，预算由模型1计算决定。不达标，则为关停状态，且plan_run_state为2

    # 数据预处理
    data = data[data['source_run_date_amount'] > 0]  # 删除消耗为0的数据

    # 对消耗300元以下的不作判断，对数据按平台、win进行分类处理
    data_not_ignore = data[data['source_run_date_amount'] >= 300]

    # 增加每次付费跑法的判断
    #     data_pre_action = data_not_ignore[data_not_ignore['deep_bid_type'] == 'BID_PER_ACTION']
    #     data_not_ignore = data_not_ignore[data_not_ignore['deep_bid_type'] != 'BID_PER_ACTION']

    data_win_0 = data_not_ignore[data_not_ignore['platform'] == 1]
    data_win_0_ios = data_not_ignore[data_not_ignore['platform'] == 2]

    #     data_win_0_pre = data_pre_action[data_pre_action['platform'] == 1]
    #     data_win_0_ios_pre = data_pre_action[data_pre_action['platform'] == 2]

    result_df = pd.DataFrame()

    # 1:关; 0:开; 2：保持原状
    # =============== 安卓（暂未修改）
    if data_win_0.shape[0] != 0:
        data_win_0_1 = data_win_0[data_win_0['source_run_date_amount'] <= 7000]
        data_win_0_2 = data_win_0[
            (data_win_0['source_run_date_amount'] > 7000) & (data_win_0['source_run_date_amount'] <= 13000)]
        data_win_0_3 = data_win_0[
            (data_win_0['source_run_date_amount'] > 13000) & (data_win_0['source_run_date_amount'] <= 25000)]
        data_win_0_4 = data_win_0[
            (data_win_0['source_run_date_amount'] > 25000) & (data_win_0['source_run_date_amount'] <= 50000)]
        data_win_0_5 = data_win_0[data_win_0['source_run_date_amount'] > 50000]

        if data_win_0_1.shape[0] != 0:
            data_win_0_1['label'] = data_win_0_1.apply(
                lambda x: 1 if (((x.create_role_cost >= 350) & (x.source_run_date_amount <= 1300) & (
                            x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 450) & (x.source_run_date_amount <= 1300) & (
                                            x.create_role_pay_sum != 0)) |
                                ((x.create_role_cost >= 200) & (x.source_run_date_amount > 1300) & (
                                            x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 300) & (x.source_run_date_amount > 1300) & (
                                            x.create_role_pay_sum != 0)) |
                                ((x.create_role_roi < 0.014) & (x.source_run_date_amount >= 4500))) else (0
                                                                                                          if (((
                                                                                                                           x.create_role_cost <= 330) & (
                                                                                                                           x.source_run_date_amount <= 1300) & (
                                                                                                                           x.create_role_pay_sum == 0)) |
                                                                                                              ((
                                                                                                                           x.create_role_cost <= 430) & (
                                                                                                                           x.source_run_date_amount <= 1300) & (
                                                                                                                           x.create_role_pay_sum != 0)) |
                                                                                                              ((
                                                                                                                           x.create_role_cost <= 190) & (
                                                                                                                           x.source_run_date_amount > 1300) & (
                                                                                                                           x.create_role_pay_sum == 0)) |
                                                                                                              ((
                                                                                                                           x.create_role_cost <= 290) & (
                                                                                                                           x.source_run_date_amount > 1300) & (
                                                                                                                           x.create_role_pay_sum != 0)) |
                                                                                                              (
                                                                                                                          x.create_role_roi >= 0.014)) else 2),
                axis=1)

            result_df = result_df.append(data_win_0_1)

        if data_win_0_2.shape[0] != 0:
            data_win_0_2['label'] = data_win_0_2.apply(
                lambda x: 1 if (x.create_role_cost >= 280) else (1 if (x.create_role_roi <= 0.016)
                                                                      | (x.create_role_pay_cost >= 4000) else (
                    0 if (x.create_role_roi >= 0.017) else 2)), axis=1)
            result_df = result_df.append(data_win_0_2)

        if data_win_0_3.shape[0] != 0:
            data_win_0_3['label'] = data_win_0_3.apply(
                lambda x: 1 if (x.create_role_cost >= 270) else (1 if (x.create_role_roi <= 0.017)
                                                                      | (x.create_role_pay_cost >= 4000) else (
                    0 if (x.create_role_roi >= 0.018) else 2)), axis=1)
            result_df = result_df.append(data_win_0_3)

        if data_win_0_4.shape[0] != 0:
            data_win_0_4['label'] = data_win_0_4.apply(
                lambda x: 1 if (x.create_role_cost >= 260) else (1 if (x.create_role_roi <= 0.018)
                                                                      | (x.create_role_pay_cost >= 3500) else (
                    0 if (x.create_role_roi >= 0.019) else 2)), axis=1)
            result_df = result_df.append(data_win_0_4)

        if data_win_0_5.shape[0] != 0:
            data_win_0_5['label'] = data_win_0_5.apply(
                lambda x: 1 if (x.create_role_cost >= 250) else (1 if (x.create_role_roi <= 0.019)
                                                                      | (x.create_role_pay_cost >= 3500) else (
                    0 if (x.create_role_roi >= 0.02) else 2)), axis=1)
            result_df = result_df.append(data_win_0_5)

    # =============== IOS
    ## TODO参数：消耗分组（保持）
    if data_win_0_ios.shape[0] != 0:
        data_win_0_ios_1 = data_win_0_ios[data_win_0_ios['source_run_date_amount'] <= 7000]
        data_win_0_ios_2 = data_win_0_ios[
            (data_win_0_ios['source_run_date_amount'] > 7000) & (data_win_0_ios['source_run_date_amount'] <= 13000)]
        data_win_0_ios_3 = data_win_0_ios[
            (data_win_0_ios['source_run_date_amount'] > 13000) & (data_win_0_ios['source_run_date_amount'] <= 25000)]
        data_win_0_ios_4 = data_win_0_ios[
            (data_win_0_ios['source_run_date_amount'] > 25000) & (data_win_0_ios['source_run_date_amount'] <= 50000)]
        data_win_0_ios_5 = data_win_0_ios[data_win_0_ios['source_run_date_amount'] > 50000]

        ## TODO参数：分段数据内的创角成本分组阈值（修改）
        if data_win_0_ios_1.shape[0] != 0:
            data_win_0_ios_1['label'] = data_win_0_ios_1.apply(
                lambda x: 1 if (((x.create_role_cost >= 350) & (x.source_run_date_amount <= 1500) & (
                            x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 420) & (x.source_run_date_amount <= 1500) & (
                                            x.create_role_pay_sum != 0)) |
                                ((x.create_role_cost >= 250) & (x.source_run_date_amount > 1500) & (
                                            x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 300) & (x.source_run_date_amount > 1500) & (
                                            x.create_role_pay_sum != 0)) |
                                ((x.create_role_roi < 0.01) & (x.source_run_date_amount >= 5000))) else (0
                                                                                                         if (((
                                                                                                                          x.create_role_cost <= 320) & (
                                                                                                                          x.source_run_date_amount <= 1500) & (
                                                                                                                          x.create_role_pay_sum == 0)) |
                                                                                                             ((
                                                                                                                          x.create_role_cost <= 380) & (
                                                                                                                          x.source_run_date_amount <= 1500) & (
                                                                                                                          x.create_role_pay_sum != 0)) |
                                                                                                             ((
                                                                                                                          x.create_role_cost <= 200) & (
                                                                                                                          x.source_run_date_amount > 1500) & (
                                                                                                                          x.create_role_pay_sum == 0)) |
                                                                                                             ((
                                                                                                                          x.create_role_cost <= 280) & (
                                                                                                                          x.source_run_date_amount > 1500) & (
                                                                                                                          x.create_role_pay_sum != 0)) |
                                                                                                             (
                                                                                                                         x.create_role_roi >= 0.01)) else 2),
                axis=1)
            result_df = result_df.append(data_win_0_ios_1)

        ## TODO参数：分段数据内的3指标分组阈值（修改）
        if data_win_0_ios_2.shape[0] != 0:
            data_win_0_ios_2['label'] = data_win_0_ios_2.apply(
                lambda x: 1 if (x.create_role_cost >= 300) else (1 if (x.create_role_roi <= 0.010)
                                                                      | (x.create_role_pay_cost >= 6000) else (
                    0 if (x.create_role_roi >= 0.013) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_2)

        ## TODO参数：分段数据内的3指标分组阈值（修改）
        if data_win_0_ios_3.shape[0] != 0:
            data_win_0_ios_3['label'] = data_win_0_ios_3.apply(
                lambda x: 1 if (x.create_role_cost >= 280) else (1 if (x.create_role_roi <= 0.012)
                                                                      | (x.create_role_pay_cost >= 5000) else (
                    0 if (x.create_role_roi >= 0.015) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_3)

        ## TODO参数：分段数据内的3指标分组阈值（修改）
        if data_win_0_ios_4.shape[0] != 0:
            data_win_0_ios_4['label'] = data_win_0_ios_4.apply(
                lambda x: 1 if (x.create_role_cost >= 260) else (1 if (x.create_role_roi <= 0.015)
                                                                      | (x.create_role_pay_cost >= 3000) else (
                    0 if (x.create_role_roi >= 0.018) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_4)

        ## TODO参数：分段数据内的3指标分组阈值（修改）
        if data_win_0_ios_5.shape[0] != 0:
            data_win_0_ios_5['label'] = data_win_0_ios_5.apply(
                lambda x: 1 if (x.create_role_cost >= 240) else (1 if (x.create_role_roi <= 0.018)
                                                                      | (x.create_role_pay_cost >= 1700) else (
                    0 if (x.create_role_roi >= 0.02) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_5)
    if result_df.shape[0] != 0:
        result_df['label'] = result_df['label'].astype(int)
        result_df['budget'] = 'None'
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


def jp_source_predict_state_2(jsondata, ):
    """ 金牌：模型2（广点通、头条共用）"""
    '''
    :param jsondata: {"columns": ["channel_id","source_id","plan_name","model_run_datetime","create_time",
    "media_id","game_id","platform","data_win","source_run_date_amount","create_role_num",
    "create_role_cost","create_role_pay_num","create_role_pay_cost","create_role_pay_sum","create_role_roi",
    "create_role_retain_1d","create_role_pay_rate","create_role_pay_num_cum","learning_type","learning_time_dt",
    "learning_time_hr","deep_bid_type"]}
    :return:
    '''
    data = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])
    data = data[data['media_id'] == 16]

    # 数据预处理
    data = data[data['source_run_date_amount'] > 0]  # 删除消耗为0的数据
    # 去重，处理某天没有消耗的情况
    data.sort_values(by='data_win', inplace=True)
    data.drop_duplicates(subset=['channel_id', 'source_id', 'source_run_date_amount', 'create_role_num'], keep='first'
                         , inplace=True)
    # 对消耗300元以下的不作判断，对数据按平台、win进行分类处理
    data_not_ignore = data[(data['source_run_date_amount'] >= 300) | (data['data_win'] == 1)]
    data_ignore = data.drop(data_not_ignore.index)

    data_win_1 = data_not_ignore[(data_not_ignore['data_win'] == 1) & (data_not_ignore['platform'] == 1)]
    data_win_2 = data_not_ignore[(data_not_ignore['data_win'] == 2) & (data_not_ignore['platform'] == 1)]
    data_win_3 = data_not_ignore[(data_not_ignore['data_win'] == 3) & (data_not_ignore['platform'] == 1)]

    data_win_1_ios = data_not_ignore[(data_not_ignore['data_win'] == 1) & (data_not_ignore['platform'] == 2)]
    data_win_2_ios = data_not_ignore[(data_not_ignore['data_win'] == 2) & (data_not_ignore['platform'] == 2)]
    data_win_3_ios = data_not_ignore[(data_not_ignore['data_win'] == 3) & (data_not_ignore['platform'] == 2)]

    # win=1预判
    result_win1 = pd.DataFrame()
    temp_win1 = pd.DataFrame()

    # data_win_1中再分deep_bid_type
    data_win_1_pre_action = data_win_1[data_win_1['deep_bid_type'] == 'BID_PER_ACTION']  ## BID_STRATEGY_AVERAGE_COST
    data_win_1 = data_win_1[data_win_1['deep_bid_type'] != 'BID_PER_ACTION']  ## BID_STRATEGY_TARGET_COST

    if data_win_1_pre_action.shape[0] != 0:
        data_win_1_pre_action['label'] = data_win_1_pre_action.apply(lambda x: 1 if x.create_role_num == 0 else (1 if
                                                                                                                 x.create_role_roi <= 0.012 else (
            0 if (x.create_role_pay_num > 1) & (
                    x.create_role_roi >= 0.014) & (x.source_run_date_amount >= 500) else (
                0 if (x.create_role_roi >= 0.020) & (x.source_run_date_amount >= 500) else 1 if
                (x.create_role_pay_cost > 6000) | (x.create_role_cost > 250) else 2))), axis=1)

        result_win1 = result_win1.append(
            data_win_1_pre_action[(data_win_1_pre_action['label'] == 1) | (data_win_1_pre_action['label'] == 0)])
        temp_win1 = temp_win1.append(data_win_1_pre_action[data_win_1_pre_action['label'] == 2])

    if data_win_1.shape[0] != 0:
        data_win_1['label'] = data_win_1.apply(lambda x: 1 if x.create_role_num == 0 else (1 if
                                                                                           x.create_role_roi <= 0.014 else (
            0 if (x.create_role_pay_num > 1) & (
                    x.create_role_roi >= 0.016) & (x.source_run_date_amount >= 500) else (
                0 if (x.create_role_roi >= 0.022) & (x.source_run_date_amount >= 500) else 1 if
                (x.create_role_pay_cost > 6000) | (x.create_role_cost > 200) else 2))), axis=1)

        result_win1 = result_win1.append(data_win_1[(data_win_1['label'] == 1) | (data_win_1['label'] == 0)])
        temp_win1 = temp_win1.append(data_win_1[data_win_1['label'] == 2])

    data_win_1_ios_pre_action = data_win_1_ios[data_win_1_ios['deep_bid_type'] == 'BID_PER_ACTION']
    data_win_1_ios = data_win_1_ios[data_win_1_ios['deep_bid_type'] != 'BID_PER_ACTION']

    if data_win_1_ios_pre_action.shape[0] != 0:
        data_win_1_ios_pre_action['label'] = data_win_1_ios_pre_action.apply(
            lambda x: 1 if x.create_role_num == 0 else (1 if
                                                        x.create_role_roi <= 0.012 else (
                0 if (x.create_role_pay_num > 1) & (
                        x.create_role_roi >= 0.014) & (x.source_run_date_amount >= 500) else (
                    0 if (x.create_role_roi >= 0.020) & (x.source_run_date_amount >= 500) else 1 if
                    (x.create_role_pay_cost > 10000) | (x.create_role_cost > 300) else 2))), axis=1)

        result_win1 = result_win1.append(
            data_win_1_ios_pre_action[
                (data_win_1_ios_pre_action['label'] == 1) | (data_win_1_ios_pre_action['label'] == 0)])
        temp_win1 = temp_win1.append(data_win_1_ios_pre_action[data_win_1_ios_pre_action['label'] == 2])

    if data_win_1_ios.shape[0] != 0:
        data_win_1_ios['label'] = data_win_1_ios.apply(lambda x: 1 if x.create_role_num == 0 else (1 if
                                                                                                   x.create_role_roi <= 0.016 else (
            0 if (x.create_role_pay_num > 1) & (
                    x.create_role_roi >= 0.018) & (x.source_run_date_amount >= 500) else (
                0 if (x.create_role_roi >= 0.024) & (x.source_run_date_amount >= 500) else 1 if
                (x.create_role_pay_cost > 10000) | (x.create_role_cost > 250) else 2))), axis=1)

        result_win1 = result_win1.append(
            data_win_1_ios[(data_win_1_ios['label'] == 1) | (data_win_1_ios['label'] == 0)])
        temp_win1 = temp_win1.append(data_win_1_ios[data_win_1_ios['label'] == 2])

    # win=1 模型预测
    if temp_win1.shape[0] != 0:
        temp_win1['label'] = gbdt_b_win1.predict(temp_win1[['create_role_cost',
                                                            'create_role_pay_cost', 'create_role_roi',
                                                            'create_role_pay_rate']])
    result_win1_data = pd.concat([result_win1, temp_win1], axis=0)

    # win=2预判
    result_win2 = pd.DataFrame()
    temp_win2 = pd.DataFrame()
    if data_win_2.shape[0] != 0:
        data_win_2['label'] = data_win_2.apply(lambda x: 1 if x.create_role_num == 0 else (1 if
                                                                                           x.create_role_roi <= 0.035 else (
            0 if (x.create_role_pay_num > 1) & (
                    x.create_role_roi >= 0.038) & (x.source_run_date_amount >= 500) else (
                0 if (x.create_role_roi >= 0.042) & (x.source_run_date_amount >= 500) else 1 if
                (x.create_role_pay_cost > 7000) | (x.create_role_cost > 150) else 2))), axis=1)

        result_win2 = result_win2.append(data_win_2[(data_win_2['label'] == 1) | (data_win_2['label'] == 0)])
        temp_win2 = temp_win2.append(data_win_2[data_win_2['label'] == 2])

    if data_win_2_ios.shape[0] != 0:
        data_win_2_ios['label'] = data_win_2_ios.apply(lambda x: 1 if x.create_role_num == 0 else (1 if
                                                                                                   x.create_role_roi <= 0.026 else (
            0 if (x.create_role_pay_num > 1) & (
                    x.create_role_roi >= 0.028) & (x.source_run_date_amount >= 500) else (
                0 if (x.create_role_roi >= 0.030) & (x.source_run_date_amount >= 500) else 1 if
                (x.create_role_pay_cost > 8000) | (x.create_role_cost > 220) else 2))), axis=1)

        result_win2 = result_win2.append(
            data_win_2_ios[(data_win_2_ios['label'] == 1) | (data_win_2_ios['label'] == 0)])
        temp_win2 = temp_win2.append(data_win_2_ios[data_win_2_ios['label'] == 2])

    # win=2 模型预测
    if temp_win2.shape[0] != 0:
        temp_win2['label'] = gbdt_b_win2.predict(temp_win2[['create_role_cost',
                                                            'create_role_pay_cost', 'create_role_roi',
                                                            'create_role_pay_rate', 'create_role_retain_1d']])
    result_win2_data = pd.concat([result_win2, temp_win2], axis=0)

    # win=3预判
    result_win3 = pd.DataFrame()
    temp_win3 = pd.DataFrame()
    if data_win_3.shape[0] != 0:
        data_win_3['label'] = data_win_3.apply(lambda x: 1 if x.create_role_num == 0 else (1 if
                                                                                           x.create_role_roi <= 0.054 else (
            0 if (x.create_role_pay_num > 1) & (
                    x.create_role_roi >= 0.06) & (x.source_run_date_amount >= 500) else (
                0 if (x.create_role_roi >= 0.75) & (x.source_run_date_amount >= 500) else 1 if
                (x.create_role_pay_cost > 6000) | (x.create_role_cost > 120) else 2))), axis=1)

        result_win3 = result_win3.append(data_win_3[(data_win_3['label'] == 1) | (data_win_3['label'] == 0)])
        temp_win3 = temp_win3.append(data_win_3[data_win_3['label'] == 2])

    if data_win_3_ios.shape[0] != 0:
        data_win_3_ios['label'] = data_win_3_ios.apply(lambda x: 1 if x.create_role_num == 0 else (1 if
                                                                                                   x.create_role_roi <= 0.040 else (
            0 if (x.create_role_pay_num > 1) & (
                    x.create_role_roi >= 0.042) & (x.source_run_date_amount >= 500) else (
                0 if (x.create_role_roi >= 0.046) & (x.source_run_date_amount >= 500) else 1 if
                (x.create_role_pay_cost > 6000) | (x.create_role_cost > 200) else 2))), axis=1)

        result_win3 = result_win3.append(
            data_win_3_ios[(data_win_3_ios['label'] == 1) | (data_win_3_ios['label'] == 0)])
        temp_win3 = temp_win3.append(data_win_3_ios[data_win_3_ios['label'] == 2])

    # win=3 模型预测
    if temp_win3.shape[0] != 0:
        temp_win3['label'] = gbdt_b_win3.predict(temp_win3[['create_role_cost',
                                                            'create_role_pay_cost', 'create_role_roi',
                                                            'create_role_pay_rate', 'create_role_retain_1d']])
    result_win3_data = pd.concat([result_win3, temp_win3], axis=0)

    result = pd.concat([result_win1_data, result_win2_data, result_win3_data], axis=0)

    # 结果输出
    if result.shape[0] != 0:
        result_1_2_3 = result[result['data_win'] != 7]
        # win1,2,3按优先级3,2,1进行去重
        if result_1_2_3.shape[0] != 0:
            result_1_2_3.sort_values('data_win', ascending=False, inplace=True)
            result_1_2_3.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first', inplace=True)

        # 再与win=7进行去重，win=7优先级小
        # result_all = pd.concat([result_1_2_3, result[result['data_win'] == 7]], axis=0)
        #
        # result_all.sort_values('data_win', inplace=True)
        source_predict = result_1_2_3.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')

    else:
        source_predict = pd.DataFrame(
            columns=["channel_id", "source_id", "plan_name", "model_run_datetime", "create_time",
                     "media_id", "game_id", "platform", "data_win", "source_run_date_amount", "create_role_num",
                     "create_role_cost", "create_role_pay_num", "create_role_pay_cost", "create_role_pay_sum",
                     "create_role_roi",
                     "create_role_retain_1d", "create_role_pay_rate", "create_role_pay_num_cum", "learning_type",
                     "learning_time_dt",
                     "learning_time_hr", "deep_bid_type", "label"])

    if data_ignore.shape[0] != 0:
        data_ignore['label'] = 1
        data_ignore.sort_values('data_win', ascending=False, inplace=True)
        data_ignore = data_ignore.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
        data_ignore['data_win'] = -1

    source_predict = pd.concat([source_predict, data_ignore], axis=0)
    source_predict.sort_values('data_win', ascending=False, inplace=True)
    source_predict = source_predict.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')

    source_predict['label'] = source_predict['label'].astype(int)
    # print(source_predict.columns.values.tolist())
    source_predict = source_predict.fillna('null')
    return {"columns": source_predict.columns.values.tolist(), "list": source_predict.values.tolist()}


"""
重新加载模型
"""


def reload_model():
    global model_path, lgb_r_1d, lgb_r_2d, lgb_r_3d, lgb_r_4d, lgb_r_5d, lgb_r_6d, lgb_r_7d, source_lgb_b, source_lgb_b_cv_0, \
        source_lgb_b_cv, gbdt, model_for_tank, gbdt_win0, gbdt_b_win1, gbdt_b_win2, gbdt_b_win3
    lgb_r_1d = joblib.load(model_path + 'lgb_r_1d.pkl')
    lgb_r_2d = joblib.load(model_path + 'lgb_r_2d.pkl')
    lgb_r_3d = joblib.load(model_path + 'lgb_r_3d.pkl')
    lgb_r_4d = joblib.load(model_path + 'lgb_r_4d.pkl')
    lgb_r_5d = joblib.load(model_path + 'lgb_r_5d.pkl')
    lgb_r_6d = joblib.load(model_path + 'lgb_r_6d.pkl')
    lgb_r_7d = joblib.load(model_path + 'lgb_r_7d.pkl')
    source_lgb_b = joblib.load(model_path + 'source_lgb_b.pkl')
    source_lgb_b_cv = joblib.load(model_path + 'lgb_b_cv.pkl')
    source_lgb_b_cv_0 = joblib.load(model_path + 'lgb_b_cv_0.pkl')
    gbdt = joblib.load(model_path + 'gbdt.pkl')
    model_for_tank = joblib.load(model_path + 'model_for_tank.pkl')
    gbdt_win0 = joblib.load(model_path + 'gbdt_win0.pkl')
    gbdt_b_win1 = joblib.load(model_path + 'gbdt_b_win1.pkl')
    gbdt_b_win2 = joblib.load(model_path + 'gbdt_b_win2.pkl')
    gbdt_b_win3 = joblib.load(model_path + 'gbdt_b_win3.pkl')
    return "Reload modelservice successed!"
