# -- coding: utf-8 --

import web
import time
import logging
import json
import pandas as pd
import numpy as np
import joblib
import datetime
from impala.dbapi import connect
from impala.util import as_pandas
import pymysql

from modelservice.__myconf__ import get_var
dicParam = get_var()

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
        # if "role_info" == action:
        #     data = do_role_info(json_data)
        # elif "source_info_1" == action:
        #     data = do_source_info_1(json_data)
        # elif "source_predict" == action:
        #     data = source_predict(json_data)
        # elif "reload_model" == action:
        #     data = reload_model()
        # elif "source_predict_for_tank" == action:
        #     data = source_predict_for_tank(json_data)
        if "source_predict_state_1" == action:
            data = source_predict_state_1(json_data)
        elif "reload_model" == action:
            data = reload_model()
        elif "source_predict_state_2" == action:
            data = source_predict_state_2(json_data)
        elif "source_predict_state_1_mr_bd" == action:      ## TODO：末日百度模型1
            data = source_predict_state_1_mr_bd(json_data)
        elif "mr_source_predict_state_2_bd" == action:      ## TODO：末日百度模型2
            data = mr_source_predict_state_2_bd(json_data)
        elif "dg_source_predict_state_1" == action:
            data = dg_source_predict_state_1(json_data)
        elif "dg_source_predict_state_2" == action:
            data = dg_source_predict_state_2(json_data)
        elif "jp_source_predict_state_1" == action:  ## TODO：金牌模型1（头条）
            data = jp_source_predict_state_1(json_data)  ## TODO
        elif "jp_source_predict_state_2" == action:  ## TODO：金牌模型2（头条）
            data = jp_source_predict_state_2(json_data)  ## TODO
        elif "source_predict_state_1_gdt" == action:
            data = source_predict_state_1_gdt(json_data)
        elif "source_predict_state_2_gdt" == action:
            data = source_predict_state_2_gdt(json_data)
        elif "dg_source_predict_state_2_gdt" == action:
            data = dg_source_predict_state_2_gdt(json_data)
        elif "source_predict_state_1_dg_gdt" == action:
            data = source_predict_state_1_dg_gdt(json_data)
        elif "jp_source_predict_state_1_gdt" == action:  ## TODO：金牌模型1（广点通）
            data = jp_source_predict_state_1_gdt(json_data)  ## TODO
        elif "jp_source_predict_state_2_gdt" == action:  ## TODO：金牌模型2（广点通）
            data = jp_source_predict_state_2_gdt(json_data)  ## TODO
        elif "source_predict_state_1_jp_bd" == action:  ## TODO：金牌模型1（百度）
            data = source_predict_state_1_jp_bd(json_data)  ## TODO
        elif "source_predict_state_2_jp_bd" == action:  ## TODO：金牌模型2（百度）
            data = source_predict_state_2_jp_bd(json_data)  ## TODO
        elif "qx_source_predict_state_2" == action:
            data = qx_source_predict_state_2(json_data)
        elif "qx_source_predict_state_2_gdt" == action:
            data = qx_source_predict_state_2_gdt(json_data)
        elif "qx_source_predict_state_1" == action:
            data = qx_source_predict_state_1(json_data)
        elif "qx_source_predict_state_1_gdt" == action:
            data = qx_source_predict_state_1_gdt(json_data)
        elif "source_predict_state_1_dg_bd" == action:
            data = source_predict_state_1_dg_bd(json_data)
        elif "dg_source_predict_state_2_bd" == action:
            data = dg_source_predict_state_2_bd(json_data)
        elif "source_predict_state_1_dg_ks" == action:
            data = source_predict_state_1_dg_ks(json_data)
        elif "dg_source_predict_state_2_ks" == action:
            data = dg_source_predict_state_2_ks(json_data)
        elif "source_predict_state_1_ddj_bd" == action:       ## 大东家百度模型1
            data = source_predict_state_1_ddj_bd(json_data)
        elif "ddj_source_predict_state_2_bd" == action:       ## 大东家百度模型2
            data = ddj_source_predict_state_2_bd(json_data)
        elif "ddj_source_predict_state_1" == action:       ## 大东家头条模型1
            data = ddj_source_predict_state_1(json_data)
        elif "ddj_source_predict_state_2" == action:       ## 大东家头条模型2
            data = ddj_source_predict_state_2(json_data)
        else:
            data = {}
        ret = json.dumps({"code": 200, "msg": "success!", "timestamp": "", "data": data})
        return ret


'''
末日模型
'''

# 2021-6-21:双状况计划监控模型——末日广点通模型1
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
                lambda x: 1 if (((x.create_role_cost >= 400) & (x.source_run_date_amount <= 1000) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 500) & (x.source_run_date_amount <= 1000) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_cost >= 200) & (x.source_run_date_amount > 1000) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 300) & (x.source_run_date_amount > 1000) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_roi < 0.01) & (x.source_run_date_amount >= 4000))) else (0
                          if (((x.create_role_cost <= 395) & (x.source_run_date_amount <= 1000) & (x.create_role_pay_sum == 0)) |
                              ((x.create_role_cost <= 495) & (x.source_run_date_amount <= 1000) & (x.create_role_pay_sum != 0)) |
                              ((x.create_role_cost <= 195) & (x.source_run_date_amount > 1000) & (x.create_role_pay_sum == 0)) |
                              ((x.create_role_cost <= 295) & (x.source_run_date_amount > 1000) & (x.create_role_pay_sum != 0)) |
                              (x.create_role_roi >= 0.01)) else 2), axis=1)

            result_df = result_df.append(data_win_0_1)

        if data_win_0_2.shape[0] != 0:
            data_win_0_2['label'] = data_win_0_2.apply(
                lambda x: 1 if (x.create_role_cost >= 200) else (1 if (x.create_role_roi <= 0.013)
                                                                      | (x.create_role_pay_cost >= 8000) else (
                    0 if (x.create_role_roi >= 0.012) else 2)), axis=1)
            result_df = result_df.append(data_win_0_2)

        if data_win_0_3.shape[0] != 0:
            data_win_0_3['label'] = data_win_0_3.apply(
                lambda x: 1 if (x.create_role_cost >= 180) else (1 if (x.create_role_roi <= 0.016)
                                                                      | (x.create_role_pay_cost >= 7000) else (
                    0 if (x.create_role_roi >= 0.014) else 2)), axis=1)
            result_df = result_df.append(data_win_0_3)

        if data_win_0_4.shape[0] != 0:
            data_win_0_4['label'] = data_win_0_4.apply(
                lambda x: 1 if (x.create_role_cost >= 180) else (1 if (x.create_role_roi <= 0.018)
                                                                      | (x.create_role_pay_cost >= 7000) else (
                    0 if (x.create_role_roi >= 0.016) else 2)), axis=1)
            result_df = result_df.append(data_win_0_4)

        if data_win_0_5.shape[0] != 0:
            data_win_0_5['label'] = data_win_0_5.apply(
                lambda x: 1 if (x.create_role_cost >= 160) else (1 if (x.create_role_roi <= 0.02)
                                                                      | (x.create_role_pay_cost >= 5500) else (
                    0 if (x.create_role_roi >= 0.02) else 2)), axis=1)
            result_df = result_df.append(data_win_0_5)

    # ISO
    if data_win_0_ios.shape[0] != 0:
        data_win_0_ios_1 = data_win_0_ios[data_win_0_ios['source_run_date_amount'] <= 12000]
        data_win_0_ios_2 = data_win_0_ios[
            (data_win_0_ios['source_run_date_amount'] > 12000) & (data_win_0_ios['source_run_date_amount'] <= 18000)]
        data_win_0_ios_3 = data_win_0_ios[
            (data_win_0_ios['source_run_date_amount'] > 18000) & (data_win_0_ios['source_run_date_amount'] <= 30000)]
        data_win_0_ios_4 = data_win_0_ios[
            (data_win_0_ios['source_run_date_amount'] > 30000) & (data_win_0_ios['source_run_date_amount'] <= 55000)]
        data_win_0_ios_5 = data_win_0_ios[data_win_0_ios['source_run_date_amount'] > 55000]
        if data_win_0_ios_1.shape[0] != 0:
            data_win_0_ios_1['label'] = data_win_0_ios_1.apply(
                lambda x: 1 if (((x.create_role_cost >= 650) & (x.source_run_date_amount <= 1000) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 750) & (x.source_run_date_amount <= 1000) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_cost >= 400) & (x.source_run_date_amount > 1000) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 500) & (x.source_run_date_amount > 1000) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_roi < 0.01) & (x.source_run_date_amount >= 10000))) else (0
                          if (((x.create_role_cost <= 640) & (x.source_run_date_amount <= 1000) & (x.create_role_pay_sum == 0)) |
                              ((x.create_role_cost <= 740) & (x.source_run_date_amount <= 1000) & (x.create_role_pay_sum != 0)) |
                              ((x.create_role_cost <= 390) & (x.source_run_date_amount > 1000) & (x.create_role_pay_sum == 0)) |
                              ((x.create_role_cost <= 490) & (x.source_run_date_amount > 1000) & (x.create_role_pay_sum != 0)) |
                              (x.create_role_roi >= 0.01)) else 2), axis=1)
            result_df = result_df.append(data_win_0_ios_1)

        if data_win_0_ios_2.shape[0] != 0:
            data_win_0_ios_2['label'] = data_win_0_ios_2.apply(
                lambda x: 1 if (x.create_role_cost >= 400) else (1 if (x.create_role_roi <= 0.01)
                                                                      | (x.create_role_pay_cost >= 15000) else (
                    0 if (x.create_role_roi >= 0.01) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_2)

        if data_win_0_ios_3.shape[0] != 0:
            data_win_0_ios_3['label'] = data_win_0_ios_3.apply(
                lambda x: 1 if (x.create_role_cost >= 400) else (1 if (x.create_role_roi <= 0.01)
                                                                      | (x.create_role_pay_cost >= 15000) else (
                    0 if (x.create_role_roi >= 0.01) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_3)

        if data_win_0_ios_4.shape[0] != 0:
            data_win_0_ios_4['label'] = data_win_0_ios_4.apply(
                lambda x: 1 if (x.create_role_cost >= 400) else (1 if (x.create_role_roi <= 0.015)
                                                                      | (x.create_role_pay_cost >= 12000) else (
                    0 if (x.create_role_roi >= 0.015) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_4)

        if data_win_0_ios_5.shape[0] != 0:
            data_win_0_ios_5['label'] = data_win_0_ios_5.apply(
                lambda x: 1 if (x.create_role_cost >= 400) else (1 if (x.create_role_roi <= 0.02)
                                                                      | (x.create_role_pay_cost >= 10000) else (
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


# 末日头条模型1
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
        # 跑录屏素材，成本高
        # data_win_0_pre['label'] = data_win_0_pre.apply(
        #     lambda x: 1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount <= 1000) & (x.create_role_cost >= 300)
        #        else (1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount > 1000) & (x.create_role_cost >= 200)
        #              else (1 if (x.create_role_roi > 0) & (x.create_role_roi <= 0.02) & (x.create_role_cost >= 300)
        #                    else (1 if x.create_role_cost >= 400 else 0))), axis=1)

        # 跑小素材，成本低
        data_win_0_pre['label'] = data_win_0_pre.apply(
            lambda x: 1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount <= 1000) & (x.create_role_cost >= 150)
               else (1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount > 1000) & (x.create_role_cost >= 100)
                     else (1 if (x.create_role_roi > 0) & (x.create_role_roi <= 0.02) & (x.create_role_cost >= 150)
                           else (1 if x.create_role_cost >= 200 else 0))), axis=1)

        result_df = result_df.append(data_win_0_pre)

    # 每次付费ISO
    if data_win_0_ios_pre.shape[0] != 0:
        data_win_0_ios_pre['label'] = data_win_0_ios_pre.apply(
            lambda x: 1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount <= 1000) & (x.create_role_cost >= 300)
                else (1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount > 1000) & (x.create_role_cost >= 250)
                      else (1 if (x.create_role_roi > 0) & (x.create_role_roi <= 0.02) & (x.create_role_cost >= 300)
                            else (1 if x.create_role_cost >= 350 else 0))), axis=1)
        result_df = result_df.append(data_win_0_ios_pre)

    # 其它付费方式安卓
    if data_win_0.shape[0] != 0:
        # 跑录屏素材，成本高
        # data_win_0['label'] = data_win_0.apply(
        #     lambda x: 1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount <= 1000) & (x.create_role_cost >= 240)
        #         else (1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount > 1000) & (x.create_role_cost >= 140)
        #               else (1 if (x.create_role_roi > 0) & (x.create_role_roi <= 0.02) & (x.create_role_cost >= 200)
        #                     else (1 if x.create_role_cost >= 260 else 0))), axis=1)
        # 跑小素材，成本低
        data_win_0['label'] = data_win_0.apply(
            lambda x: 1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount <= 1000) & (x.create_role_cost >= 120)
                else (1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount > 1000) & (x.create_role_cost >= 70)
                      else (1 if (x.create_role_roi > 0) & (x.create_role_roi <= 0.02) & (x.create_role_cost >= 100)
                            else (1 if x.create_role_cost >= 130 else 0))), axis=1)

        result_df = result_df.append(data_win_0)

    # 其它付费方式IOS
    if data_win_0_ios.shape[0] != 0:
        data_win_0_ios['label'] = data_win_0_ios.apply(
            lambda x: 1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount <= 1000) & (x.create_role_cost >= 200)
                else (1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount > 1000) & (x.create_role_cost >= 150)
                      else (1 if (x.create_role_roi > 0) & (x.create_role_roi <= 0.02) & (x.create_role_cost >= 250)
                            else (1 if x.create_role_cost >= 250 else 0))), axis=1)
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
                lambda x: 13000 if (x.create_role_roi >= 0.025) & (x.create_role_pay_num >= 2) else np.nan, axis=1)
            result_df = result_df.append(data_2)
        if data_3.shape[0] != 0:
            data_3['budget'] = data_3.apply(
                lambda x: 23000 if (x.create_role_roi >= 0.026) & (x.create_role_pay_num >= 3) else
                (23000 if x.create_role_roi >= 0.04 else np.nan), axis=1)
            # data_3['budget'] = data_3.apply(
            #     lambda x: 23000 if (x.create_role_roi >= 0.035) & (x.create_role_pay_num >= 2) else
            #     np.nan, axis=1)
            result_df = result_df.append(data_3)
        if data_4.shape[0] != 0:
            data_4['budget'] = data_4.apply(
                lambda x: 50000 if (x.create_role_roi >= 0.026) & (x.create_role_pay_cost <= 5000) else
                (50000 if (x.create_role_roi >= 0.032) & (x.create_role_pay_cost <= 6000) else
                 np.nan), axis=1)
            result_df = result_df.append(data_4)
        if data_5.shape[0] != 0:
            data_5['budget'] = data_5.apply(
                lambda x: 95000 if (x.create_role_roi >= 0.026) & (x.create_role_pay_cost <= 5000) else
                (95000 if (x.create_role_roi >= 0.032) & (x.create_role_pay_cost <= 5500) else
                 np.nan), axis=1)
            result_df = result_df.append(data_5)
        if data_6.shape[0] != 0:
            data_6['budget'] = data_6.apply(
                lambda x: 148000 if (x.create_role_roi >= 0.028) & (x.create_role_pay_cost <= 4500) else
                (148000 if (x.create_role_roi >= 0.034) & (x.create_role_pay_cost <= 5000) else
                 np.nan), axis=1)
            result_df = result_df.append(data_6)
        if data_7.shape[0] != 0:
            data_7['budget'] = data_7.apply(
                lambda x: 220000 if (x.create_role_roi >= 0.031) & (x.create_role_pay_cost <= 4000) else
                (220000 if (x.create_role_roi >= 0.036) & (x.create_role_pay_cost <= 4500) else
                 np.nan), axis=1)
            result_df = result_df.append(data_7)
        if data_8.shape[0] != 0:
            data_8['budget'] = data_8.apply(
                lambda x: 275000 if (x.create_role_roi >= 0.032) & (x.create_role_pay_cost <= 3800) else
                (275000 if (x.create_role_roi >= 0.037) & (x.create_role_pay_cost <= 4300) else
                 np.nan), axis=1)
            result_df = result_df.append(data_8)
        if data_9.shape[0] != 0:
            data_9['budget'] = data_9.apply(
                lambda x: 320000 if (x.create_role_roi >= 0.032) & (x.create_role_pay_cost <= 3600) else
                (320000 if (x.create_role_roi >= 0.037) & (x.create_role_pay_cost <= 4100) else
                 np.nan), axis=1)
            result_df = result_df.append(data_9)
        if data_10.shape[0] != 0:
            data_10['budget'] = data_10.apply(
                lambda x: 450000 if (x.create_role_roi >= 0.033) & (x.create_role_pay_cost <= 3500) else
                (450000 if (x.create_role_roi >= 0.038) & (x.create_role_pay_cost <= 4100) else
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
                lambda x: 13000 if (x.create_role_roi >= 0.021) & (x.create_role_pay_num >= 2) else np.nan, axis=1)
            result_df = result_df.append(data_2_ios)
        if data_3_ios.shape[0] != 0:
            # data_3_ios['budget'] = data_3_ios.apply(
            #     lambda x: 23000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_num >= 2) else
            #     np.nan, axis=1)
            data_3_ios['budget'] = data_3_ios.apply(
                lambda x: 23000 if (x.create_role_roi >= 0.022) & (x.create_role_pay_num >= 3) else
                (23000 if x.create_role_roi >= 0.035 else np.nan), axis=1)
            result_df = result_df.append(data_3_ios)
        if data_4_ios.shape[0] != 0:
            data_4_ios['budget'] = data_4_ios.apply(
                lambda x: 50000 if (x.create_role_roi >= 0.023) & (x.create_role_pay_cost <= 6000) else
                (50000 if (x.create_role_roi >= 0.028) & (x.create_role_pay_cost <= 7500) else
                 np.nan), axis=1)
            result_df = result_df.append(data_4_ios)
        if data_5_ios.shape[0] != 0:
            data_5_ios['budget'] = data_5_ios.apply(
                lambda x: 95000 if (x.create_role_roi >= 0.024) & (x.create_role_pay_cost <= 5500) else
                (95000 if (x.create_role_roi >= 0.029) & (x.create_role_pay_cost <= 7000) else
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


# 末日百度模型1
def source_predict_state_1_mr_bd(jsondata):
    '''
    :param jsondata: {"columns": ["channel_id","source_id","plan_name", "model_run_datetime","create_time","media_id",
    "game_id","platform","source_run_date_amount","create_role_num","create_role_cost","create_role_pay_num",
    "create_role_pay_cost","create_role_pay_sum","create_role_roi","create_role_pay_rate","create_role_pay_num_cum",
    "learning_type","learning_time_dt","learning_time_hr","deep_bid_type"]}
    :return:'label','budget'
    '''

    # 该模型只能接受计划状态plan_run_state=1且data_win=0 且media_id=45 且 model_run_datetime 等于当日的数据，即只接受当日广点通实时数据
    # label为1时关计划，为0时开计划，如果模型输出label结果与计划当前状况（开/关）相同，则不操作。例如，计划当前是关停状况，当模型输出为1时，不操作

    # 早上7：55分，将所有在运行的计划状态plan_run_state改为2。调用模型2进行判断，是否达标。达标则状态改为1，开计划，预算由模型1计算决定。不达标，则为关停状态，且plan_run_state为2

    data = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])
    data = data[data['media_id'] == 45]
    # 数据预处理
    data = data[data['source_run_date_amount'] > 0]  # 删除消耗为0的数据

    # 对消耗300元以下的不作判断，对数据按平台、win进行分类处理
    data_not_ignore = data[data['source_run_date_amount'] >= 300]

    data_win_0 = data_not_ignore[data_not_ignore['platform'] == 1]
    data_win_0_ios = data_not_ignore[data_not_ignore['platform'] == 2]

    result_df = pd.DataFrame()

    # 获取近15天平均创角成本
    def get_an_cost():
        conn = pymysql.connect(host=dicParam['DB_SLAVE_FENXI_HOST'], port=int(dicParam['DB_SLAVE_FENXI_PORT']), user=dicParam['DB_SLAVE_FENXI_USERNAME'],
                               passwd=dicParam['DB_SLAVE_FENXI_PASSWORD'])
        cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
        sql = '''
                SELECT
                    sum( amount ) / sum( create_role_num ) AS cost 
                FROM
                    db_stdata.st_lauch_report 
                WHERE
                    tdate_type = 'day' 
                    AND tdate >= DATE_SUB( date( NOW()), INTERVAL 15 DAY ) 
                    AND media_id = 45 
                    AND platform = 1 
                    AND game_id IN (
                    SELECT
                        dev_game_id AS game_id 
                    FROM
                        db_data.t_game_config 
                    WHERE
                        game_id = 1056 
                    AND dev_game_id IS NOT NULL 
                    )
        '''
        cur.execute(sql)
        result_df = pd.read_sql(sql, conn)
        cur.close()
        conn.close()
        try:
            result = int(result_df['cost'].values)
        except:
            result = 40
        return result

    def get_ios_cost():
        conn = pymysql.connect(host=dicParam['DB_SLAVE_FENXI_HOST'], port=int(dicParam['DB_SLAVE_FENXI_PORT']), user=dicParam['DB_SLAVE_FENXI_USERNAME'],
                               passwd=dicParam['DB_SLAVE_FENXI_PASSWORD'])
        cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
        sql = '''
                SELECT
                    sum( amount ) / sum( create_role_num ) AS cost 
                FROM
                    db_stdata.st_lauch_report 
                WHERE
                    tdate_type = 'day' 
                    AND tdate >= DATE_SUB( date( NOW()), INTERVAL 15 DAY ) 
                    AND media_id = 45 
                    AND platform = 2 
                    AND game_id IN (
                    SELECT
                        dev_game_id AS game_id 
                    FROM
                        db_data.t_game_config 
                    WHERE
                        game_id = 1056 
                    AND dev_game_id IS NOT NULL 
                    )
        '''
        cur.execute(sql)
        result_df = pd.read_sql(sql, conn)
        cur.close()
        conn.close()
        try:
            result = int(result_df['cost'].values)
        except:
            result = 120
        return result
    # 获取该游戏该媒体的双端创角成本
    cost_an = get_an_cost()
    cost_ios = get_ios_cost()

    # 1:关; 0:开； 2：保持原状
    # win=0预判
    if data_win_0.shape[0] != 0:
        data_win_0_1 = data_win_0[data_win_0['source_run_date_amount'] <= 5000]
        data_win_0_2 = data_win_0[(data_win_0['source_run_date_amount'] > 5000) & (data_win_0['source_run_date_amount'] <= 12000)]
        data_win_0_3 = data_win_0[(data_win_0['source_run_date_amount'] > 12000) & (data_win_0['source_run_date_amount'] <= 22000)]
        data_win_0_4 = data_win_0[(data_win_0['source_run_date_amount'] > 22000) & (data_win_0['source_run_date_amount'] <= 50000)]
        data_win_0_5 = data_win_0[data_win_0['source_run_date_amount'] > 50000]
        if data_win_0_1.shape[0] != 0:
            data_win_0_1['label'] = data_win_0_1.apply(
                lambda x: 1 if (((x.create_role_cost >= 2.5 * cost_an) & (x.source_run_date_amount <= 800) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 3.5 * cost_an) & (x.source_run_date_amount <= 800) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_cost >= 2 * cost_an) & (x.source_run_date_amount > 800) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 2.5 * cost_an) & (x.source_run_date_amount > 800) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_roi < 0.018) & (x.source_run_date_amount >= 3500))) else (0
                             if ((( x.create_role_cost <= (2.5 * cost_an - 10)) & (x.source_run_date_amount <= 800) & (x.create_role_pay_sum == 0)) |
                                 (( x.create_role_cost <= (3.5 * cost_an - 10)) & (x.source_run_date_amount <= 800) & (x.create_role_pay_sum != 0)) |
                                 (( x.create_role_cost <= (2 * cost_an - 10)) & (x.source_run_date_amount > 800) & (x.create_role_pay_sum == 0)) |
                                 (( x.create_role_cost <= (2.5 * cost_an - 10)) & (x.source_run_date_amount > 800) & (x.create_role_pay_sum != 0)) |
                                 (x.create_role_roi >= 0.018)) else 2), axis=1)

            result_df = result_df.append(data_win_0_1)

        if data_win_0_2.shape[0] != 0:
            data_win_0_2['label'] = data_win_0_2.apply(
                lambda x: 1 if (x.create_role_cost >= 3 * cost_an)
                        else (1 if (x.create_role_roi <= 0.019) | (x.create_role_pay_cost >= 6000)
                              else (0 if (x.create_role_roi >= 0.02) else 2)), axis=1)
            result_df = result_df.append(data_win_0_2)

        if data_win_0_3.shape[0] != 0:
            data_win_0_3['label'] = data_win_0_3.apply(
                lambda x: 1 if (x.create_role_cost >= 3 * cost_an)
                        else (1 if (x.create_role_roi <= 0.019) | (x.create_role_pay_cost >= 6000)
                              else (0 if (x.create_role_roi >= 0.02) else 2)), axis=1)
            result_df = result_df.append(data_win_0_3)

        if data_win_0_4.shape[0] != 0:
            data_win_0_4['label'] = data_win_0_4.apply(
                lambda x: 1 if (x.create_role_cost >= 2.8 * cost_an)
                        else (1 if (x.create_role_roi <= 0.019) | (x.create_role_pay_cost >= 6000)
                              else (0 if (x.create_role_roi >= 0.02) else 2)), axis=1)
            result_df = result_df.append(data_win_0_4)

        if data_win_0_5.shape[0] != 0:
            data_win_0_5['label'] = data_win_0_5.apply(
                lambda x: 1 if (x.create_role_cost >= 2.8 * cost_an)
                        else (1 if (x.create_role_roi <= 0.019) | (x.create_role_pay_cost >= 6000)
                              else (0 if (x.create_role_roi >= 0.02) else 2)), axis=1)
            result_df = result_df.append(data_win_0_5)

    # ISO
    if data_win_0_ios.shape[0] != 0:
        data_win_0_ios_1 = data_win_0_ios[data_win_0_ios['source_run_date_amount'] <= 8000]
        data_win_0_ios_2 = data_win_0_ios[
            (data_win_0_ios['source_run_date_amount'] > 8000) & (data_win_0_ios['source_run_date_amount'] <= 14000)]
        data_win_0_ios_3 = data_win_0_ios[
            (data_win_0_ios['source_run_date_amount'] > 14000) & (data_win_0_ios['source_run_date_amount'] <= 28000)]
        data_win_0_ios_4 = data_win_0_ios[
            (data_win_0_ios['source_run_date_amount'] > 28000) & (data_win_0_ios['source_run_date_amount'] <= 48000)]
        data_win_0_ios_5 = data_win_0_ios[data_win_0_ios['source_run_date_amount'] > 48000]
        if data_win_0_ios_1.shape[0] != 0:
            data_win_0_ios_1['label'] = data_win_0_ios_1.apply(
                lambda x: 1 if (((x.create_role_cost >= 2.5 * cost_ios) & (x.source_run_date_amount <= 1500) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 3.5 * cost_ios) & (x.source_run_date_amount <= 1500) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_cost >= 2 * cost_ios) & (x.source_run_date_amount > 1500) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 2.5 * cost_ios) & (x.source_run_date_amount > 1500) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_roi < 0.015) & (x.source_run_date_amount >= 5000))) else (0
                          if (((x.create_role_cost <= (2.5 * cost_ios - 10)) & (x.source_run_date_amount <= 1300) & (x.create_role_pay_sum == 0)) |
                              ((x.create_role_cost <= (2.5 * cost_ios - 10)) & (x.source_run_date_amount <= 1300) & (x.create_role_pay_sum != 0)) |
                              ((x.create_role_cost <= (2.5 * cost_ios - 10)) & (x.source_run_date_amount > 1300) & (x.create_role_pay_sum == 0)) |
                              ((x.create_role_cost <= (2.5 * cost_ios - 10)) & (x.source_run_date_amount > 1300) & (x.create_role_pay_sum != 0)) |
                              (x.create_role_roi >= 0.015)) else 2), axis=1)

            result_df = result_df.append(data_win_0_ios_1)

        if data_win_0_ios_2.shape[0] != 0:
            data_win_0_ios_2['label'] = data_win_0_ios_2.apply(
                lambda x: 1 if (x.create_role_cost >= 3 * cost_ios)
                        else (1 if (x.create_role_roi <= 0.019) | (x.create_role_pay_cost >= 7000)
                              else (0 if (x.create_role_roi >= 0.02) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_2)

        if data_win_0_ios_3.shape[0] != 0:
            data_win_0_ios_3['label'] = data_win_0_ios_3.apply(
                lambda x: 1 if (x.create_role_cost >= 3 * cost_ios)
                        else (1 if (x.create_role_roi <= 0.019) | (x.create_role_pay_cost >= 7000)
                              else (0 if (x.create_role_roi >= 0.02) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_3)

        if data_win_0_ios_4.shape[0] != 0:
            data_win_0_ios_4['label'] = data_win_0_ios_4.apply(
                lambda x: 1 if (x.create_role_cost >= 2.8 * cost_ios)
                        else (1 if (x.create_role_roi <= 0.019) | (x.create_role_pay_cost >= 7000)
                              else (0 if (x.create_role_roi >= 0.02) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_4)

        if data_win_0_ios_5.shape[0] != 0:
            data_win_0_ios_5['label'] = data_win_0_ios_5.apply(
                lambda x: 1 if (x.create_role_cost >= 2.8 * cost_ios)
                        else (1 if (x.create_role_roi <= 0.019) | (x.create_role_pay_cost >= 7000)
                              else (0 if (x.create_role_roi >= 0.02) else 2)), axis=1)
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


# 末日广点通state2模型2
# def source_predict_state_2_gdt(jsondata):
#     '''
#     :param jsondata: {"columns": ["channel_id","source_id","plan_name","model_run_datetime","create_time",
#     "media_id","game_id","platform","data_win","source_run_date_amount","create_role_num",
#     "create_role_cost","create_role_pay_num","create_role_pay_cost","create_role_pay_sum","create_role_roi",
#     "create_role_retain_1d","create_role_pay_rate","create_role_pay_num_cum","learning_type","learning_time_dt",
#     "learning_time_hr","deep_bid_type","cum_amount","cum_day","cum_role_cost"]}
#     :return:
#     '''
#     data = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])
#
#     # 数据预处理
#     data = data[data['source_run_date_amount'] > 0]  # 删除消耗为0的数据
#     # 去重，处理某天没有消耗的情况
#     data.sort_values(by='data_win', inplace=True)
#     data.drop_duplicates(subset=['channel_id', 'source_id', 'source_run_date_amount', 'create_role_num'], keep='first'
#                          , inplace=True)
#
#     # 计划7天内总消耗不足4000，且总成本合格的计划，让他继续跑，先不做判断
#     data_0 = data[(data['cum_amount'] >= 200) & (data['cum_amount'] <= 6000) & (data['cum_day'] <= 7) & (
#                 data['cum_role_cost'] <= 300) & (data['platform'] == 1)]
#     data_1 = data[(data['cum_amount'] >= 200) & (data['cum_amount'] <= 10000) & (data['cum_day'] <= 7) & (
#                 data['cum_role_cost'] <= 400) & (data['platform'] == 2)]
#     data_0 = data_0.append(data_1)
#     data_0['label'] = data_0['cum_amount'].apply(lambda x: 0)
#
#     data = data.drop(data_0.index).reset_index(drop=True)
#     # 对窗口期消耗200元以下的不作判断，对数据按平台、win进行分类处理
#     data_not_ignore = data[(data['source_run_date_amount'] >= 200) | (data['data_win'] == 1)]
#     data_ignore = data.drop(data_not_ignore.index)
#
#     data_win_1 = data_not_ignore[(data_not_ignore['data_win'] == 1) & (data_not_ignore['platform'] == 1)]
#     data_win_2 = data_not_ignore[(data_not_ignore['data_win'] == 2) & (data_not_ignore['platform'] == 1)]
#     data_win_3 = data_not_ignore[(data_not_ignore['data_win'] == 3) & (data_not_ignore['platform'] == 1)]
#
#     data_win_1_ios = data_not_ignore[(data_not_ignore['data_win'] == 1) & (data_not_ignore['platform'] == 2)]
#     data_win_2_ios = data_not_ignore[(data_not_ignore['data_win'] == 2) & (data_not_ignore['platform'] == 2)]
#     data_win_3_ios = data_not_ignore[(data_not_ignore['data_win'] == 3) & (data_not_ignore['platform'] == 2)]
#
#     # win=1预判
#     result_win1 = pd.DataFrame()
#     temp_win1 = pd.DataFrame()
#
#     # data_win_1中再分deep_bid_type
#     data_win_1_pre_action = data_win_1[data_win_1['deep_bid_type'] == 'BID_PER_ACTION']
#     data_win_1 = data_win_1[data_win_1['deep_bid_type'] != 'BID_PER_ACTION']
#
#     if data_win_1_pre_action.shape[0] != 0:
#         data_win_1_pre_action['label'] = data_win_1_pre_action.apply(lambda x: 1 if x.create_role_num == 0
#         else (1 if (x.create_role_pay_cost > 8000) | (x.create_role_cost > 220)
#               else (1 if x.create_role_roi <= 0.012
#                     else (0 if x.create_role_roi >= 0.013 else 2))), axis=1)
#
#         result_win1 = result_win1.append(
#             data_win_1_pre_action[(data_win_1_pre_action['label'] == 1) | (data_win_1_pre_action['label'] == 0)])
#         temp_win1 = temp_win1.append(data_win_1_pre_action[data_win_1_pre_action['label'] == 2])
#
#     if data_win_1.shape[0] != 0:
#         data_win_1['label'] = data_win_1.apply(lambda x: 1 if x.create_role_num == 0
#         else (1 if (x.create_role_pay_cost > 8000) | (x.create_role_cost > 200)
#               else (1 if x.create_role_roi <= 0.012
#                     else (0 if x.create_role_roi >= 0.013 else 2))), axis=1)
#
#         result_win1 = result_win1.append(data_win_1[(data_win_1['label'] == 1) | (data_win_1['label'] == 0)])
#         temp_win1 = temp_win1.append(data_win_1[data_win_1['label'] == 2])
#
#     data_win_1_ios_pre_action = data_win_1_ios[data_win_1_ios['deep_bid_type'] == 'BID_PER_ACTION']
#     data_win_1_ios = data_win_1_ios[data_win_1_ios['deep_bid_type'] != 'BID_PER_ACTION']
#
#     if data_win_1_ios_pre_action.shape[0] != 0:
#         data_win_1_ios_pre_action['label'] = data_win_1_ios_pre_action.apply(lambda x: 1 if x.create_role_num == 0
#         else (1 if (x.create_role_pay_cost > 12000) | (x.create_role_cost > 400)
#               else (1 if x.create_role_roi <= 0.01
#                     else (0 if x.create_role_roi >= 0.011 else 2))), axis=1)
#
#         result_win1 = result_win1.append(data_win_1_ios_pre_action[(data_win_1_ios_pre_action['label'] == 1) | (
#                 data_win_1_ios_pre_action['label'] == 0)])
#         temp_win1 = temp_win1.append(data_win_1_ios_pre_action[data_win_1_ios_pre_action['label'] == 2])
#
#     if data_win_1_ios.shape[0] != 0:
#         data_win_1_ios['label'] = data_win_1_ios.apply(lambda x: 1 if x.create_role_num == 0
#         else (1 if (x.create_role_pay_cost > 12000) | (x.create_role_cost > 350)
#               else (1 if x.create_role_roi <= 0.01
#                     else (0 if x.create_role_roi >= 0.011 else 2))), axis=1)
#
#         result_win1 = result_win1.append(
#             data_win_1_ios[(data_win_1_ios['label'] == 1) | (data_win_1_ios['label'] == 0)])
#         temp_win1 = temp_win1.append(data_win_1_ios[data_win_1_ios['label'] == 2])
#
#     # win=1 模型预测
#     if temp_win1.shape[0] != 0:
#         temp_win1['label'] = gbdt_b_win1.predict(
#             temp_win1[['create_role_cost', 'create_role_pay_cost', 'create_role_roi',
#                        'create_role_pay_rate']])
#     result_win1_data = pd.concat([result_win1, temp_win1], axis=0)
#
#     # win=2预判
#     result_win2 = pd.DataFrame()
#     temp_win2 = pd.DataFrame()
#
#     if data_win_2.shape[0] != 0:
#         data_win_2['label'] = data_win_2.apply(lambda x: 1 if x.create_role_num == 0
#         else (1 if (x.create_role_pay_cost > 7000) | (x.create_role_cost > 200)
#               else (3 if (x.create_role_roi == 0) & (x.source_run_date_amount >= 6000)
#                     else (1 if x.create_role_roi <= 0.017
#                           else (0 if x.create_role_roi > 0.017 else 2)))), axis=1)
#         result_win2 = result_win2.append(
#             data_win_2[(data_win_2['label'] == 1) | (data_win_2['label'] == 0) | (data_win_2['label'] == 3)])
#         temp_win2 = temp_win2.append(data_win_2[data_win_2['label'] == 2])
#
#     if data_win_2_ios.shape[0] != 0:
#         data_win_2_ios['label'] = data_win_2_ios.apply(lambda x: 1 if x.create_role_num == 0
#         else (1 if (x.create_role_pay_cost > 10000) | (x.create_role_cost > 350)
#               else (3 if (x.create_role_roi == 0) & (x.source_run_date_amount >= 7000)
#                     else (1 if x.create_role_roi <= 0.016
#                           else (0 if x.create_role_roi > 0.016 else 2)))), axis=1)
#
#         result_win2 = result_win2.append(
#             data_win_2_ios[(data_win_2_ios['label'] == 1) | (data_win_2_ios['label'] == 0)])
#         temp_win2 = temp_win2.append(data_win_2_ios[data_win_2_ios['label'] == 2])
#
#     # win=2 模型预测
#     if temp_win2.shape[0] != 0:
#         temp_win2['label'] = gbdt_b_win2.predict(
#             temp_win2[['create_role_cost', 'create_role_pay_cost', 'create_role_roi',
#                        'create_role_pay_rate', 'create_role_retain_1d']])
#     result_win2_data = pd.concat([result_win2, temp_win2], axis=0)
#
#     # win=3预判
#     result_win3 = pd.DataFrame()
#     temp_win3 = pd.DataFrame()
#     if data_win_3.shape[0] != 0:
#         data_win_3['label'] = data_win_3.apply(lambda x: 1 if x.create_role_num == 0
#         else (1 if (x.create_role_pay_cost > 6000) | (x.create_role_cost > 200)
#               else (0 if (x.cum_day <= 7) & (x.source_run_date_amount <= 5000) & (x.cum_role_cost <= 220)
#                     else (0 if (x.create_role_pay_cost <= 4000) & (x.create_role_pay_cost > 0)
#                     else (0 if x.create_role_roi >= 0.025
#                           else (1 if x.create_role_roi <= 0.024 else 2))))), axis=1)
#
#         result_win3 = result_win3.append(data_win_3[(data_win_3['label'] == 1) | (data_win_3['label'] == 0)])
#         temp_win3 = temp_win3.append(data_win_3[data_win_3['label'] == 2])
#
#     if data_win_3_ios.shape[0] != 0:
#         data_win_3_ios['label'] = data_win_3_ios.apply(lambda x: 1 if x.create_role_num == 0
#         else (1 if (x.create_role_pay_cost > 8000) | (x.create_role_cost > 350)
#               else (0 if (x.cum_day <= 7) & (x.source_run_date_amount <= 6000) & (x.cum_role_cost <= 400)
#                     else (0 if (x.create_role_pay_cost <= 4000) & (x.create_role_pay_cost > 0)
#                     else (0 if x.create_role_roi >= 0.024
#                           else (1 if x.create_role_roi <= 0.023 else 2))))), axis=1)
#
#         result_win3 = result_win3.append(
#             data_win_3_ios[(data_win_3_ios['label'] == 1) | (data_win_3_ios['label'] == 0)])
#         temp_win3 = temp_win3.append(data_win_3_ios[data_win_3_ios['label'] == 2])
#
#     # win=3 模型预测
#     if temp_win3.shape[0] != 0:
#         temp_win3['label'] = gbdt_b_win3.predict(
#             temp_win3[['create_role_cost', 'create_role_pay_cost', 'create_role_roi',
#                        'create_role_pay_rate', 'create_role_retain_1d']])
#     result_win3_data = pd.concat([result_win3, temp_win3], axis=0)
#
#     result = pd.concat([result_win1_data, result_win2_data, result_win3_data], axis=0)
#     if result.shape[0] != 0:
#         result.sort_values('data_win', ascending=True, inplace=True)
#
#     # 结果输出
#     if result.shape[0] != 0:
#         # 1\2\3有一个开，则开，除非win2=3
#         if (result[result['data_win'] == 3].shape[0] != 0) & (result[result['data_win'] == 2].shape[0] != 0) & (
#                 result[result['data_win'] == 1].shape[0] != 0):
#             result_1_2_3 = result[(result['data_win'] == 1) | (result['data_win'] == 2) | (result['data_win'] == 3)]
#             result_1_2_3_label = result_1_2_3[['channel_id', 'source_id', 'model_run_datetime', 'data_win', 'label']]
#             result_1_2_3_label['data_win'] = result_1_2_3_label['data_win'].astype(int)
#             result_1_2_3_piv = pd.pivot_table(result_1_2_3_label,
#                                               index=['channel_id', 'source_id', 'model_run_datetime'],
#                                               columns='data_win')
#             result_1_2_3_piv.columns = result_1_2_3_piv.columns.droplevel()
#             result_1_2_3_piv = result_1_2_3_piv.rename(columns={1: 'label_1', 2: 'label_2', 3: 'label_3'})
#
#             result_1_2_3_piv = result_1_2_3_piv.reset_index()
#
#             result_1_2_3_piv['label'] = result_1_2_3_piv.apply(lambda x: 0 if x.label_1 == 0
#             else (0 if x.label_2 == 0
#                   else (1 if x.label_2 == 3
#                         else (0 if x.label_3 == 0 else 1))), axis=1)
#             result_1_2_3 = result_1_2_3.drop('label', axis=1)
#             #             result_1_2 = result_1_2[result_1_2['data_win'] == 1]
#             result_1_2_3 = result_1_2_3.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='last')
#
#             result_1_2_3_piv.drop(['label_1', 'label_2', 'label_3'], axis=1, inplace=True)
#             source_predict = pd.merge(result_1_2_3, result_1_2_3_piv,
#                                       on=['channel_id', 'source_id', 'model_run_datetime'],
#                                       how='left')
#             # 1\2有一个为开，则开
#         elif (result[result['data_win'] == 2].shape[0] != 0) & (result[result['data_win'] == 1].shape[0] != 0):
#             result_1_2 = result[(result['data_win'] == 1) | (result['data_win'] == 2)]
#             result_1_2_label = result_1_2[['channel_id', 'source_id', 'model_run_datetime', 'data_win', 'label']]
#             result_1_2_label['data_win'] = result_1_2_label['data_win'].astype(int)
#             result_1_2_piv = pd.pivot_table(result_1_2_label, index=['channel_id', 'source_id', 'model_run_datetime'],
#                                             columns='data_win')
#             result_1_2_piv.columns = result_1_2_piv.columns.droplevel()
#             result_1_2_piv = result_1_2_piv.rename(columns={1: 'label_1', 2: 'label_2'})
#
#             result_1_2_piv = result_1_2_piv.reset_index()
#             result_1_2_piv['label'] = result_1_2_piv.apply(lambda x: 0 if x.label_1 == 0
#             else (0 if x.label_2 == 0 else 1), axis=1)
#             result_1_2 = result_1_2.drop('label', axis=1)
#             #             result_1_2 = result_1_2[result_1_2['data_win'] == 1]
#             result_1_2 = result_1_2.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='last')
#
#             result_1_2_piv.drop(['label_1', 'label_2'], axis=1, inplace=True)
#             source_predict = pd.merge(result_1_2, result_1_2_piv, on=['channel_id', 'source_id', 'model_run_datetime'],
#                                       how='left')
#         else:
#             source_predict = result
#
#     else:
#         source_predict = pd.DataFrame(
#             columns=["channel_id", "source_id", "plan_name", "model_run_datetime", "create_time",
#                      "media_id", "game_id", "platform", "data_win", "source_run_date_amount", "create_role_num",
#                      "create_role_cost", "create_role_pay_num", "create_role_pay_cost", "create_role_pay_sum",
#                      "create_role_roi",
#                      "create_role_retain_1d", "create_role_pay_rate", "create_role_pay_num_cum", "learning_type",
#                      "learning_time_dt",
#                      "learning_time_hr", "deep_bid_type", "cum_amount", "cum_day", "cum_role_cost", "label"])
#
#     # 消耗小的ignore，直接关停计划，赋值1，同时data_win变为9，以便后续按datawin大小筛选优先级
#     if data_ignore.shape[0] != 0:
#         data_ignore['label'] = 1
#         data_ignore.sort_values('data_win', ascending=False, inplace=True)
#         data_ignore = data_ignore.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
#         data_ignore['data_win'] = 9  # 9 代表是被ignore的计划，消耗太小
#
#     # 7天内总消耗不足4000，成本合格的计划，直接开计划，账值为0，同时data_win变为-1，以便后续按datawin大小筛选优先级
#     if data_0.shape[0] != 0:
#         data_0.sort_values('data_win', ascending=False, inplace=True)
#         data_0 = data_0.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
#         data_0['data_win'] = -1  # -1 代表是被data_0的计划，累积消耗太小
#
#     source_predict = pd.concat([source_predict, data_ignore], axis=0)
#     source_predict = pd.concat([source_predict, data_0], axis=0)
#
#     # 综合结果优先级： data_0    大于 data   大于  data_ignore
#     source_predict.sort_values('data_win', ascending=True, inplace=True)
#     source_predict = source_predict.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
#     #     print(source_predict.head())
#     source_predict['label'] = source_predict['label'].astype(int)
#     # print(source_predict.columns.values.tolist())
#     source_predict = source_predict.fillna('null')
#
#     return {"columns": source_predict.columns.values.tolist(), "list": source_predict.values.tolist()}


# 末日广点通模型2
def source_predict_state_2_gdt(jsondata):
    '''
    :param jsondata: {"columns": ["channel_id","source_id","plan_name","model_run_datetime","create_time",
    "media_id","game_id","platform","data_win","source_run_date_amount","create_role_num",
    "create_role_cost","create_role_pay_num","create_role_pay_cost","create_role_pay_sum","create_role_roi",
    "create_role_retain_1d","create_role_pay_rate","create_role_pay_num_cum","learning_type","learning_time_dt",
    "learning_time_hr","deep_bid_type","cum_amount","cum_day","cum_role_cost"]}
    :return:
    '''
    data = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])

    # 数据预处理
    data = data[data['source_run_date_amount'] > 0]  # 删除消耗为0的数据
    # 去重，处理某天没有消耗的情况
    data.sort_values(by='data_win', inplace=True)
    data.drop_duplicates(subset=['channel_id', 'source_id', 'source_run_date_amount', 'create_role_num'], keep='first'
                         , inplace=True)

    # 计划7天内总消耗不足4000，且总成本合格的计划，让他继续跑，先不做判断
    data_0 = data[(data['cum_amount'] >= 100) & (data['cum_amount'] <= 4000) & (data['cum_day'] <= 7) & (
            data['cum_role_cost'] <= 200) & (data['platform'] == 1)]
    data_1 = data[(data['cum_amount'] >= 100) & (data['cum_amount'] <= 6000) & (data['cum_day'] <= 7) & (
            data['cum_role_cost'] <= 300) & (data['platform'] == 2)]
    data_0 = data_0.append(data_1)
    data_0['label'] = data_0['cum_amount'].apply(lambda x: 0)

    data = data.drop(data_0.index).reset_index(drop=True)
    # 对窗口期消耗200元以下的不作判断，对数据按平台、win进行分类处理
    data_not_ignore = data[(data['source_run_date_amount'] >= 200) | (data['data_win'] == 1)]
    data_ignore = data.drop(data_not_ignore.index)

    # 对最近1日\2日 消耗过6000，且没有付费的计划进行强制关停 data_2
    data_2 = data_not_ignore[
        (data_not_ignore['data_win'] <= 2) & (data_not_ignore['source_run_date_amount'] >= 6000) & (
                    data_not_ignore['create_role_roi'] == 0)]
    data_not_ignore = data_not_ignore.drop(data_2.index)

    # 对data_not_ignoref进行周期筛选,1-3窗口期，保留最大周期
    data_not_ignore = data_not_ignore[data_not_ignore['data_win'] <= 3]
    data_not_ignore.sort_values('data_win', ascending=False, inplace=True)
    data_not_ignore = data_not_ignore.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')

    def get_source_data():
        conn = connect(host=dicParam['HIVE_HOST'], port=int(dicParam['HIVE_PORT']), auth_mechanism=dicParam['HIVE_AUTH_MECHANISM'], user=dicParam['HIVE_USERNAME'],
                       password=dicParam['HIVE_PASSWORD'], database=dicParam['HIVE_DATABASE'])
        cursor = conn.cursor()
        sql = '''
            SELECT
                channel_id,
                source_id,
                7_amount as amount_7,
                7_roi as roi_7,
                data_win 
            FROM
                tmp_data.tmp_ra_media_source 
            WHERE
                dt = `current_date` () 
                AND mgame_id = 1056 
                AND media_id = 16
        '''
        cursor.execute(sql)
        result = as_pandas(cursor)

        # 关闭链接
        cursor.close()
        conn.close()
        return result

    # 计划评分数据获取
    source_data = get_source_data()
    source_data_3 = source_data[source_data['data_win'] == 3]
    source_data_7 = source_data[source_data['data_win'] == 7]

    # 计划评分数据与源数据进行拼接
    data_not_ignore = pd.merge(data_not_ignore, source_data_3.drop(['data_win'], axis=1),
                               on=['channel_id', 'source_id'], how='left')
    data_not_ignore = data_not_ignore.rename(columns={'amount_7': 'amount_3', 'roi_7': 'roi_3'})
    data_not_ignore = pd.merge(data_not_ignore, source_data_7.drop(['data_win'], axis=1),
                               on=['channel_id', 'source_id'], how='left')

    # 依据3_roi和7_roi对计划进行判断是否关停
    if data_not_ignore.shape[0] != 0:
        data_not_ignore['label'] = data_not_ignore.apply(lambda x: 1 if (x.amount_3 >= 4500) & ((x.create_role_pay_cost >= 8000) | (x.create_role_roi == 0))
                                                else (0 if x.roi_3 >= 0.04
                                                      else (0 if (x.roi_3 >= 0.025) & (x.amount_3 <= 3500)
                                                            else (0 if (x.roi_7 >= 0.07) & (x.roi_3 >= 0.02) else 1))), axis=1)
        data_not_ignore.drop(['amount_3', 'roi_3', 'amount_7', 'roi_7'], axis=1, inplace=True)
    else:
        data_not_ignore = pd.DataFrame(
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
        data_ignore['data_win'] = 9  # 9 代表是被ignore的计划，消耗太小

    # 1\2日消耗过6000，充值为0，赋值为1，同时data_win变为-1，以便后续按datawin大小筛选优先级
    if data_2.shape[0] != 0:
        data_2['label'] = 1
        data_2.sort_values('data_win', ascending=False, inplace=True)
        data_2 = data_2.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
        data_2['data_win'] = -1  # -1 代表是被data_0的计划，累积消耗太小

    # 7天内总消耗不足4000，成本合格的计划，直接开计划，账值为0，同时data_win变为-2，以便后续按datawin大小筛选优先级
    if data_0.shape[0] != 0:
        data_0.sort_values('data_win', ascending=False, inplace=True)
        data_0 = data_0.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
        data_0['data_win'] = -2  # -1 代表是被data_0的计划，累积消耗太小

    source_predict = pd.concat([data_not_ignore, data_ignore], axis=0)
    source_predict = pd.concat([source_predict, data_0], axis=0)
    source_predict = pd.concat([source_predict, data_2], axis=0)

    # 综合结果优先级： data_0   data_2  大于 data   大于  data_ignore
    source_predict.sort_values('data_win', ascending=True, inplace=True)
    source_predict = source_predict.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
    #     print(source_predict.head())
    source_predict['label'] = source_predict['label'].astype(int)
    # print(source_predict.columns.values.tolist())
    source_predict = source_predict.fillna('null')

    return {"columns": source_predict.columns.values.tolist(), "list": source_predict.values.tolist()}


# 末日头条模型2
def source_predict_state_2(jsondata):
    '''
    :param jsondata: {"columns": ["channel_id","source_id","plan_name","model_run_datetime","create_time",
    "media_id","game_id","platform","data_win","source_run_date_amount","create_role_num",
    "create_role_cost","create_role_pay_num","create_role_pay_cost","create_role_pay_sum","create_role_roi",
    "create_role_retain_1d","create_role_pay_rate","create_role_pay_num_cum","learning_type","learning_time_dt",
    "learning_time_hr","deep_bid_type","cum_amount","cum_day","cum_role_cost"]}
    :return:
    '''
    data = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])

    # 数据预处理
    data = data[data['source_run_date_amount'] > 0]  # 删除消耗为0的数据
    # 去重，处理某天没有消耗的情况
    data.sort_values(by='data_win', inplace=True)
    data.drop_duplicates(subset=['channel_id', 'source_id', 'source_run_date_amount', 'create_role_num'], keep='first'
                         , inplace=True)

    # 计划7天内总消耗不足4000，且总成本合格的计划，让他继续跑，先不做判断
    data_0 = data[(data['cum_amount'] >= 100) & (data['cum_amount'] <= 3100) & (data['cum_day'] <= 7) & (
            data['cum_role_cost'] <= 200) & (data['platform'] == 1)]
    data_1 = data[(data['cum_amount'] >= 100) & (data['cum_amount'] <= 4000) & (data['cum_day'] <= 7) & (
            data['cum_role_cost'] <= 300) & (data['platform'] == 2)]
    data_0 = data_0.append(data_1)
    data_0['label'] = data_0['cum_amount'].apply(lambda x: 0)

    data = data.drop(data_0.index).reset_index(drop=True)
    # 对窗口期消耗200元以下的不作判断，对数据按平台、win进行分类处理
    data_not_ignore = data[(data['source_run_date_amount'] >= 200) | (data['data_win'] == 1)]
    data_ignore = data.drop(data_not_ignore.index)

    # 对最近1日\2日 消耗过6000，且没有付费的计划进行强制关停 data_2
    data_2 = data_not_ignore[
        (data_not_ignore['data_win'] <= 2) & (data_not_ignore['source_run_date_amount'] >= 6000) & (
                    data_not_ignore['create_role_roi'] == 0)]
    data_not_ignore = data_not_ignore.drop(data_2.index)

    # 对data_not_ignoref进行周期筛选,1-3窗口期，保留最大周期
    data_not_ignore = data_not_ignore[data_not_ignore['data_win'] <= 3]
    data_not_ignore.sort_values('data_win', ascending=False, inplace=True)
    data_not_ignore = data_not_ignore.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')

    def get_source_data():
        conn = connect(host=dicParam['HIVE_HOST'], port=int(dicParam['HIVE_PORT']), auth_mechanism=dicParam['HIVE_AUTH_MECHANISM'], user=dicParam['HIVE_USERNAME'],
                       password=dicParam['HIVE_PASSWORD'], database=dicParam['HIVE_DATABASE'])
        cursor = conn.cursor()
        sql = '''
            SELECT
                channel_id,
                source_id,
                7_amount as amount_7,
                7_roi as roi_7,
                data_win 
            FROM
                tmp_data.tmp_ra_media_source 
            WHERE
                dt = `current_date` () 
                AND mgame_id = 1056 
                AND media_id = 10
        '''
        cursor.execute(sql)
        result = as_pandas(cursor)

        # 关闭链接
        cursor.close()
        conn.close()
        return result

    # 计划评分数据获取
    source_data = get_source_data()
    source_data_3 = source_data[source_data['data_win'] == 3]
    source_data_7 = source_data[source_data['data_win'] == 7]

    # 计划评分数据与源数据进行拼接
    data_not_ignore = pd.merge(data_not_ignore, source_data_3.drop(['data_win'], axis=1),
                               on=['channel_id', 'source_id'], how='left')
    data_not_ignore = data_not_ignore.rename(columns={'amount_7': 'amount_3', 'roi_7': 'roi_3'})
    data_not_ignore = pd.merge(data_not_ignore, source_data_7.drop(['data_win'], axis=1),
                               on=['channel_id', 'source_id'], how='left')

    # 依据3_roi和7_roi对计划进行判断是否关停
    if data_not_ignore.shape[0] != 0:
        data_not_ignore['label'] = data_not_ignore.apply(lambda x: 1 if (x.amount_3 >= 3500) & ((x.create_role_pay_cost >= 8000) | (x.create_role_roi == 0))
                                                else (0 if x.roi_3 >= 0.04
                                                      else (0 if (x.roi_3 >= 0.025) & (x.amount_3 <= 3500)
                                                            else (0 if (x.roi_7 >= 0.08) & (x.roi_3 >= 0.02) else 1))), axis=1)
        data_not_ignore.drop(['amount_3', 'roi_3', 'amount_7', 'roi_7'], axis=1, inplace=True)
    else:
        data_not_ignore = pd.DataFrame(
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
        data_ignore['data_win'] = 9  # 9 代表是被ignore的计划，消耗太小

    # 1\2日消耗过6000，充值为0，赋值为1，同时data_win变为-1，以便后续按datawin大小筛选优先级
    if data_2.shape[0] != 0:
        data_2['label'] = 1
        data_2.sort_values('data_win', ascending=False, inplace=True)
        data_2 = data_2.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
        data_2['data_win'] = -1  # -1 代表是被data_0的计划，累积消耗太小

    # 7天内总消耗不足4000，成本合格的计划，直接开计划，账值为0，同时data_win变为-2，以便后续按datawin大小筛选优先级
    if data_0.shape[0] != 0:
        data_0.sort_values('data_win', ascending=False, inplace=True)
        data_0 = data_0.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
        data_0['data_win'] = -2  # -1 代表是被data_0的计划，累积消耗太小

    source_predict = pd.concat([data_not_ignore, data_ignore], axis=0)
    source_predict = pd.concat([source_predict, data_0], axis=0)
    source_predict = pd.concat([source_predict, data_2], axis=0)

    # 综合结果优先级： data_0   data_2  大于 data   大于  data_ignore
    source_predict.sort_values('data_win', ascending=True, inplace=True)
    source_predict = source_predict.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
    #     print(source_predict.head())
    source_predict['label'] = source_predict['label'].astype(int)
    # print(source_predict.columns.values.tolist())
    source_predict = source_predict.fillna('null')

    return {"columns": source_predict.columns.values.tolist(), "list": source_predict.values.tolist()}


# 末日百度模型2
def mr_source_predict_state_2_bd(jsondata):
    '''
    :param jsondata: {"columns": ["channel_id","source_id","plan_name","model_run_datetime","create_time",
    "media_id","game_id","platform","data_win","source_run_date_amount","create_role_num",
    "create_role_cost","create_role_pay_num","create_role_pay_cost","create_role_pay_sum","create_role_roi",
    "create_role_retain_1d","create_role_pay_rate","create_role_pay_num_cum","learning_type","learning_time_dt",
    "learning_time_hr","deep_bid_type","cum_amount","cum_day","cum_role_cost"]}
    :return:
    '''
    data = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])

    # 数据预处理
    data = data[data['source_run_date_amount'] > 0]  # 删除消耗为0的数据
    # 去重，处理某天没有消耗的情况
    data.sort_values(by='data_win', inplace=True)
    data.drop_duplicates(subset=['channel_id', 'source_id', 'source_run_date_amount', 'create_role_num'], keep='first'
                         , inplace=True)

    # 计划7天内总消耗不足一定额度，且总成本合格的计划，让他继续跑，先不做判断
    data_0 = data[(data['cum_amount'] >= 100) & (data['cum_amount'] <= 4000) & (data['cum_day'] <= 7) & (
            data['cum_role_cost'] <= 100) & (data['platform'] == 1)]
    data_1 = data[(data['cum_amount'] >= 100) & (data['cum_amount'] <= 5500) & (data['cum_day'] <= 7) & (
            data['cum_role_cost'] <= 300) & (data['platform'] == 2)]
    data_0 = data_0.append(data_1)
    data_0['label'] = data_0['cum_amount'].apply(lambda x: 0)

    data = data.drop(data_0.index).reset_index(drop=True)
    # 对窗口期消耗200元以下的不作判断，对数据按平台、win进行分类处理
    data_not_ignore = data[(data['source_run_date_amount'] >= 200) | (data['data_win'] == 1)]
    data_ignore = data.drop(data_not_ignore.index)

    # 对最近1日\2日 消耗过6000，且没有付费的计划进行强制关停 data_2
    data_2 = data_not_ignore[
        (data_not_ignore['data_win'] <= 2) & (data_not_ignore['source_run_date_amount'] >= 6000) & (
                    data_not_ignore['create_role_roi'] == 0)]
    data_not_ignore = data_not_ignore.drop(data_2.index)

    # 对data_not_ignore进行周期筛选,1-3窗口期，保留最大周期
    data_not_ignore = data_not_ignore[data_not_ignore['data_win'] <= 3]
    data_not_ignore.sort_values('data_win', ascending=False, inplace=True)
    data_not_ignore = data_not_ignore.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')

    def get_source_data():
        conn = connect(host=dicParam['HIVE_HOST'], port=int(dicParam['HIVE_PORT']), auth_mechanism=dicParam['HIVE_AUTH_MECHANISM'], user=dicParam['HIVE_USERNAME'],
                       password=dicParam['HIVE_PASSWORD'], database=dicParam['HIVE_DATABASE'])
        cursor = conn.cursor()
        sql = '''
            SELECT
                channel_id,
                source_id,
                7_amount as amount_7,
                7_roi as roi_7,
                data_win 
            FROM
                tmp_data.tmp_ra_media_source 
            WHERE
                dt = `current_date` () 
                AND mgame_id = 1056 
                AND media_id = 45
        '''
        cursor.execute(sql)
        result = as_pandas(cursor)

        # 关闭链接
        cursor.close()
        conn.close()
        return result

    # 计划评分数据获取
    source_data = get_source_data()
    source_data_3 = source_data[source_data['data_win'] == 3]
    source_data_7 = source_data[source_data['data_win'] == 7]

    # 计划评分数据与源数据进行拼接
    data_not_ignore = pd.merge(data_not_ignore, source_data_3.drop(['data_win'], axis=1),
                               on=['channel_id', 'source_id'], how='left')
    data_not_ignore = data_not_ignore.rename(columns={'amount_7': 'amount_3', 'roi_7': 'roi_3'})
    data_not_ignore = pd.merge(data_not_ignore, source_data_7.drop(['data_win'], axis=1),
                               on=['channel_id', 'source_id'], how='left')

    # 依据3_roi和7_roi对计划进行判断是否关停
    if data_not_ignore.shape[0] != 0:
        data_not_ignore['label'] = data_not_ignore.apply(lambda x: 1 if (x.amount_3 >= 4000) & ((x.create_role_pay_cost >= 8000) | (x.create_role_roi == 0))
                                                else (0 if x.roi_3 >= 0.04
                                                      else (0 if (x.roi_3 >= 0.025) & (x.amount_3 <= 3500)
                                                            else (0 if (x.roi_7 >= 0.08) & (x.roi_3 >= 0.02) else 1))), axis=1)
        data_not_ignore.drop(['amount_3', 'roi_3', 'amount_7', 'roi_7'], axis=1, inplace=True)
    else:
        data_not_ignore = pd.DataFrame(
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
        data_ignore['data_win'] = 9  # 9 代表是被ignore的计划，消耗太小

    # 1\2日消耗过4000，充值为0，赋值为1，同时data_win变为-1，以便后续按datawin大小筛选优先级
    if data_2.shape[0] != 0:
        data_2['label'] = 1
        data_2.sort_values('data_win', ascending=False, inplace=True)
        data_2 = data_2.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
        data_2['data_win'] = -1  # -1 代表是被data_0的计划，累积消耗太小

    # 7天内总消耗不足，成本合格的计划，直接开计划，账值为0，同时data_win变为-2，以便后续按datawin大小筛选优先级
    if data_0.shape[0] != 0:
        data_0.sort_values('data_win', ascending=False, inplace=True)
        data_0 = data_0.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
        data_0['data_win'] = -2  # -1 代表是被data_0的计划，累积消耗太小

    source_predict = pd.concat([data_not_ignore, data_ignore], axis=0)
    source_predict = pd.concat([source_predict, data_0], axis=0)
    source_predict = pd.concat([source_predict, data_2], axis=0)

    # 综合结果优先级： data_0   data_2  大于 data   大于  data_ignore
    source_predict.sort_values('data_win', ascending=True, inplace=True)
    source_predict = source_predict.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
    #     print(source_predict.head())
    source_predict['label'] = source_predict['label'].astype(int)
    # print(source_predict.columns.values.tolist())
    source_predict = source_predict.fillna('null')

    return {"columns": source_predict.columns.values.tolist(), "list": source_predict.values.tolist()}


"""
大东家模型   2022-4-27
"""
# 大东家百度模型1
def source_predict_state_1_ddj_bd(jsondata):
    '''
    :param jsondata: {"columns": ["channel_id","source_id","plan_name", "model_run_datetime","create_time","media_id",
    "game_id","platform","source_run_date_amount","create_role_num","create_role_cost","create_role_pay_num",
    "create_role_pay_cost","create_role_pay_sum","create_role_roi","create_role_pay_rate","create_role_pay_num_cum",
    "learning_type","learning_time_dt","learning_time_hr","deep_bid_type"]}
    :return:'label','budget'
    '''

    # 该模型只能接受计划状态plan_run_state=1且data_win=0 且media_id=45 且 model_run_datetime 等于当日的数据，即只接受当日广点通实时数据
    # label为1时关计划，为0时开计划，如果模型输出label结果与计划当前状况（开/关）相同，则不操作。例如，计划当前是关停状况，当模型输出为1时，不操作

    # 早上7：55分，将所有在运行的计划状态plan_run_state改为2。调用模型2进行判断，是否达标。达标则状态改为1，开计划，预算由模型1计算决定。不达标，则为关停状态，且plan_run_state为2

    data = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])
    data = data[data['media_id'] == 45]
    # 数据预处理
    data = data[data['source_run_date_amount'] > 0]  # 删除消耗为0的数据

    # 对消耗300元以下的不作判断，对数据按平台、win进行分类处理
    data_not_ignore = data[data['source_run_date_amount'] >= 300]

    data_win_0 = data_not_ignore[data_not_ignore['platform'] == 1]
    data_win_0_ios = data_not_ignore[data_not_ignore['platform'] == 2]

    result_df = pd.DataFrame()

    # 获取近15天平均创角成本
    def get_an_cost():
        conn = pymysql.connect(host=dicParam['DB_SLAVE_FENXI_HOST'], port=int(dicParam['DB_SLAVE_FENXI_PORT']), user=dicParam['DB_SLAVE_FENXI_USERNAME'],
                               passwd=dicParam['DB_SLAVE_FENXI_PASSWORD'])
        cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
        sql = '''
                SELECT
                    sum( amount ) / sum( create_role_num ) AS cost 
                FROM
                    db_stdata.st_lauch_report 
                WHERE
                    tdate_type = 'day' 
                    AND tdate >= DATE_SUB( date( NOW()), INTERVAL 15 DAY ) 
                    AND media_id = 45 
                    AND platform = 1 
                    AND game_id IN (
                    SELECT
                        dev_game_id AS game_id 
                    FROM
                        db_data.t_game_config 
                    WHERE
                        game_id = 1136 
                    AND dev_game_id IS NOT NULL 
                    )
        '''
        cur.execute(sql)
        result_df = pd.read_sql(sql, conn)
        cur.close()
        conn.close()
        try:
            result = int(result_df['cost'].values)
        except:
            result = 30
        return result

    def get_ios_cost():
        conn = pymysql.connect(host=dicParam['DB_SLAVE_FENXI_HOST'], port=int(dicParam['DB_SLAVE_FENXI_PORT']), user=dicParam['DB_SLAVE_FENXI_USERNAME'],
                               passwd=dicParam['DB_SLAVE_FENXI_PASSWORD'])
        cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
        sql = '''
                SELECT
                    sum( amount ) / sum( create_role_num ) AS cost 
                FROM
                    db_stdata.st_lauch_report 
                WHERE
                    tdate_type = 'day' 
                    AND tdate >= DATE_SUB( date( NOW()), INTERVAL 15 DAY ) 
                    AND media_id = 45 
                    AND platform = 2 
                    AND game_id IN (
                    SELECT
                        dev_game_id AS game_id 
                    FROM
                        db_data.t_game_config 
                    WHERE
                        game_id = 1136 
                    AND dev_game_id IS NOT NULL 
                    )
        '''
        cur.execute(sql)
        result_df = pd.read_sql(sql, conn)
        cur.close()
        conn.close()
        try:
            result = int(result_df['cost'].values)
        except:
            result = 100
        return result
    # 获取该游戏该媒体的双端创角成本
    cost_an = get_an_cost()
    cost_ios = get_ios_cost()

    # 1:关; 0:开； 2：保持原状
    # win=0预判
    if data_win_0.shape[0] != 0:
        data_win_0_1 = data_win_0[data_win_0['source_run_date_amount'] <= 5000]
        data_win_0_2 = data_win_0[(data_win_0['source_run_date_amount'] > 5000) & (data_win_0['source_run_date_amount'] <= 12000)]
        data_win_0_3 = data_win_0[(data_win_0['source_run_date_amount'] > 12000) & (data_win_0['source_run_date_amount'] <= 22000)]
        data_win_0_4 = data_win_0[(data_win_0['source_run_date_amount'] > 22000) & (data_win_0['source_run_date_amount'] <= 50000)]
        data_win_0_5 = data_win_0[data_win_0['source_run_date_amount'] > 50000]
        if data_win_0_1.shape[0] != 0:
            data_win_0_1['label'] = data_win_0_1.apply(
                lambda x: 1 if (((x.create_role_cost >= 2.5 * cost_an) & (x.source_run_date_amount <= 800) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 3.5 * cost_an) & (x.source_run_date_amount <= 800) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_cost >= 2 * cost_an) & (x.source_run_date_amount > 800) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 2.5 * cost_an) & (x.source_run_date_amount > 800) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_roi < 0.025) & (x.source_run_date_amount >= 2500))) else (0
                             if ((( x.create_role_cost <= (2.5 * cost_an - 10)) & (x.source_run_date_amount <= 800) & (x.create_role_pay_sum == 0)) |
                                 (( x.create_role_cost <= (3.5 * cost_an - 10)) & (x.source_run_date_amount <= 800) & (x.create_role_pay_sum != 0)) |
                                 (( x.create_role_cost <= (2 * cost_an - 10)) & (x.source_run_date_amount > 800) & (x.create_role_pay_sum == 0)) |
                                 (( x.create_role_cost <= (2.5 * cost_an - 10)) & (x.source_run_date_amount > 800) & (x.create_role_pay_sum != 0)) |
                                 (x.create_role_roi >= 0.025)) else 2), axis=1)

            result_df = result_df.append(data_win_0_1)

        if data_win_0_2.shape[0] != 0:
            data_win_0_2['label'] = data_win_0_2.apply(
                lambda x: 1 if (x.create_role_cost >= 3 * cost_an)
                        else (1 if (x.create_role_roi <= 0.029) | (x.create_role_pay_cost >= 3000)
                              else (0 if (x.create_role_roi >= 0.03) else 2)), axis=1)
            result_df = result_df.append(data_win_0_2)

        if data_win_0_3.shape[0] != 0:
            data_win_0_3['label'] = data_win_0_3.apply(
                lambda x: 1 if (x.create_role_cost >= 3 * cost_an)
                        else (1 if (x.create_role_roi <= 0.029) | (x.create_role_pay_cost >= 3000)
                              else (0 if (x.create_role_roi >= 0.03) else 2)), axis=1)
            result_df = result_df.append(data_win_0_3)

        if data_win_0_4.shape[0] != 0:
            data_win_0_4['label'] = data_win_0_4.apply(
                lambda x: 1 if (x.create_role_cost >= 2.8 * cost_an)
                        else (1 if (x.create_role_roi <= 0.029) | (x.create_role_pay_cost >= 3000)
                              else (0 if (x.create_role_roi >= 0.03) else 2)), axis=1)
            result_df = result_df.append(data_win_0_4)

        if data_win_0_5.shape[0] != 0:
            data_win_0_5['label'] = data_win_0_5.apply(
                lambda x: 1 if (x.create_role_cost >= 2.8 * cost_an)
                        else (1 if (x.create_role_roi <= 0.029) | (x.create_role_pay_cost >= 3000)
                              else (0 if (x.create_role_roi >= 0.03) else 2)), axis=1)
            result_df = result_df.append(data_win_0_5)

    # ISO
    if data_win_0_ios.shape[0] != 0:
        data_win_0_ios_1 = data_win_0_ios[data_win_0_ios['source_run_date_amount'] <= 8000]
        data_win_0_ios_2 = data_win_0_ios[
            (data_win_0_ios['source_run_date_amount'] > 8000) & (data_win_0_ios['source_run_date_amount'] <= 14000)]
        data_win_0_ios_3 = data_win_0_ios[
            (data_win_0_ios['source_run_date_amount'] > 14000) & (data_win_0_ios['source_run_date_amount'] <= 28000)]
        data_win_0_ios_4 = data_win_0_ios[
            (data_win_0_ios['source_run_date_amount'] > 28000) & (data_win_0_ios['source_run_date_amount'] <= 48000)]
        data_win_0_ios_5 = data_win_0_ios[data_win_0_ios['source_run_date_amount'] > 48000]
        if data_win_0_ios_1.shape[0] != 0:
            data_win_0_ios_1['label'] = data_win_0_ios_1.apply(
                lambda x: 1 if (((x.create_role_cost >= 2.5 * cost_ios) & (x.source_run_date_amount <= 1500) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 3.5 * cost_ios) & (x.source_run_date_amount <= 1500) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_cost >= 2 * cost_ios) & (x.source_run_date_amount > 1500) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 2.5 * cost_ios) & (x.source_run_date_amount > 1500) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_roi < 0.025) & (x.source_run_date_amount >= 5000))) else (0
                          if (((x.create_role_cost <= (2.5 * cost_ios - 10)) & (x.source_run_date_amount <= 1300) & (x.create_role_pay_sum == 0)) |
                              ((x.create_role_cost <= (2.5 * cost_ios - 10)) & (x.source_run_date_amount <= 1300) & (x.create_role_pay_sum != 0)) |
                              ((x.create_role_cost <= (2.5 * cost_ios - 10)) & (x.source_run_date_amount > 1300) & (x.create_role_pay_sum == 0)) |
                              ((x.create_role_cost <= (2.5 * cost_ios - 10)) & (x.source_run_date_amount > 1300) & (x.create_role_pay_sum != 0)) |
                              (x.create_role_roi >= 0.025)) else 2), axis=1)

            result_df = result_df.append(data_win_0_ios_1)

        if data_win_0_ios_2.shape[0] != 0:
            data_win_0_ios_2['label'] = data_win_0_ios_2.apply(
                lambda x: 1 if (x.create_role_cost >= 3 * cost_ios)
                        else (1 if (x.create_role_roi <= 0.029) | (x.create_role_pay_cost >= 4000)
                              else (0 if (x.create_role_roi >= 0.03) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_2)

        if data_win_0_ios_3.shape[0] != 0:
            data_win_0_ios_3['label'] = data_win_0_ios_3.apply(
                lambda x: 1 if (x.create_role_cost >= 3 * cost_ios)
                        else (1 if (x.create_role_roi <= 0.029) | (x.create_role_pay_cost >= 4000)
                              else (0 if (x.create_role_roi >= 0.03) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_3)

        if data_win_0_ios_4.shape[0] != 0:
            data_win_0_ios_4['label'] = data_win_0_ios_4.apply(
                lambda x: 1 if (x.create_role_cost >= 2.8 * cost_ios)
                        else (1 if (x.create_role_roi <= 0.029) | (x.create_role_pay_cost >= 4000)
                              else (0 if (x.create_role_roi >= 0.03) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_4)

        if data_win_0_ios_5.shape[0] != 0:
            data_win_0_ios_5['label'] = data_win_0_ios_5.apply(
                lambda x: 1 if (x.create_role_cost >= 2.8 * cost_ios)
                        else (1 if (x.create_role_roi <= 0.029) | (x.create_role_pay_cost >= 4000)
                              else (0 if (x.create_role_roi >= 0.03) else 2)), axis=1)
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


# 大东家头条模型1
def ddj_source_predict_state_1(jsondata):
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

    # 获取近15天平均创角成本
    def get_an_cost():
        conn = pymysql.connect(host=dicParam['DB_SLAVE_FENXI_HOST'], port=int(dicParam['DB_SLAVE_FENXI_PORT']), user=dicParam['DB_SLAVE_FENXI_USERNAME'],
                               passwd=dicParam['DB_SLAVE_FENXI_PASSWORD'])
        cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
        sql = '''
                    SELECT
                        sum( amount ) / sum( create_role_num ) AS cost 
                    FROM
                        db_stdata.st_lauch_report 
                    WHERE
                        tdate_type = 'day' 
                        AND tdate >= DATE_SUB( date( NOW()), INTERVAL 15 DAY ) 
                        AND media_id = 10 
                        AND platform = 1 
                        AND game_id IN (
                        SELECT
                            dev_game_id AS game_id 
                        FROM
                            db_data.t_game_config 
                        WHERE
                            game_id = 1136 
                        AND dev_game_id IS NOT NULL 
                        )
            '''
        cur.execute(sql)
        result_df = pd.read_sql(sql, conn)
        cur.close()
        conn.close()
        try:
            result = int(result_df['cost'].values)
        except:
            result = 80
        return result

    def get_ios_cost():
        conn = pymysql.connect(host=dicParam['DB_SLAVE_FENXI_HOST'], port=int(dicParam['DB_SLAVE_FENXI_PORT']), user=dicParam['DB_SLAVE_FENXI_USERNAME'],
                               passwd=dicParam['DB_SLAVE_FENXI_PASSWORD'])
        cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
        sql = '''
                    SELECT
                        sum( amount ) / sum( create_role_num ) AS cost 
                    FROM
                        db_stdata.st_lauch_report 
                    WHERE
                        tdate_type = 'day' 
                        AND tdate >= DATE_SUB( date( NOW()), INTERVAL 15 DAY ) 
                        AND media_id = 10 
                        AND platform = 2 
                        AND game_id IN (
                        SELECT
                            dev_game_id AS game_id 
                        FROM
                            db_data.t_game_config 
                        WHERE
                            game_id = 1136 
                        AND dev_game_id IS NOT NULL 
                        )
            '''
        cur.execute(sql)
        result_df = pd.read_sql(sql, conn)
        cur.close()
        conn.close()
        try:
            result = int(result_df['cost'].values)
        except:
            result = 160
        return result

    # 获取该游戏该媒体的双端创角成本
    cost_an = get_an_cost()
    cost_ios = get_ios_cost()

    # 1:关; 0:开； 3：加预算
    # win=0预判
    # 每次付费安卓
    if data_win_0_pre.shape[0] != 0:
        data_win_0_pre['label'] = data_win_0_pre.apply(
            lambda x: 1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount <= 1500) & (x.create_role_cost >= 2.5 * cost_an)
                   else (1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount > 1500) & (x.create_role_cost >= 2 * cost_an)
                         else (1 if (x.create_role_roi > 0) & (x.create_role_roi <= 0.025) & (x.create_role_cost >= 3 * cost_an)
                               else (1 if x.create_role_cost >= 3.5 * cost_an else 0))), axis=1)

        result_df = result_df.append(data_win_0_pre)

    # 每次付费ISO
    if data_win_0_ios_pre.shape[0] != 0:
        data_win_0_ios_pre['label'] = data_win_0_ios_pre.apply(
            lambda x: 1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount <= 1500) & (x.create_role_cost >= 2.5 * cost_ios)
                   else (1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount > 1500) & (x.create_role_cost >= 2 * cost_ios)
                         else (1 if (x.create_role_roi > 0) & (x.create_role_roi <= 0.024) & (x.create_role_cost >= 3 * cost_ios)
                               else (1 if x.create_role_cost >= 3.5 * cost_ios else 0))), axis=1)

        result_df = result_df.append(data_win_0_ios_pre)

    # 其它付费方式安卓
    if data_win_0.shape[0] != 0:
        data_win_0['label'] = data_win_0.apply(
            lambda x: 1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount <= 1500) & (x.create_role_cost >= 2 * cost_an)
                   else (1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount > 1500) & (x.create_role_cost >= 1.8 * cost_an)
                         else (1 if (x.create_role_roi > 0) & (x.create_role_roi <= 0.025) & (x.create_role_cost >= 2.5 * cost_an)
                               else (1 if x.create_role_cost >= 3 * cost_an else 0))), axis=1)

        result_df = result_df.append(data_win_0)

    # 其它付费方式IOS
    if data_win_0_ios.shape[0] != 0:
        data_win_0_ios['label'] = data_win_0_ios.apply(
            lambda x: 1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount <= 1500) & (x.create_role_cost >= 2 * cost_ios)
                   else (1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount > 1500) & (x.create_role_cost >= 1.8 * cost_ios)
                         else (1 if (x.create_role_roi > 0) & (x.create_role_roi <= 0.024) & (x.create_role_cost >= 2.5 * cost_ios)
                               else (1 if x.create_role_cost >= 3 * cost_ios else 0))), axis=1)

        result_df = result_df.append(data_win_0_ios)

    # 凌晨前关闭消耗大于2000且ROI<0.8%的计划
    # d_time = datetime.datetime.strptime(str(datetime.datetime.now().date()) + '23:40', '%Y-%m-%d%H:%M')
    # d_time1 = datetime.datetime.strptime(
    #     str((datetime.datetime.now() + datetime.timedelta(days=1)).date()) + '00:01',
    #     '%Y-%m-%d%H:%M')
    # n_time = datetime.datetime.now()
    # if n_time > d_time and n_time < d_time1 and result_df.shape[0] != 0:
    #     result_df['label'] = result_df.apply(
    #         lambda x: 1 if (x.source_run_date_amount >= 3000) & (x.create_role_roi <= 0.008) else x.label, axis=1)
    if result_df.shape[0] != 0:
        result_df_an = result_df[result_df['platform'] == 1]
        result_df_ios = result_df[result_df['platform'] == 2]
        result_df = pd.DataFrame()

        # 安卓加预算
        data_1 = result_df_an[result_df_an['source_run_date_amount'] <= 4200]
        data_2 = result_df_an[
            (result_df_an['source_run_date_amount'] > 4200) & (result_df_an['source_run_date_amount'] <= 9200)]
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
                lambda x: 5000 if (x.create_role_pay_sum >= 24) & (x.create_role_pay_num >= 2) else
                (5000 if x.create_role_pay_sum >= 48 else np.nan), axis=1)
            result_df = result_df.append(data_1)
        if data_2.shape[0] != 0:
            data_2['budget'] = data_2.apply(
                lambda x: 13000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_num >= 3) else np.nan, axis=1)
            result_df = result_df.append(data_2)
        if data_3.shape[0] != 0:
            data_3['budget'] = data_3.apply(
                lambda x: 23000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_num >= 4) else
                (23000 if x.create_role_roi >= 0.045 else np.nan), axis=1)
            result_df = result_df.append(data_3)
        if data_4.shape[0] != 0:
            data_4['budget'] = data_4.apply(
                lambda x: 50000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_cost <= 3500) else
                (50000 if (x.create_role_roi >= 0.045) & (x.create_role_pay_cost <= 4500) else
                 np.nan), axis=1)
            result_df = result_df.append(data_4)
        if data_5.shape[0] != 0:
            data_5['budget'] = data_5.apply(
                lambda x: 95000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_cost <= 3500) else
                (95000 if (x.create_role_roi >= 0.045) & (x.create_role_pay_cost <= 4500) else
                 np.nan), axis=1)
            result_df = result_df.append(data_5)
        if data_6.shape[0] != 0:
            data_6['budget'] = data_6.apply(
                lambda x: 148000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_cost <= 3000) else
                (148000 if (x.create_role_roi >= 0.045) & (x.create_role_pay_cost <= 4000) else
                 np.nan), axis=1)
            result_df = result_df.append(data_6)
        if data_7.shape[0] != 0:
            data_7['budget'] = data_7.apply(
                lambda x: 220000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_cost <= 3000) else
                (220000 if (x.create_role_roi >= 0.045) & (x.create_role_pay_cost <= 4000) else
                 np.nan), axis=1)
            result_df = result_df.append(data_7)
        if data_8.shape[0] != 0:
            data_8['budget'] = data_8.apply(
                lambda x: 275000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_cost <= 2800) else
                (275000 if (x.create_role_roi >= 0.045) & (x.create_role_pay_cost <= 3800) else
                 np.nan), axis=1)
            result_df = result_df.append(data_8)
        if data_9.shape[0] != 0:
            data_9['budget'] = data_9.apply(
                lambda x: 320000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_cost <= 2800) else
                (320000 if (x.create_role_roi >= 0.045) & (x.create_role_pay_cost <= 3800) else
                 np.nan), axis=1)
            result_df = result_df.append(data_9)
        if data_10.shape[0] != 0:
            data_10['budget'] = data_10.apply(
                lambda x: 450000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_cost <= 2800) else
                (450000 if (x.create_role_roi >= 0.045) & (x.create_role_pay_cost <= 3800) else
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
                lambda x: 8000 if (x.create_role_pay_sum >= 24) & (x.create_role_pay_num >= 2) else
                (8000 if x.create_role_pay_sum >= 48 else np.nan), axis=1)
            result_df = result_df.append(data_1_ios)
        if data_2_ios.shape[0] != 0:
            # data_2_ios['budget'] = data_2_ios.apply(
            #     lambda x: 13000 if (x.create_role_roi >= 0.028) & (x.create_role_pay_num >= 2) else
            #     (13000 if x.create_role_roi >= 0.04 else np.nan), axis=1)
            data_2_ios['budget'] = data_2_ios.apply(
                lambda x: 13000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_num >= 2) else np.nan, axis=1)
            result_df = result_df.append(data_2_ios)
        if data_3_ios.shape[0] != 0:
            # data_3_ios['budget'] = data_3_ios.apply(
            #     lambda x: 23000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_num >= 2) else
            #     np.nan, axis=1)
            data_3_ios['budget'] = data_3_ios.apply(
                lambda x: 23000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_num >= 3) else
                (23000 if x.create_role_roi >= 0.035 else np.nan), axis=1)
            result_df = result_df.append(data_3_ios)
        if data_4_ios.shape[0] != 0:
            data_4_ios['budget'] = data_4_ios.apply(
                lambda x: 50000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_cost <= 4500) else
                (50000 if (x.create_role_roi >= 0.035) & (x.create_role_pay_cost <= 5500) else
                 np.nan), axis=1)
            result_df = result_df.append(data_4_ios)
        if data_5_ios.shape[0] != 0:
            data_5_ios['budget'] = data_5_ios.apply(
                lambda x: 95000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_cost <= 4500) else
                (95000 if (x.create_role_roi >= 0.035) & (x.create_role_pay_cost <= 5500) else
                 np.nan), axis=1)
            result_df = result_df.append(data_5_ios)
        if data_6_ios.shape[0] != 0:
            data_6_ios['budget'] = data_6_ios.apply(
                lambda x: 148000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_cost <= 4500) else
                (148000 if (x.create_role_roi >= 0.035) & (x.create_role_pay_cost <= 4500) else
                 np.nan), axis=1)
            result_df = result_df.append(data_6_ios)
        if data_7_ios.shape[0] != 0:
            data_7_ios['budget'] = data_7_ios.apply(
                lambda x: 220000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_cost <= 4000) else
                (220000 if (x.create_role_roi >= 0.35) & (x.create_role_pay_cost <= 5000) else
                 np.nan), axis=1)
            result_df = result_df.append(data_7_ios)
        if data_8_ios.shape[0] != 0:
            data_8_ios['budget'] = data_8_ios.apply(
                lambda x: 275000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_cost <= 4000) else
                (275000 if (x.create_role_roi >= 0.035) & (x.create_role_pay_cost <= 5000) else
                 np.nan), axis=1)
            result_df = result_df.append(data_8_ios)
        if data_9_ios.shape[0] != 0:
            data_9_ios['budget'] = data_9_ios.apply(
                lambda x: 320000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_cost <= 4000) else
                (320000 if (x.create_role_roi >= 0.035) & (x.create_role_pay_cost <= 5000) else
                 np.nan), axis=1)
            result_df = result_df.append(data_9_ios)
        if data_10_ios.shape[0] != 0:
            data_10_ios['budget'] = data_10_ios.apply(
                lambda x: 450000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_cost <= 4000) else
                (450000 if (x.create_role_roi >= 0.035) & (x.create_role_pay_cost <= 5000) else
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


# 大东家百度模型2
def ddj_source_predict_state_2_bd(jsondata):
    '''
    :param jsondata: {"columns": ["channel_id","source_id","plan_name","model_run_datetime","create_time",
    "media_id","game_id","platform","data_win","source_run_date_amount","create_role_num",
    "create_role_cost","create_role_pay_num","create_role_pay_cost","create_role_pay_sum","create_role_roi",
    "create_role_retain_1d","create_role_pay_rate","create_role_pay_num_cum","learning_type","learning_time_dt",
    "learning_time_hr","deep_bid_type","cum_amount","cum_day","cum_role_cost"]}
    :return:
    '''
    data = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])

    # 数据预处理
    data = data[data['source_run_date_amount'] > 0]  # 删除消耗为0的数据
    # 去重，处理某天没有消耗的情况
    data.sort_values(by='data_win', inplace=True)
    data.drop_duplicates(subset=['channel_id', 'source_id', 'source_run_date_amount', 'create_role_num'], keep='first'
                         , inplace=True)

    # 计划7天内总消耗不足一定额度，且总成本合格的计划，让他继续跑，先不做判断
    data_0 = data[(data['cum_amount'] >= 100) & (data['cum_amount'] <= 2500) & (data['cum_day'] <= 7) & (
            data['cum_role_cost'] <= 80) & (data['platform'] == 1)]
    data_1 = data[(data['cum_amount'] >= 100) & (data['cum_amount'] <= 4000) & (data['cum_day'] <= 7) & (
            data['cum_role_cost'] <= 150) & (data['platform'] == 2)]
    data_0 = data_0.append(data_1)
    data_0['label'] = data_0['cum_amount'].apply(lambda x: 0)

    data = data.drop(data_0.index).reset_index(drop=True)
    # 对窗口期消耗200元以下的不作判断，对数据按平台、win进行分类处理
    data_not_ignore = data[(data['source_run_date_amount'] >= 200) | (data['data_win'] == 1)]
    data_ignore = data.drop(data_not_ignore.index)

    # 对最近1日\2日 消耗过6000，且没有付费的计划进行强制关停 data_2
    data_2 = data_not_ignore[
        (data_not_ignore['data_win'] <= 2) & (data_not_ignore['source_run_date_amount'] >= 4000) & (
                    data_not_ignore['create_role_roi'] == 0)]
    data_not_ignore = data_not_ignore.drop(data_2.index)

    # 对data_not_ignore进行周期筛选,1-3窗口期，保留最大周期
    data_not_ignore = data_not_ignore[data_not_ignore['data_win'] <= 3]
    data_not_ignore.sort_values('data_win', ascending=False, inplace=True)
    data_not_ignore = data_not_ignore.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')

    def get_source_data():
        conn = connect(host=dicParam['HIVE_HOST'], port=int(dicParam['HIVE_PORT']), auth_mechanism=dicParam['HIVE_AUTH_MECHANISM'], user=dicParam['HIVE_USERNAME'],
                       password=dicParam['HIVE_PASSWORD'], database=dicParam['HIVE_DATABASE'])
        cursor = conn.cursor()
        sql = '''
            SELECT
                channel_id,
                source_id,
                7_amount as amount_7,
                7_roi as roi_7,
                data_win 
            FROM
                tmp_data.tmp_ra_media_source 
            WHERE
                dt = `current_date` () 
                AND mgame_id = 1136 
                AND media_id = 45
        '''
        cursor.execute(sql)
        result = as_pandas(cursor)

        # 关闭链接
        cursor.close()
        conn.close()
        return result

    # 计划评分数据获取
    source_data = get_source_data()
    source_data_3 = source_data[source_data['data_win'] == 3]
    source_data_7 = source_data[source_data['data_win'] == 7]

    # 计划评分数据与源数据进行拼接
    data_not_ignore = pd.merge(data_not_ignore, source_data_3.drop(['data_win'], axis=1),
                               on=['channel_id', 'source_id'], how='left')
    data_not_ignore = data_not_ignore.rename(columns={'amount_7': 'amount_3', 'roi_7': 'roi_3'})
    data_not_ignore = pd.merge(data_not_ignore, source_data_7.drop(['data_win'], axis=1),
                               on=['channel_id', 'source_id'], how='left')

    # 依据3_roi和7_roi对计划进行判断是否关停
    if data_not_ignore.shape[0] != 0:
        data_not_ignore['label'] = data_not_ignore.apply(lambda x: 1 if (x.amount_3 >= 5000) & ((x.create_role_pay_cost >= 5000) | (x.create_role_roi == 0))
                                                else (0 if x.roi_3 >= 0.065
                                                      else (0 if (x.roi_3 >= 0.05) & (x.amount_3 <= 3000)
                                                            else (0 if (x.roi_7 >= 0.11) & (x.roi_3 >= 0.04) else 1))), axis=1)
        data_not_ignore.drop(['amount_3', 'roi_3', 'amount_7', 'roi_7'], axis=1, inplace=True)
    else:
        data_not_ignore = pd.DataFrame(
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
        data_ignore['data_win'] = 9  # 9 代表是被ignore的计划，消耗太小

    # 1\2日消耗过4000，充值为0，赋值为1，同时data_win变为-1，以便后续按datawin大小筛选优先级
    if data_2.shape[0] != 0:
        data_2['label'] = 1
        data_2.sort_values('data_win', ascending=False, inplace=True)
        data_2 = data_2.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
        data_2['data_win'] = -1  # -1 代表是被data_0的计划，累积消耗太小

    # 7天内总消耗不足，成本合格的计划，直接开计划，账值为0，同时data_win变为-2，以便后续按datawin大小筛选优先级
    if data_0.shape[0] != 0:
        data_0.sort_values('data_win', ascending=False, inplace=True)
        data_0 = data_0.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
        data_0['data_win'] = -2  # -1 代表是被data_0的计划，累积消耗太小

    source_predict = pd.concat([data_not_ignore, data_ignore], axis=0)
    source_predict = pd.concat([source_predict, data_0], axis=0)
    source_predict = pd.concat([source_predict, data_2], axis=0)

    # 综合结果优先级： data_0   data_2  大于 data   大于  data_ignore
    source_predict.sort_values('data_win', ascending=True, inplace=True)
    source_predict = source_predict.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
    #     print(source_predict.head())
    source_predict['label'] = source_predict['label'].astype(int)
    # print(source_predict.columns.values.tolist())
    source_predict = source_predict.fillna('null')

    return {"columns": source_predict.columns.values.tolist(), "list": source_predict.values.tolist()}


# 大东家头条模型2
def ddj_source_predict_state_2(jsondata):
    '''
    :param jsondata: {"columns": ["channel_id","source_id","plan_name","model_run_datetime","create_time",
    "media_id","game_id","platform","data_win","source_run_date_amount","create_role_num",
    "create_role_cost","create_role_pay_num","create_role_pay_cost","create_role_pay_sum","create_role_roi",
    "create_role_retain_1d","create_role_pay_rate","create_role_pay_num_cum","learning_type","learning_time_dt",
    "learning_time_hr","deep_bid_type","cum_amount","cum_day","cum_role_cost"]}
    :return:
    '''
    data = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])

    # 数据预处理
    data = data[data['source_run_date_amount'] > 0]  # 删除消耗为0的数据
    # 去重，处理某天没有消耗的情况
    data.sort_values(by='data_win', inplace=True)
    data.drop_duplicates(subset=['channel_id', 'source_id', 'source_run_date_amount', 'create_role_num'], keep='first'
                         , inplace=True)

    # 计划7天内总消耗不足4000，且总成本合格的计划，让他继续跑，先不做判断
    data_0 = data[(data['cum_amount'] >= 100) & (data['cum_amount'] <= 3000) & (data['cum_day'] <= 7) & (
            data['cum_role_cost'] <= 160) & (data['platform'] == 1)]
    data_1 = data[(data['cum_amount'] >= 100) & (data['cum_amount'] <= 4000) & (data['cum_day'] <= 7) & (
            data['cum_role_cost'] <= 300) & (data['platform'] == 2)]
    data_0 = data_0.append(data_1)
    data_0['label'] = data_0['cum_amount'].apply(lambda x: 0)

    data = data.drop(data_0.index).reset_index(drop=True)
    # 对窗口期消耗200元以下的不作判断，对数据按平台、win进行分类处理
    data_not_ignore = data[(data['source_run_date_amount'] >= 200) | (data['data_win'] == 1)]
    data_ignore = data.drop(data_not_ignore.index)

    # 对最近1日\2日 消耗过6000，且没有付费的计划进行强制关停 data_2
    data_2 = data_not_ignore[
        (data_not_ignore['data_win'] <= 2) & (data_not_ignore['source_run_date_amount'] >= 5000) & (
                    data_not_ignore['create_role_roi'] == 0)]
    data_not_ignore = data_not_ignore.drop(data_2.index)

    # 对data_not_ignore进行周期筛选,1-3窗口期，保留最大周期
    data_not_ignore = data_not_ignore[data_not_ignore['data_win'] <= 3]
    data_not_ignore.sort_values('data_win', ascending=False, inplace=True)
    data_not_ignore = data_not_ignore.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')

    def get_source_data():
        conn = connect(host=dicParam['HIVE_HOST'], port=int(dicParam['HIVE_PORT']), auth_mechanism=dicParam['HIVE_AUTH_MECHANISM'], user=dicParam['HIVE_USERNAME'],
                       password=dicParam['HIVE_PASSWORD'], database=dicParam['HIVE_DATABASE'])
        cursor = conn.cursor()
        sql = '''
            SELECT
                channel_id,
                source_id,
                7_amount as amount_7,
                7_roi as roi_7,
                data_win 
            FROM
                tmp_data.tmp_ra_media_source 
            WHERE
                dt = `current_date` () 
                AND mgame_id = 1136 
                AND media_id = 10
        '''
        cursor.execute(sql)
        result = as_pandas(cursor)

        # 关闭链接
        cursor.close()
        conn.close()
        return result

    # 计划评分数据获取
    source_data = get_source_data()
    source_data_3 = source_data[source_data['data_win'] == 3]
    source_data_7 = source_data[source_data['data_win'] == 7]

    # 计划评分数据与源数据进行拼接
    data_not_ignore = pd.merge(data_not_ignore, source_data_3.drop(['data_win'], axis=1),
                               on=['channel_id', 'source_id'], how='left')
    data_not_ignore = data_not_ignore.rename(columns={'amount_7': 'amount_3', 'roi_7': 'roi_3'})
    data_not_ignore = pd.merge(data_not_ignore, source_data_7.drop(['data_win'], axis=1),
                               on=['channel_id', 'source_id'], how='left')

    # 依据3_roi和7_roi对计划进行判断是否关停
    if data_not_ignore.shape[0] != 0:
        data_not_ignore['label'] = data_not_ignore.apply(lambda x: 1 if (x.amount_3 >= 5000) & ((x.create_role_pay_cost >= 5000) | (x.create_role_roi == 0))
                                                else (0 if x.roi_3 >= 0.065
                                                      else (0 if (x.roi_3 >= 0.05) & (x.amount_3 <= 4000)
                                                            else (0 if (x.roi_7 >= 0.11) & (x.roi_3 >= 0.04) else 1))), axis=1)
        data_not_ignore.drop(['amount_3', 'roi_3', 'amount_7', 'roi_7'], axis=1, inplace=True)
    else:
        data_not_ignore = pd.DataFrame(
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
        data_ignore['data_win'] = 9  # 9 代表是被ignore的计划，消耗太小

    # 1\2日消耗过6000，充值为0，赋值为1，同时data_win变为-1，以便后续按datawin大小筛选优先级
    if data_2.shape[0] != 0:
        data_2['label'] = 1
        data_2.sort_values('data_win', ascending=False, inplace=True)
        data_2 = data_2.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
        data_2['data_win'] = -1  # -1 代表是被data_0的计划，累积消耗太小

    # 7天内总消耗不足4000，成本合格的计划，直接开计划，账值为0，同时data_win变为-2，以便后续按datawin大小筛选优先级
    if data_0.shape[0] != 0:
        data_0.sort_values('data_win', ascending=False, inplace=True)
        data_0 = data_0.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
        data_0['data_win'] = -2  # -1 代表是被data_0的计划，累积消耗太小

    source_predict = pd.concat([data_not_ignore, data_ignore], axis=0)
    source_predict = pd.concat([source_predict, data_0], axis=0)
    source_predict = pd.concat([source_predict, data_2], axis=0)

    # 综合结果优先级： data_0   data_2  大于 data   大于  data_ignore
    source_predict.sort_values('data_win', ascending=True, inplace=True)
    source_predict = source_predict.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
    #     print(source_predict.head())
    source_predict['label'] = source_predict['label'].astype(int)
    # print(source_predict.columns.values.tolist())
    source_predict = source_predict.fillna('null')

    return {"columns": source_predict.columns.values.tolist(), "list": source_predict.values.tolist()}

"""
七雄模型
"""


# 2021-11-24:双状态计划监控模型——七雄广点通
def qx_source_predict_state_1_gdt(jsondata):
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

    data_win_0 = data_not_ignore[data_not_ignore['platform'] == 1]
    data_win_0_ios = data_not_ignore[data_not_ignore['platform'] == 2]

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
            data_win_0_1['label'] = data_win_0_1.apply(
                lambda x: 1 if (((x.create_role_cost >= 800) & (x.source_run_date_amount <= 1200) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 1000) & (x.source_run_date_amount <= 1200) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_cost >= 700) & (x.source_run_date_amount > 1200) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 800) & (x.source_run_date_amount > 1200) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_roi < 0.025) & (x.source_run_date_amount >= 5000))) else (0
                          if (((x.create_role_cost <= 795) & (x.source_run_date_amount <= 1200) & (x.create_role_pay_sum == 0)) |
                              ((x.create_role_cost <= 995) & (x.source_run_date_amount <= 1200) & (x.create_role_pay_sum != 0)) |
                              ((x.create_role_cost <= 695) & (x.source_run_date_amount > 1200) & (x.create_role_pay_sum == 0)) |
                              ((x.create_role_cost <= 795) & (x.source_run_date_amount > 1200) & (x.create_role_pay_sum != 0)) |
                              (x.create_role_roi >= 0.025)) else 2), axis=1)

            result_df = result_df.append(data_win_0_1)

        if data_win_0_2.shape[0] != 0:
            data_win_0_2['label'] = data_win_0_2.apply(
                lambda x: 1 if (x.create_role_cost >= 600) else (1 if (x.create_role_roi <= 0.028)
                                                                      | (x.create_role_pay_cost >= 4000) else (
                    0 if (x.create_role_roi >= 0.028) else 2)), axis=1)
            result_df = result_df.append(data_win_0_2)

        if data_win_0_3.shape[0] != 0:
            data_win_0_3['label'] = data_win_0_3.apply(
                lambda x: 1 if (x.create_role_cost >= 600) else (1 if (x.create_role_roi <= 0.028)
                                                                      | (x.create_role_pay_cost >= 3500) else (
                    0 if (x.create_role_roi >= 0.028) else 2)), axis=1)
            result_df = result_df.append(data_win_0_3)

        if data_win_0_4.shape[0] != 0:
            data_win_0_4['label'] = data_win_0_4.apply(
                lambda x: 1 if (x.create_role_cost >= 550) else (1 if (x.create_role_roi <= 0.03)
                                                                      | (x.create_role_pay_cost >= 3500) else (
                    0 if (x.create_role_roi >= 0.03) else 2)), axis=1)
            result_df = result_df.append(data_win_0_4)

        if data_win_0_5.shape[0] != 0:
            data_win_0_5['label'] = data_win_0_5.apply(
                lambda x: 1 if (x.create_role_cost >= 550) else (1 if (x.create_role_roi <= 0.03)
                                                                      | (x.create_role_pay_cost >= 3500) else (
                    0 if (x.create_role_roi >= 0.03) else 2)), axis=1)
            result_df = result_df.append(data_win_0_5)

    # ISO    ## TODO 七雄暂时没有ios，数据参数未调整
    if data_win_0_ios.shape[0] != 0:
        data_win_0_ios_1 = data_win_0_ios[data_win_0_ios['source_run_date_amount'] <= 12000]
        data_win_0_ios_2 = data_win_0_ios[
            (data_win_0_ios['source_run_date_amount'] > 12000) & (data_win_0_ios['source_run_date_amount'] <= 18000)]
        data_win_0_ios_3 = data_win_0_ios[
            (data_win_0_ios['source_run_date_amount'] > 18000) & (data_win_0_ios['source_run_date_amount'] <= 30000)]
        data_win_0_ios_4 = data_win_0_ios[
            (data_win_0_ios['source_run_date_amount'] > 30000) & (data_win_0_ios['source_run_date_amount'] <= 55000)]
        data_win_0_ios_5 = data_win_0_ios[data_win_0_ios['source_run_date_amount'] > 55000]
        if data_win_0_ios_1.shape[0] != 0:
            data_win_0_ios_1['label'] = data_win_0_ios_1.apply(
                lambda x: 1 if (((x.create_role_cost >= 650) & (x.source_run_date_amount <= 1000) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 750) & (x.source_run_date_amount <= 1000) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_cost >= 400) & (x.source_run_date_amount > 1000) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 500) & (x.source_run_date_amount > 1000) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_roi < 0.01) & (x.source_run_date_amount >= 10000))) else (0
                          if (((x.create_role_cost <= 640) & (x.source_run_date_amount <= 1000) & (x.create_role_pay_sum == 0)) |
                              ((x.create_role_cost <= 740) & (x.source_run_date_amount <= 1000) & (x.create_role_pay_sum != 0)) |
                              ((x.create_role_cost <= 390) & (x.source_run_date_amount > 1000) & (x.create_role_pay_sum == 0)) |
                              ((x.create_role_cost <= 490) & (x.source_run_date_amount > 1000) & (x.create_role_pay_sum != 0)) |
                              (x.create_role_roi >= 0.01)) else 2), axis=1)
            result_df = result_df.append(data_win_0_ios_1)

        if data_win_0_ios_2.shape[0] != 0:
            data_win_0_ios_2['label'] = data_win_0_ios_2.apply(
                lambda x: 1 if (x.create_role_cost >= 400) else (1 if (x.create_role_roi <= 0.01)
                                                                      | (x.create_role_pay_cost >= 15000) else (
                    0 if (x.create_role_roi >= 0.01) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_2)

        if data_win_0_ios_3.shape[0] != 0:
            data_win_0_ios_3['label'] = data_win_0_ios_3.apply(
                lambda x: 1 if (x.create_role_cost >= 400) else (1 if (x.create_role_roi <= 0.01)
                                                                      | (x.create_role_pay_cost >= 15000) else (
                    0 if (x.create_role_roi >= 0.01) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_3)

        if data_win_0_ios_4.shape[0] != 0:
            data_win_0_ios_4['label'] = data_win_0_ios_4.apply(
                lambda x: 1 if (x.create_role_cost >= 400) else (1 if (x.create_role_roi <= 0.015)
                                                                      | (x.create_role_pay_cost >= 12000) else (
                    0 if (x.create_role_roi >= 0.015) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_4)

        if data_win_0_ios_5.shape[0] != 0:
            data_win_0_ios_5['label'] = data_win_0_ios_5.apply(
                lambda x: 1 if (x.create_role_cost >= 400) else (1 if (x.create_role_roi <= 0.02)
                                                                      | (x.create_role_pay_cost >= 10000) else (
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


# 七雄头条模型1
def qx_source_predict_state_1(jsondata):
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
            lambda x: 1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount <= 1000) & (x.create_role_cost >= 700)
               else (1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount > 1000) & (x.create_role_cost >= 600)
                     else (1 if (x.create_role_roi > 0) & (x.create_role_roi <= 0.02) & (x.create_role_cost >= 700)
                           else (1 if x.create_role_cost >= 900 else 0))), axis=1)

        result_df = result_df.append(data_win_0_pre)

    # 每次付费ISO  ## TODO 暂无IOS
    if data_win_0_ios_pre.shape[0] != 0:
        data_win_0_ios_pre['label'] = data_win_0_ios_pre.apply(
            lambda x: 1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount <= 1000) & (x.create_role_cost >= 250)
                else (1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount > 1000) & (x.create_role_cost >= 200)
                      else (1 if (x.create_role_roi <= 0.02) & (x.create_role_cost >= 300)
                            else (1 if x.create_role_cost >= 300 else 0))), axis=1)
        result_df = result_df.append(data_win_0_ios_pre)

    # 其它付费方式安卓
    if data_win_0.shape[0] != 0:
        data_win_0['label'] = data_win_0.apply(
            lambda x: 1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount <= 1000) & (x.create_role_cost >= 600)
                else (1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount > 1000) & (x.create_role_cost >= 500)
                      else (1 if (x.create_role_roi > 0) & (x.create_role_roi <= 0.02) & (x.create_role_cost >= 600)
                            else (1 if x.create_role_cost >= 700 else 0))), axis=1)

        result_df = result_df.append(data_win_0)

    # 其它付费方式IOS  ## TODO 暂无IOS
    if data_win_0_ios.shape[0] != 0:
        data_win_0_ios['label'] = data_win_0_ios.apply(
            lambda x: 1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount <= 1000) & (x.create_role_cost >= 200)
                else (1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount > 1000) & (x.create_role_cost >= 150)
                      else (1 if (x.create_role_roi <= 0.02) & (x.create_role_cost >= 250)
                            else (1 if x.create_role_cost >= 250 else 0))), axis=1)
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

        # 安卓加预算  ## 初始6000预算
        data_1 = result_df_an[result_df_an['source_run_date_amount'] <= 7500]
        data_2 = result_df_an[
            (result_df_an['source_run_date_amount'] > 7500) & (result_df_an['source_run_date_amount'] <= 12200)]
        data_3 = result_df_an[
            (result_df_an['source_run_date_amount'] > 12200) & (result_df_an['source_run_date_amount'] <= 21000)]
        data_4 = result_df_an[
            (result_df_an['source_run_date_amount'] > 21000) & (result_df_an['source_run_date_amount'] <= 43000)]
        data_5 = result_df_an[
            (result_df_an['source_run_date_amount'] > 43000) & (result_df_an['source_run_date_amount'] <= 79000)]
        data_6 = result_df_an[
            (result_df_an['source_run_date_amount'] > 79000) & (result_df_an['source_run_date_amount'] <= 120000)]
        data_7 = result_df_an[
            (result_df_an['source_run_date_amount'] > 120000) & (result_df_an['source_run_date_amount'] <= 190000)]
        data_8 = result_df_an[
            (result_df_an['source_run_date_amount'] > 190000) & (result_df_an['source_run_date_amount'] <= 240000)]
        data_9 = result_df_an[
            (result_df_an['source_run_date_amount'] > 240000) & (result_df_an['source_run_date_amount'] <= 290000)]
        data_10 = result_df_an[result_df_an['source_run_date_amount'] > 290000]

        if data_1.shape[0] != 0:
            data_1['budget'] = data_1.apply(
                lambda x: 11000 if (x.create_role_pay_sum >= 98) & (x.create_role_pay_num >= 2) else
                (11000 if x.create_role_pay_sum >= 148 else np.nan), axis=1)
            result_df = result_df.append(data_1)
        if data_2.shape[0] != 0:
            # data_2['budget'] = data_2.apply(
            #     lambda x: 13000 if (x.create_role_roi >= 0.028) & (x.create_role_pay_num >= 2) else
            #     (13000 if x.create_role_roi >= 0.038 else np.nan), axis=1)
            data_2['budget'] = data_2.apply(
                lambda x: 16000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_num >= 3) else np.nan, axis=1)
            result_df = result_df.append(data_2)
        if data_3.shape[0] != 0:
            data_3['budget'] = data_3.apply(
                lambda x: 23000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_num >= 4) else
                (23000 if x.create_role_roi >= 0.05 else np.nan), axis=1)
            # data_3['budget'] = data_3.apply(
            #     lambda x: 23000 if (x.create_role_roi >= 0.035) & (x.create_role_pay_num >= 2) else
            #     np.nan, axis=1)
            result_df = result_df.append(data_3)
        if data_4.shape[0] != 0:
            data_4['budget'] = data_4.apply(
                lambda x: 50000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_cost <= 4000) else
                (50000 if (x.create_role_roi >= 0.035) & (x.create_role_pay_cost <= 6000) else
                 np.nan), axis=1)
            result_df = result_df.append(data_4)
        if data_5.shape[0] != 0:
            data_5['budget'] = data_5.apply(
                lambda x: 95000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_cost <= 3500) else
                (95000 if (x.create_role_roi >= 0.035) & (x.create_role_pay_cost <= 5500) else
                 np.nan), axis=1)
            result_df = result_df.append(data_5)
        if data_6.shape[0] != 0:
            data_6['budget'] = data_6.apply(
                lambda x: 148000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_cost <= 3000) else
                (148000 if (x.create_role_roi >= 0.035) & (x.create_role_pay_cost <= 3500) else
                 np.nan), axis=1)
            result_df = result_df.append(data_6)
        if data_7.shape[0] != 0:
            data_7['budget'] = data_7.apply(
                lambda x: 220000 if (x.create_role_roi >= 0.031) & (x.create_role_pay_cost <= 3000) else
                (220000 if (x.create_role_roi >= 0.036) & (x.create_role_pay_cost <= 3500) else
                 np.nan), axis=1)
            result_df = result_df.append(data_7)
        if data_8.shape[0] != 0:
            data_8['budget'] = data_8.apply(
                lambda x: 275000 if (x.create_role_roi >= 0.032) & (x.create_role_pay_cost <= 3000) else
                (275000 if (x.create_role_roi >= 0.037) & (x.create_role_pay_cost <= 3300) else
                 np.nan), axis=1)
            result_df = result_df.append(data_8)
        if data_9.shape[0] != 0:
            data_9['budget'] = data_9.apply(
                lambda x: 320000 if (x.create_role_roi >= 0.032) & (x.create_role_pay_cost <= 3000) else
                (320000 if (x.create_role_roi >= 0.037) & (x.create_role_pay_cost <= 3300) else
                 np.nan), axis=1)
            result_df = result_df.append(data_9)
        if data_10.shape[0] != 0:
            data_10['budget'] = data_10.apply(
                lambda x: 450000 if (x.create_role_roi >= 0.033) & (x.create_role_pay_cost <= 3000) else
                (450000 if (x.create_role_roi >= 0.038) & (x.create_role_pay_cost <= 3300) else
                 np.nan), axis=1)
            result_df = result_df.append(data_10)

        # ios加预算   ## TODO 暂时不跑IOS
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


# 七雄广点通state2模型2
def qx_source_predict_state_2_gdt(jsondata):
    '''
    :param jsondata: {"columns": ["channel_id","source_id","plan_name","model_run_datetime","create_time",
    "media_id","game_id","platform","data_win","source_run_date_amount","create_role_num",
    "create_role_cost","create_role_pay_num","create_role_pay_cost","create_role_pay_sum","create_role_roi",
    "create_role_retain_1d","create_role_pay_rate","create_role_pay_num_cum","learning_type","learning_time_dt",
    "learning_time_hr","deep_bid_type","cum_amount","cum_day","cum_role_cost"]}
    :return:
    '''
    data = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])

    # 数据预处理
    data = data[data['source_run_date_amount'] > 0]  # 删除消耗为0的数据
    # 去重，处理某天没有消耗的情况
    data.sort_values(by='data_win', inplace=True)
    data.drop_duplicates(subset=['channel_id', 'source_id', 'source_run_date_amount', 'create_role_num'], keep='first'
                         , inplace=True)

    # 计划7天内总消耗不足4000，且总成本合格的计划，让他继续跑，先不做判断
    data_0 = data[(data['cum_amount'] >= 200) & (data['cum_amount'] <= 6000) & (data['cum_day'] <= 7) & (
                data['cum_role_cost'] <= 600) & (data['platform'] == 1)]
    data_1 = data[(data['cum_amount'] >= 200) & (data['cum_amount'] <= 10000) & (data['cum_day'] <= 7) & (
                data['cum_role_cost'] <= 700) & (data['platform'] == 2)]
    data_0 = data_0.append(data_1)
    data_0['label'] = data_0['cum_amount'].apply(lambda x: 0)

    data = data.drop(data_0.index).reset_index(drop=True)
    # 对窗口期消耗200元以下的不作判断，对数据按平台、win进行分类处理
    data_not_ignore = data[(data['source_run_date_amount'] >= 200) | (data['data_win'] == 1)]
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

    if data_win_1.shape[0] != 0:
        data_win_1['label'] = data_win_1.apply(lambda x: 1 if x.create_role_num == 0
        else (1 if (x.create_role_pay_cost > 4500) | (x.create_role_cost > 650)
              else (1 if x.create_role_roi < 0.028
                    else (0 if x.create_role_roi >= 0.028 else 2))), axis=1)

        result_win1 = result_win1.append(data_win_1[(data_win_1['label'] == 1) | (data_win_1['label'] == 0)])
        temp_win1 = temp_win1.append(data_win_1[data_win_1['label'] == 2])

    if data_win_1_ios.shape[0] != 0:
        data_win_1_ios['label'] = data_win_1_ios.apply(lambda x: 1 if x.create_role_num == 0
        else (1 if (x.create_role_pay_cost > 4500) | (x.create_role_cost > 700)
              else (1 if x.create_role_roi <= 0.026
                    else (0 if x.create_role_roi >= 0.026 else 2))), axis=1)

        result_win1 = result_win1.append(
            data_win_1_ios[(data_win_1_ios['label'] == 1) | (data_win_1_ios['label'] == 0)])
        temp_win1 = temp_win1.append(data_win_1_ios[data_win_1_ios['label'] == 2])

    # win=1 模型预测
    if temp_win1.shape[0] != 0:
        temp_win1['label'] = gbdt_b_win1.predict(temp_win1[['create_role_cost', 'create_role_pay_cost', 'create_role_roi',
                       'create_role_pay_rate']])
    result_win1_data = pd.concat([result_win1, temp_win1], axis=0)

    # win=2预判
    result_win2 = pd.DataFrame()
    temp_win2 = pd.DataFrame()

    if data_win_2.shape[0] != 0:
        data_win_2['label'] = data_win_2.apply(lambda x: 1 if x.create_role_num == 0
        else (1 if (x.create_role_pay_cost > 4000) | (x.create_role_cost > 700)
              else (3 if (x.create_role_roi == 0) & (x.source_run_date_amount >= 10000)
                    else (1 if x.create_role_roi <= 0.05
                          else (0 if x.create_role_roi > 0.05 else 2)))), axis=1)
        result_win2 = result_win2.append(
            data_win_2[(data_win_2['label'] == 1) | (data_win_2['label'] == 0) | (data_win_2['label'] == 3)])
        temp_win2 = temp_win2.append(data_win_2[data_win_2['label'] == 2])

    if data_win_2_ios.shape[0] != 0:
        data_win_2_ios['label'] = data_win_2_ios.apply(lambda x: 1 if x.create_role_num == 0
        else (1 if (x.create_role_pay_cost > 4000) | (x.create_role_cost > 700)
              else (3 if (x.create_role_roi == 0) & (x.source_run_date_amount >= 10000)
                    else (1 if x.create_role_roi <= 0.048
                          else (0 if x.create_role_roi > 0.048 else 2)))), axis=1)

        result_win2 = result_win2.append(
            data_win_2_ios[(data_win_2_ios['label'] == 1) | (data_win_2_ios['label'] == 0)])
        temp_win2 = temp_win2.append(data_win_2_ios[data_win_2_ios['label'] == 2])

    # win=2 模型预测
    if temp_win2.shape[0] != 0:
        temp_win2['label'] = gbdt_b_win2.predict(
            temp_win2[['create_role_cost', 'create_role_pay_cost', 'create_role_roi',
                       'create_role_pay_rate', 'create_role_retain_1d']])
    result_win2_data = pd.concat([result_win2, temp_win2], axis=0)

    # win=3预判
    result_win3 = pd.DataFrame()
    temp_win3 = pd.DataFrame()
    if data_win_3.shape[0] != 0:
        data_win_3['label'] = data_win_3.apply(lambda x: 1 if x.create_role_num == 0
        else (1 if (x.create_role_pay_cost > 4000) | (x.create_role_cost > 600)
              else (0 if (x.cum_day <= 7) & (x.source_run_date_amount <= 6000) & (x.cum_role_cost <= 600)
                    else (0 if (x.create_role_pay_cost <= 2500) & (x.create_role_pay_cost > 0)
                    else (0 if x.create_role_roi >= 0.065
                          else (1 if x.create_role_roi < 0.065 else 2))))), axis=1)

        result_win3 = result_win3.append(data_win_3[(data_win_3['label'] == 1) | (data_win_3['label'] == 0)])
        temp_win3 = temp_win3.append(data_win_3[data_win_3['label'] == 2])

    if data_win_3_ios.shape[0] != 0:
        data_win_3_ios['label'] = data_win_3_ios.apply(lambda x: 1 if x.create_role_num == 0
        else (1 if (x.create_role_pay_cost > 4000) | (x.create_role_cost > 600)
              else (0 if (x.cum_day <= 7) & (x.source_run_date_amount <= 6000) & (x.cum_role_cost <= 600)
                    else (0 if (x.create_role_pay_cost <= 2500) & (x.create_role_pay_cost > 0)
                    else (0 if x.create_role_roi >= 0.06
                          else (1 if x.create_role_roi < 0.06 else 2))))), axis=1)

        result_win3 = result_win3.append(
            data_win_3_ios[(data_win_3_ios['label'] == 1) | (data_win_3_ios['label'] == 0)])
        temp_win3 = temp_win3.append(data_win_3_ios[data_win_3_ios['label'] == 2])

    # win=3 模型预测
    if temp_win3.shape[0] != 0:
        temp_win3['label'] = gbdt_b_win3.predict(
            temp_win3[['create_role_cost', 'create_role_pay_cost', 'create_role_roi',
                       'create_role_pay_rate', 'create_role_retain_1d']])
    result_win3_data = pd.concat([result_win3, temp_win3], axis=0)

    result = pd.concat([result_win1_data, result_win2_data, result_win3_data], axis=0)
    if result.shape[0] != 0:
        result.sort_values('data_win', ascending=True, inplace=True)

    # 结果输出
    if result.shape[0] != 0:
        # 1\2\3有一个开，则开，除非win2=3
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
            #             result_1_2 = result_1_2[result_1_2['data_win'] == 1]
            result_1_2_3 = result_1_2_3.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='last')

            result_1_2_3_piv.drop(['label_1', 'label_2', 'label_3'], axis=1, inplace=True)
            source_predict = pd.merge(result_1_2_3, result_1_2_3_piv,
                                      on=['channel_id', 'source_id', 'model_run_datetime'],
                                      how='left')
            # 1\2有一个为开，则开
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
            #             result_1_2 = result_1_2[result_1_2['data_win'] == 1]
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

    # 消耗小的ignore，直接关停计划，赋值1，同时data_win变为9，以便后续按datawin大小筛选优先级
    if data_ignore.shape[0] != 0:
        data_ignore['label'] = 1
        data_ignore.sort_values('data_win', ascending=False, inplace=True)
        data_ignore = data_ignore.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
        data_ignore['data_win'] = 9  # 9 代表是被ignore的计划，消耗太小

    # 7天内总消耗不足4000，成本合格的计划，直接开计划，账值为0，同时data_win变为-1，以便后续按datawin大小筛选优先级
    if data_0.shape[0] != 0:
        data_0.sort_values('data_win', ascending=False, inplace=True)
        data_0 = data_0.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
        data_0['data_win'] = -1  # -1 代表是被data_0的计划，累积消耗太小

    source_predict = pd.concat([source_predict, data_ignore], axis=0)
    source_predict = pd.concat([source_predict, data_0], axis=0)

    # 综合结果优先级： data_0    大于 data   大于  data_ignore
    source_predict.sort_values('data_win', ascending=True, inplace=True)
    source_predict = source_predict.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
    #     print(source_predict.head())
    source_predict['label'] = source_predict['label'].astype(int)
    # print(source_predict.columns.values.tolist())
    source_predict = source_predict.fillna('null')

    return {"columns": source_predict.columns.values.tolist(), "list": source_predict.values.tolist()}


# 七雄头条模型2
def qx_source_predict_state_2(jsondata):
    '''
    :param jsondata: {"columns": ["channel_id","source_id","plan_name","model_run_datetime","create_time",
    "media_id","game_id","platform","data_win","source_run_date_amount","create_role_num",
    "create_role_cost","create_role_pay_num","create_role_pay_cost","create_role_pay_sum","create_role_roi",
    "create_role_retain_1d","create_role_pay_rate","create_role_pay_num_cum","learning_type","learning_time_dt",
    "learning_time_hr","deep_bid_type","cum_amount","cum_day","cum_role_cost"]}
    :return:
    '''
    data = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])

    # 数据预处理
    data = data[data['source_run_date_amount'] > 0]  # 删除消耗为0的数据
    # 去重，处理某天没有消耗的情况
    data.sort_values(by='data_win', inplace=True)
    data.drop_duplicates(subset=['channel_id', 'source_id', 'source_run_date_amount', 'create_role_num'], keep='first'
                         , inplace=True)

    # 计划7天内总消耗不足4000，且总成本合格的计划，让他继续跑，先不做判断
    data_0 = data[(data['cum_amount'] >= 200) & (data['cum_amount'] <= 6000) & (data['cum_day'] <= 7) & (
                data['cum_role_cost'] <= 600) & (data['platform'] == 1)]
    data_1 = data[(data['cum_amount'] >= 200) & (data['cum_amount'] <= 8000) & (data['cum_day'] <= 7) & (
                data['cum_role_cost'] <= 700) & (data['platform'] == 2)]
    data_0 = data_0.append(data_1)
    data_0['label'] = data_0['cum_amount'].apply(lambda x: 0)

    data = data.drop(data_0.index).reset_index(drop=True)
    # 对窗口期消耗200元以下的不作判断，对数据按平台、win进行分类处理
    data_not_ignore = data[(data['source_run_date_amount'] >= 200) | (data['data_win'] == 1)]
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
        data_win_1_pre_action['label'] = data_win_1_pre_action.apply(lambda x: 1 if x.create_role_num == 0
        else (1 if (x.create_role_pay_cost > 4500) | (x.create_role_cost > 650)
              else (1 if x.create_role_roi < 0.03
                    else (0 if x.create_role_roi >= 0.03 else 2))), axis=1)

        result_win1 = result_win1.append(
            data_win_1_pre_action[(data_win_1_pre_action['label'] == 1) | (data_win_1_pre_action['label'] == 0)])
        temp_win1 = temp_win1.append(data_win_1_pre_action[data_win_1_pre_action['label'] == 2])

    if data_win_1.shape[0] != 0:
        data_win_1['label'] = data_win_1.apply(lambda x: 1 if x.create_role_num == 0
        else (1 if (x.create_role_pay_cost > 4500) | (x.create_role_cost > 500)
              else (1 if x.create_role_roi < 0.03
                    else (0 if x.create_role_roi >= 0.03 else 2))), axis=1)

        result_win1 = result_win1.append(data_win_1[(data_win_1['label'] == 1) | (data_win_1['label'] == 0)])
        temp_win1 = temp_win1.append(data_win_1[data_win_1['label'] == 2])

    data_win_1_ios_pre_action = data_win_1_ios[data_win_1_ios['deep_bid_type'] == 'BID_PER_ACTION']
    data_win_1_ios = data_win_1_ios[data_win_1_ios['deep_bid_type'] != 'BID_PER_ACTION']

    if data_win_1_ios_pre_action.shape[0] != 0:
        data_win_1_ios_pre_action['label'] = data_win_1_ios_pre_action.apply(lambda x: 1 if x.create_role_num == 0
        else (1 if (x.create_role_pay_cost > 5000) | (x.create_role_cost > 700)
              else (1 if x.create_role_roi < 0.028
                    else (0 if x.create_role_roi >= 0.028 else 2))), axis=1)

        result_win1 = result_win1.append(data_win_1_ios_pre_action[(data_win_1_ios_pre_action['label'] == 1) | (
                data_win_1_ios_pre_action['label'] == 0)])
        temp_win1 = temp_win1.append(data_win_1_ios_pre_action[data_win_1_ios_pre_action['label'] == 2])

    if data_win_1_ios.shape[0] != 0:
        data_win_1_ios['label'] = data_win_1_ios.apply(lambda x: 1 if x.create_role_num == 0
        else (1 if (x.create_role_pay_cost > 5000) | (x.create_role_cost > 600)
              else (1 if x.create_role_roi < 0.028
                    else (0 if x.create_role_roi >= 0.028 else 2))), axis=1)

        result_win1 = result_win1.append(
            data_win_1_ios[(data_win_1_ios['label'] == 1) | (data_win_1_ios['label'] == 0)])
        temp_win1 = temp_win1.append(data_win_1_ios[data_win_1_ios['label'] == 2])

    # win=1 模型预测
    if temp_win1.shape[0] != 0:
        temp_win1['label'] = gbdt_b_win1.predict(
            temp_win1[['create_role_cost', 'create_role_pay_cost', 'create_role_roi',
                       'create_role_pay_rate']])
    result_win1_data = pd.concat([result_win1, temp_win1], axis=0)

    # win=2预判
    result_win2 = pd.DataFrame()
    temp_win2 = pd.DataFrame()

    if data_win_2.shape[0] != 0:
        data_win_2['label'] = data_win_2.apply(lambda x: 1 if x.create_role_num == 0
        else (1 if (x.create_role_pay_cost > 4500) | (x.create_role_cost > 600)
              else (3 if (x.create_role_roi == 0) & (x.source_run_date_amount >= 7000)
                    else (1 if x.create_role_roi <= 0.05
                          else (0 if x.create_role_roi > 0.05 else 2)))), axis=1)
        result_win2 = result_win2.append(
            data_win_2[(data_win_2['label'] == 1) | (data_win_2['label'] == 0) | (data_win_2['label'] == 3)])
        temp_win2 = temp_win2.append(data_win_2[data_win_2['label'] == 2])

    if data_win_2_ios.shape[0] != 0:
        data_win_2_ios['label'] = data_win_2_ios.apply(lambda x: 1 if x.create_role_num == 0
        else (1 if (x.create_role_pay_cost > 4500) | (x.create_role_cost > 600)
              else (3 if (x.create_role_roi == 0) & (x.source_run_date_amount >= 7000)
                    else (1 if x.create_role_roi <= 0.05
                          else (0 if x.create_role_roi > 0.05 else 2)))), axis=1)

        result_win2 = result_win2.append(
            data_win_2_ios[(data_win_2_ios['label'] == 1) | (data_win_2_ios['label'] == 0)])
        temp_win2 = temp_win2.append(data_win_2_ios[data_win_2_ios['label'] == 2])

    # win=2 模型预测
    if temp_win2.shape[0] != 0:
        temp_win2['label'] = gbdt_b_win2.predict(
            temp_win2[['create_role_cost', 'create_role_pay_cost', 'create_role_roi',
                       'create_role_pay_rate', 'create_role_retain_1d']])
    result_win2_data = pd.concat([result_win2, temp_win2], axis=0)

    # win=3预判
    result_win3 = pd.DataFrame()
    temp_win3 = pd.DataFrame()
    if data_win_3.shape[0] != 0:
        data_win_3['label'] = data_win_3.apply(lambda x: 1 if x.create_role_num == 0
        else (1 if (x.create_role_pay_cost > 4500) | (x.create_role_cost > 600)
              else (0 if (x.cum_day <= 7) & (x.source_run_date_amount <= 6000) & (x.cum_role_cost <= 600)
                    else (0 if (x.create_role_pay_cost <= 2500) & (x.create_role_pay_cost > 0)
                    else (0 if x.create_role_roi >= 0.075
                          else (1 if x.create_role_roi < 0.075 else 2))))), axis=1)

        result_win3 = result_win3.append(data_win_3[(data_win_3['label'] == 1) | (data_win_3['label'] == 0)])
        temp_win3 = temp_win3.append(data_win_3[data_win_3['label'] == 2])

    if data_win_3_ios.shape[0] != 0:
        data_win_3_ios['label'] = data_win_3_ios.apply(lambda x: 1 if x.create_role_num == 0
        else (1 if (x.create_role_pay_cost > 4500) | (x.create_role_cost > 600)
              else (0 if (x.cum_day <= 7) & (x.source_run_date_amount <= 6000) & (x.cum_role_cost <= 600)
                    else (0 if (x.create_role_pay_cost <= 2500) & (x.create_role_pay_cost > 0)
                    else (0 if x.create_role_roi >= 0.075
                          else (1 if x.create_role_roi < 0.075 else 2))))), axis=1)

        result_win3 = result_win3.append(
            data_win_3_ios[(data_win_3_ios['label'] == 1) | (data_win_3_ios['label'] == 0)])
        temp_win3 = temp_win3.append(data_win_3_ios[data_win_3_ios['label'] == 2])

    # win=3 模型预测
    if temp_win3.shape[0] != 0:
        temp_win3['label'] = gbdt_b_win3.predict(
            temp_win3[['create_role_cost', 'create_role_pay_cost', 'create_role_roi',
                       'create_role_pay_rate', 'create_role_retain_1d']])
    result_win3_data = pd.concat([result_win3, temp_win3], axis=0)

    result = pd.concat([result_win1_data, result_win2_data, result_win3_data], axis=0)
    if result.shape[0] != 0:
        result.sort_values('data_win', ascending=True, inplace=True)

    # 结果输出
    if result.shape[0] != 0:
        # 1\2\3有一个开，则开，除非win2=3
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
            #             result_1_2 = result_1_2[result_1_2['data_win'] == 1]
            result_1_2_3 = result_1_2_3.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='last')

            result_1_2_3_piv.drop(['label_1', 'label_2', 'label_3'], axis=1, inplace=True)
            source_predict = pd.merge(result_1_2_3, result_1_2_3_piv,
                                      on=['channel_id', 'source_id', 'model_run_datetime'],
                                      how='left')
            # 1\2有一个为开，则开
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
            #             result_1_2 = result_1_2[result_1_2['data_win'] == 1]
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

    # 消耗小的ignore，直接关停计划，赋值1，同时data_win变为9，以便后续按datawin大小筛选优先级
    if data_ignore.shape[0] != 0:
        data_ignore['label'] = 1
        data_ignore.sort_values('data_win', ascending=False, inplace=True)
        data_ignore = data_ignore.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
        data_ignore['data_win'] = 9  # 9 代表是被ignore的计划，消耗太小

    # 7天内总消耗不足4000，成本合格的计划，直接开计划，账值为0，同时data_win变为-1，以便后续按datawin大小筛选优先级
    if data_0.shape[0] != 0:
        data_0.sort_values('data_win', ascending=False, inplace=True)
        data_0 = data_0.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
        data_0['data_win'] = -1  # -1 代表是被data_0的计划，累积消耗太小

    source_predict = pd.concat([source_predict, data_ignore], axis=0)
    source_predict = pd.concat([source_predict, data_0], axis=0)

    # 综合结果优先级： data_0    大于 data   大于  data_ignore
    source_predict.sort_values('data_win', ascending=True, inplace=True)
    source_predict = source_predict.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
    #     print(source_predict.head())
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

    # 获取近15天平均创角成本
    def get_an_cost():
        conn = pymysql.connect(host=dicParam['DB_SLAVE_FENXI_HOST'], port=int(dicParam['DB_SLAVE_FENXI_PORT']), user=dicParam['DB_SLAVE_FENXI_USERNAME'],
                               passwd=dicParam['DB_SLAVE_FENXI_PASSWORD'])
        cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
        sql = '''
                    SELECT
                        sum( amount ) / sum( create_role_num ) AS cost 
                    FROM
                        db_stdata.st_lauch_report 
                    WHERE
                        tdate_type = 'day' 
                        AND tdate >= DATE_SUB( date( NOW()), INTERVAL 15 DAY ) 
                        AND media_id = 16 
                        AND platform = 1 
                        AND game_id IN (
                        SELECT
                            dev_game_id AS game_id 
                        FROM
                            db_data.t_game_config 
                        WHERE
                            game_id = 1112 
                        AND dev_game_id IS NOT NULL 
                        )
            '''
        cur.execute(sql)
        result_df = pd.read_sql(sql, conn)
        cur.close()
        conn.close()
        try:
            result = int(result_df['cost'].values)
        except:
            result = 200
        return result

    def get_ios_cost():
        conn = pymysql.connect(host=dicParam['DB_SLAVE_FENXI_HOST'], port=int(dicParam['DB_SLAVE_FENXI_PORT']), user=dicParam['DB_SLAVE_FENXI_USERNAME'],
                               passwd=dicParam['DB_SLAVE_FENXI_PASSWORD'])
        cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
        sql = '''
                    SELECT
                        sum( amount ) / sum( create_role_num ) AS cost 
                    FROM
                        db_stdata.st_lauch_report 
                    WHERE
                        tdate_type = 'day' 
                        AND tdate >= DATE_SUB( date( NOW()), INTERVAL 15 DAY ) 
                        AND media_id = 16 
                        AND platform = 2 
                        AND game_id IN (
                        SELECT
                            dev_game_id AS game_id 
                        FROM
                            db_data.t_game_config 
                        WHERE
                            game_id = 1112 
                        AND dev_game_id IS NOT NULL 
                        )
            '''
        cur.execute(sql)
        result_df = pd.read_sql(sql, conn)
        cur.close()
        conn.close()
        try:
            result = int(result_df['cost'].values)
        except:
            result = 380
        return result

    # 获取该游戏该媒体的双端创角成本
    cost_an = get_an_cost()
    cost_ios = get_ios_cost()


    result_df = pd.DataFrame()

    # 1:关; 0:开； 2：保持原状
    # win=0预判
    if data_win_0.shape[0] != 0:
        data_win_0_1 = data_win_0[data_win_0['source_run_date_amount'] <= 13000]
        data_win_0_2 = data_win_0[
            (data_win_0['source_run_date_amount'] > 13000) & (data_win_0['source_run_date_amount'] <= 18000)]
        data_win_0_3 = data_win_0[
            (data_win_0['source_run_date_amount'] > 18000) & (data_win_0['source_run_date_amount'] <= 31000)]
        data_win_0_4 = data_win_0[
            (data_win_0['source_run_date_amount'] > 31000) & (data_win_0['source_run_date_amount'] <= 56000)]
        data_win_0_5 = data_win_0[data_win_0['source_run_date_amount'] > 56000]
        if data_win_0_1.shape[0] != 0:
            # data_win_0_1['label'] = data_win_0_1.apply(
            #     lambda x: 1 if (x.create_role_cost >= 200) & (x.deep_bid_type == 'BID_PER_ACTION') |
            #                    (x.create_role_cost >= 100) & (x.deep_bid_type != 'BID_PER_ACTION') else (
            #         1 if (x.create_role_roi <= 0.02)
            #              & (x.source_run_date_amount >= 3500) else (0 if (x.create_role_roi >= 0.03) else 2)), axis=1)

            data_win_0_1['label'] = data_win_0_1.apply(
                lambda x: 1 if (((x.create_role_cost >= 2.5 * cost_an) & (x.source_run_date_amount <= 1300) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 3.5 * cost_an) & (x.source_run_date_amount <= 1300) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_cost >= 2 * cost_an) & (x.source_run_date_amount > 1300) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 2.5 * cost_an) & (x.source_run_date_amount > 1300) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_roi < 0.01) & (x.source_run_date_amount >= 6000))) else (0
                             if ((( x.create_role_cost <= (2.5 * cost_an - 10)) & (x.source_run_date_amount <= 1300) & (x.create_role_pay_sum == 0)) |
                                 (( x.create_role_cost <= (3.5 * cost_an - 10)) & (x.source_run_date_amount <= 1300) & (x.create_role_pay_sum != 0)) |
                                 (( x.create_role_cost <= (2 * cost_an - 10)) & (x.source_run_date_amount > 1300) & (x.create_role_pay_sum == 0)) |
                                 (( x.create_role_cost <= (2.5 * cost_an - 10)) & (x.source_run_date_amount > 1300) & (x.create_role_pay_sum != 0)) |
                                 (x.create_role_roi >= 0.01)) else 2), axis=1)

            result_df = result_df.append(data_win_0_1)

        if data_win_0_2.shape[0] != 0:
            data_win_0_2['label'] = data_win_0_2.apply(
                lambda x: 1 if (x.create_role_cost >= 2.5 * cost_an)
                                else (1 if (x.create_role_roi <= 0.014) | (x.create_role_pay_cost >= 7000)
                                      else (0 if (x.create_role_roi >= 0.015) else 2)), axis=1)
            result_df = result_df.append(data_win_0_2)

        if data_win_0_3.shape[0] != 0:
            data_win_0_3['label'] = data_win_0_3.apply(
                lambda x: 1 if (x.create_role_cost >= 2.5 * cost_an)
                                else (1 if (x.create_role_roi <= 0.014) | (x.create_role_pay_cost >= 7000)
                                      else (0 if (x.create_role_roi >= 0.015) else 2)), axis=1)
            result_df = result_df.append(data_win_0_3)

        if data_win_0_4.shape[0] != 0:
            data_win_0_4['label'] = data_win_0_4.apply(
                lambda x: 1 if (x.create_role_cost >= 2.3 * cost_an)
                                else (1 if (x.create_role_roi <= 0.015) | (x.create_role_pay_cost >= 6000)
                                      else (0 if (x.create_role_roi >= 0.016) else 2)), axis=1)
            result_df = result_df.append(data_win_0_4)

        if data_win_0_5.shape[0] != 0:
            data_win_0_5['label'] = data_win_0_5.apply(
                lambda x: 1 if (x.create_role_cost >=  2 * cost_an)
                                else (1 if (x.create_role_roi <= 0.015) | (x.create_role_pay_cost >= 5500)
                                      else (0 if (x.create_role_roi >= 0.016) else 2)), axis=1)
            result_df = result_df.append(data_win_0_5)

    # ISO
    if data_win_0_ios.shape[0] != 0:
        data_win_0_ios_1 = data_win_0_ios[data_win_0_ios['source_run_date_amount'] <= 15000]
        data_win_0_ios_2 = data_win_0_ios[
            (data_win_0_ios['source_run_date_amount'] > 15000) & (data_win_0_ios['source_run_date_amount'] <= 21000)]
        data_win_0_ios_3 = data_win_0_ios[
            (data_win_0_ios['source_run_date_amount'] > 21000) & (data_win_0_ios['source_run_date_amount'] <= 33000)]
        data_win_0_ios_4 = data_win_0_ios[
            (data_win_0_ios['source_run_date_amount'] > 33000) & (data_win_0_ios['source_run_date_amount'] <= 58000)]
        data_win_0_ios_5 = data_win_0_ios[data_win_0_ios['source_run_date_amount'] > 58000]
        if data_win_0_ios_1.shape[0] != 0:
            data_win_0_ios_1['label'] = data_win_0_ios_1.apply(
                lambda x: 1 if (((x.create_role_cost >= 2.2 * cost_ios) & (x.source_run_date_amount <= 1300) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 3 * cost_ios) & (x.source_run_date_amount <= 1300) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_cost >= 1.8 * cost_ios) & (x.source_run_date_amount > 1300) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 2.2 * cost_ios) & (x.source_run_date_amount > 1300) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_roi < 0.013) & (x.source_run_date_amount >= 8000))) else (0
                          if (((x.create_role_cost <= (2.2 * cost_ios - 10)) & (x.source_run_date_amount <= 1300) & (x.create_role_pay_sum == 0)) |
                              ((x.create_role_cost <= (3 * cost_ios - 10)) & (x.source_run_date_amount <= 1300) & (x.create_role_pay_sum != 0)) |
                              ((x.create_role_cost <= (1.8 * cost_ios - 10)) & (x.source_run_date_amount > 1300) & (x.create_role_pay_sum == 0)) |
                              ((x.create_role_cost <= (2.2 * cost_ios - 10)) & (x.source_run_date_amount > 1300) & (x.create_role_pay_sum != 0)) |
                              (x.create_role_roi >= 0.013)) else 2), axis=1)

            result_df = result_df.append(data_win_0_ios_1)

        if data_win_0_ios_2.shape[0] != 0:
            data_win_0_ios_2['label'] = data_win_0_ios_2.apply(
                lambda x: 1 if (x.create_role_cost >= 2.2 * cost_ios)
                            else (1 if (x.create_role_roi <= 0.015) | (x.create_role_pay_cost >= 5000)
                                  else (0 if (x.create_role_roi >= 0.016) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_2)

        if data_win_0_ios_3.shape[0] != 0:
            data_win_0_ios_3['label'] = data_win_0_ios_3.apply(
                lambda x: 1 if (x.create_role_cost >= 2.2 * cost_ios)
                            else (1 if (x.create_role_roi <= 0.016) | (x.create_role_pay_cost >= 5000)
                                  else (0 if (x.create_role_roi >= 0.017) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_3)

        if data_win_0_ios_4.shape[0] != 0:
            data_win_0_ios_4['label'] = data_win_0_ios_4.apply(
                lambda x: 1 if (x.create_role_cost >= 2.2 * cost_ios)
                            else (1 if (x.create_role_roi <= 0.017) | (x.create_role_pay_cost >= 5000)
                                  else (0 if (x.create_role_roi >= 0.018) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_4)

        if data_win_0_ios_5.shape[0] != 0:
            data_win_0_ios_5['label'] = data_win_0_ios_5.apply(
                lambda x: 1 if (x.create_role_cost >= 2.2 * cost_ios)
                            else (1 if (x.create_role_roi <= 0.018) | (x.create_role_pay_cost >= 5000)
                                  else (0 if (x.create_role_roi >= 0.019) else 2)), axis=1)
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


# 帝国百度模型1
def source_predict_state_1_dg_bd(jsondata):
    '''
    :param jsondata: {"columns": ["channel_id","source_id","plan_name", "model_run_datetime","create_time","media_id",
    "game_id","platform","source_run_date_amount","create_role_num","create_role_cost","create_role_pay_num",
    "create_role_pay_cost","create_role_pay_sum","create_role_roi","create_role_pay_rate","create_role_pay_num_cum",
    "learning_type","learning_time_dt","learning_time_hr","deep_bid_type"]}
    :return:'label','budget'
    '''

    # 该模型只能接受计划状态plan_run_state=1且data_win=0 且media_id=45 且 model_run_datetime 等于当日的数据，即只接受当日广点通实时数据
    # label为1时关计划，为0时开计划，如果模型输出label结果与计划当前状况（开/关）相同，则不操作。例如，计划当前是关停状况，当模型输出为1时，不操作

    # 早上7：55分，将所有在运行的计划状态plan_run_state改为2。调用模型2进行判断，是否达标。达标则状态改为1，开计划，预算由模型1计算决定。不达标，则为关停状态，且plan_run_state为2

    data = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])
    data = data[data['media_id'] == 45]
    # 数据预处理
    data = data[data['source_run_date_amount'] > 0]  # 删除消耗为0的数据

    # 对消耗300元以下的不作判断，对数据按平台、win进行分类处理
    data_not_ignore = data[data['source_run_date_amount'] >= 300]

    data_win_0 = data_not_ignore[data_not_ignore['platform'] == 1]
    data_win_0_ios = data_not_ignore[data_not_ignore['platform'] == 2]

    result_df = pd.DataFrame()

    # 获取近15天平均创角成本
    def get_an_cost():
        conn = pymysql.connect(host=dicParam['DB_SLAVE_FENXI_HOST'], port=int(dicParam['DB_SLAVE_FENXI_PORT']), user=dicParam['DB_SLAVE_FENXI_USERNAME'],
                               passwd=dicParam['DB_SLAVE_FENXI_PASSWORD'])
        cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
        sql = '''
                SELECT
                    sum( amount ) / sum( create_role_num ) AS cost 
                FROM
                    db_stdata.st_lauch_report 
                WHERE
                    tdate_type = 'day' 
                    AND tdate >= DATE_SUB( date( NOW()), INTERVAL 15 DAY ) 
                    AND media_id = 45 
                    AND platform = 1 
                    AND game_id IN (
                    SELECT
                        dev_game_id AS game_id 
                    FROM
                        db_data.t_game_config 
                    WHERE
                        game_id = 1112 
                    AND dev_game_id IS NOT NULL 
                    )
        '''
        cur.execute(sql)
        result_df = pd.read_sql(sql, conn)
        cur.close()
        conn.close()
        try:
            result = int(result_df['cost'].values)
        except:
            result = 100
        return result

    def get_ios_cost():
        conn = pymysql.connect(host=dicParam['DB_SLAVE_FENXI_HOST'], port=int(dicParam['DB_SLAVE_FENXI_PORT']), user=dicParam['DB_SLAVE_FENXI_USERNAME'],
                               passwd=dicParam['DB_SLAVE_FENXI_PASSWORD'])
        cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
        sql = '''
                SELECT
                    sum( amount ) / sum( create_role_num ) AS cost 
                FROM
                    db_stdata.st_lauch_report 
                WHERE
                    tdate_type = 'day' 
                    AND tdate >= DATE_SUB( date( NOW()), INTERVAL 15 DAY ) 
                    AND media_id = 45 
                    AND platform = 2 
                    AND game_id IN (
                    SELECT
                        dev_game_id AS game_id 
                    FROM
                        db_data.t_game_config 
                    WHERE
                        game_id = 1112 
                    AND dev_game_id IS NOT NULL 
                    )
        '''
        cur.execute(sql)
        result_df = pd.read_sql(sql, conn)
        cur.close()
        conn.close()
        try:
            result = int(result_df['cost'].values)
        except:
            result = 200
        return result
    # 获取该游戏该媒体的双端创角成本
    cost_an = get_an_cost()
    cost_ios = get_ios_cost()

    # 1:关; 0:开； 2：保持原状
    # win=0预判
    if data_win_0.shape[0] != 0:
        data_win_0_1 = data_win_0[data_win_0['source_run_date_amount'] <= 6000]
        data_win_0_2 = data_win_0[(data_win_0['source_run_date_amount'] > 6000) & (data_win_0['source_run_date_amount'] <= 12000)]
        data_win_0_3 = data_win_0[(data_win_0['source_run_date_amount'] > 12000) & (data_win_0['source_run_date_amount'] <= 22000)]
        data_win_0_4 = data_win_0[(data_win_0['source_run_date_amount'] > 22000) & (data_win_0['source_run_date_amount'] <= 50000)]
        data_win_0_5 = data_win_0[data_win_0['source_run_date_amount'] > 50000]
        if data_win_0_1.shape[0] != 0:
            data_win_0_1['label'] = data_win_0_1.apply(
                lambda x: 1 if (((x.create_role_cost >= 2.5 * cost_an) & (x.source_run_date_amount <= 800) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 3.5 * cost_an) & (x.source_run_date_amount <= 800) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_cost >= 2 * cost_an) & (x.source_run_date_amount > 800) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 2.5 * cost_an) & (x.source_run_date_amount > 800) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_roi < 0.01) & (x.source_run_date_amount >= 3000))) else (0
                             if ((( x.create_role_cost <= (2.5 * cost_an - 10)) & (x.source_run_date_amount <= 800) & (x.create_role_pay_sum == 0)) |
                                 (( x.create_role_cost <= (3.5 * cost_an - 10)) & (x.source_run_date_amount <= 800) & (x.create_role_pay_sum != 0)) |
                                 (( x.create_role_cost <= (2 * cost_an - 10)) & (x.source_run_date_amount > 800) & (x.create_role_pay_sum == 0)) |
                                 (( x.create_role_cost <= (2.5 * cost_an - 10)) & (x.source_run_date_amount > 800) & (x.create_role_pay_sum != 0)) |
                                 (x.create_role_roi >= 0.01)) else 2), axis=1)

            result_df = result_df.append(data_win_0_1)

        if data_win_0_2.shape[0] != 0:
            data_win_0_2['label'] = data_win_0_2.apply(
                lambda x: 1 if (x.create_role_cost >= 3 * cost_an)
                        else (1 if (x.create_role_roi <= 0.011) | (x.create_role_pay_cost >= 4000)
                              else (0 if (x.create_role_roi >= 0.012) else 2)), axis=1)
            result_df = result_df.append(data_win_0_2)

        if data_win_0_3.shape[0] != 0:
            data_win_0_3['label'] = data_win_0_3.apply(
                lambda x: 1 if (x.create_role_cost >= 3 * cost_an)
                        else (1 if (x.create_role_roi <= 0.011) | (x.create_role_pay_cost >= 4000)
                              else (0 if (x.create_role_roi >= 0.012) else 2)), axis=1)
            result_df = result_df.append(data_win_0_3)

        if data_win_0_4.shape[0] != 0:
            data_win_0_4['label'] = data_win_0_4.apply(
                lambda x: 1 if (x.create_role_cost >= 2.8 * cost_an)
                        else (1 if (x.create_role_roi <= 0.012) | (x.create_role_pay_cost >= 4000)
                              else (0 if (x.create_role_roi >= 0.013) else 2)), axis=1)
            result_df = result_df.append(data_win_0_4)

        if data_win_0_5.shape[0] != 0:
            data_win_0_5['label'] = data_win_0_5.apply(
                lambda x: 1 if (x.create_role_cost >= 2.8 * cost_an)
                        else (1 if (x.create_role_roi <= 0.013) | (x.create_role_pay_cost >= 4000)
                              else (0 if (x.create_role_roi >= 0.014) else 2)), axis=1)
            result_df = result_df.append(data_win_0_5)

    # ISO
    if data_win_0_ios.shape[0] != 0:
        data_win_0_ios_1 = data_win_0_ios[data_win_0_ios['source_run_date_amount'] <= 8000]
        data_win_0_ios_2 = data_win_0_ios[
            (data_win_0_ios['source_run_date_amount'] > 8000) & (data_win_0_ios['source_run_date_amount'] <= 14000)]
        data_win_0_ios_3 = data_win_0_ios[
            (data_win_0_ios['source_run_date_amount'] > 14000) & (data_win_0_ios['source_run_date_amount'] <= 28000)]
        data_win_0_ios_4 = data_win_0_ios[
            (data_win_0_ios['source_run_date_amount'] > 28000) & (data_win_0_ios['source_run_date_amount'] <= 48000)]
        data_win_0_ios_5 = data_win_0_ios[data_win_0_ios['source_run_date_amount'] > 48000]
        if data_win_0_ios_1.shape[0] != 0:
            data_win_0_ios_1['label'] = data_win_0_ios_1.apply(
                lambda x: 1 if (((x.create_role_cost >= 2.5 * cost_ios) & (x.source_run_date_amount <= 1500) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 3.5 * cost_ios) & (x.source_run_date_amount <= 1500) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_cost >= 2 * cost_ios) & (x.source_run_date_amount > 1500) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 2.5 * cost_ios) & (x.source_run_date_amount > 1500) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_roi < 0.01) & (x.source_run_date_amount >= 5000))) else (0
                          if (((x.create_role_cost <= (2.5 * cost_ios - 10)) & (x.source_run_date_amount <= 1300) & (x.create_role_pay_sum == 0)) |
                              ((x.create_role_cost <= (2.5 * cost_ios - 10)) & (x.source_run_date_amount <= 1300) & (x.create_role_pay_sum != 0)) |
                              ((x.create_role_cost <= (2.5 * cost_ios - 10)) & (x.source_run_date_amount > 1300) & (x.create_role_pay_sum == 0)) |
                              ((x.create_role_cost <= (2.5 * cost_ios - 10)) & (x.source_run_date_amount > 1300) & (x.create_role_pay_sum != 0)) |
                              (x.create_role_roi >= 0.01)) else 2), axis=1)

            result_df = result_df.append(data_win_0_ios_1)

        if data_win_0_ios_2.shape[0] != 0:
            data_win_0_ios_2['label'] = data_win_0_ios_2.apply(
                lambda x: 1 if (x.create_role_cost >= 3 * cost_ios)
                        else (1 if (x.create_role_roi <= 0.011) | (x.create_role_pay_cost >= 5000)
                              else (0 if (x.create_role_roi >= 0.012) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_2)

        if data_win_0_ios_3.shape[0] != 0:
            data_win_0_ios_3['label'] = data_win_0_ios_3.apply(
                lambda x: 1 if (x.create_role_cost >= 3 * cost_ios)
                        else (1 if (x.create_role_roi <= 0.012) | (x.create_role_pay_cost >= 5000)
                              else (0 if (x.create_role_roi >= 0.013) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_3)

        if data_win_0_ios_4.shape[0] != 0:
            data_win_0_ios_4['label'] = data_win_0_ios_4.apply(
                lambda x: 1 if (x.create_role_cost >= 2.8 * cost_ios)
                        else (1 if (x.create_role_roi <= 0.013) | (x.create_role_pay_cost >= 5000)
                              else (0 if (x.create_role_roi >= 0.014) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_4)

        if data_win_0_ios_5.shape[0] != 0:
            data_win_0_ios_5['label'] = data_win_0_ios_5.apply(
                lambda x: 1 if (x.create_role_cost >= 2.8 * cost_ios)
                        else (1 if (x.create_role_roi <= 0.014) | (x.create_role_pay_cost >= 5000)
                              else (0 if (x.create_role_roi >= 0.015) else 2)), axis=1)
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


# 帝国头条模型1
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
            lambda x: 1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount <= 1500) & (x.create_role_cost >= 400)
                   else (1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount > 1500) & (x.create_role_cost >= 300)
                         else (1 if (x.create_role_roi > 0) & (x.create_role_roi <= 0.015) & (x.create_role_cost >= 450)
                               else (1 if x.create_role_cost >= 420 else 0))), axis=1)

        result_df = result_df.append(data_win_0_pre)

    # 每次付费ISO
    if data_win_0_ios_pre.shape[0] != 0:
        data_win_0_ios_pre['label'] = data_win_0_ios_pre.apply(
            lambda x: 1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount <= 1500) & (x.create_role_cost >= 800)
                   else (1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount > 1500) & (x.create_role_cost >= 650)
                         else (1 if (x.create_role_roi > 0) & (x.create_role_roi <= 0.014) & (x.create_role_cost >= 900)
                               else (1 if x.create_role_cost >= 1000 else 0))), axis=1)

        result_df = result_df.append(data_win_0_ios_pre)

    # 其它付费方式安卓
    if data_win_0.shape[0] != 0:
        data_win_0['label'] = data_win_0.apply(
            lambda x: 1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount <= 1500) & (x.create_role_cost >= 300)
                   else (1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount > 1500) & (x.create_role_cost >= 200)
                         else (1 if (x.create_role_roi > 0) & (x.create_role_roi <= 0.015) & (x.create_role_cost >= 350)
                               else (1 if x.create_role_cost >= 380 else 0))), axis=1)

        result_df = result_df.append(data_win_0)

    # 其它付费方式IOS
    if data_win_0_ios.shape[0] != 0:
        data_win_0_ios['label'] = data_win_0_ios.apply(
            lambda x: 1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount <= 1500) & (x.create_role_cost >= 700)
                   else (1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount > 1500) & (x.create_role_cost >= 500)
                         else (1 if (x.create_role_roi > 0) & (x.create_role_roi <= 0.014) & (x.create_role_cost >= 800)
                               else (1 if x.create_role_cost >= 900 else 0))), axis=1)

        result_df = result_df.append(data_win_0_ios)

    # 凌晨前关闭消耗大于2000且ROI<0.8%的计划
    # d_time = datetime.datetime.strptime(str(datetime.datetime.now().date()) + '23:40', '%Y-%m-%d%H:%M')
    # d_time1 = datetime.datetime.strptime(
    #     str((datetime.datetime.now() + datetime.timedelta(days=1)).date()) + '00:01',
    #     '%Y-%m-%d%H:%M')
    # n_time = datetime.datetime.now()
    # if n_time > d_time and n_time < d_time1 and result_df.shape[0] != 0:
    #     result_df['label'] = result_df.apply(
    #         lambda x: 1 if (x.source_run_date_amount >= 3000) & (x.create_role_roi <= 0.008) else x.label, axis=1)
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
                lambda x: 6600 if (x.create_role_pay_sum >= 24) & (x.create_role_pay_num >= 2) else
                (6600 if x.create_role_pay_sum >= 48 else np.nan), axis=1)
            result_df = result_df.append(data_1)
        if data_2.shape[0] != 0:
            data_2['budget'] = data_2.apply(
                lambda x: 13000 if (x.create_role_roi >= 0.018) & (x.create_role_pay_num >= 2) else np.nan, axis=1)
            result_df = result_df.append(data_2)
        if data_3.shape[0] != 0:
            data_3['budget'] = data_3.apply(
                lambda x: 23000 if (x.create_role_roi >= 0.018) & (x.create_role_pay_num >= 3) else
                (23000 if x.create_role_roi >= 0.03 else np.nan), axis=1)
            result_df = result_df.append(data_3)
        if data_4.shape[0] != 0:
            data_4['budget'] = data_4.apply(
                lambda x: 50000 if (x.create_role_roi >= 0.02) & (x.create_role_pay_cost <= 4500) else
                (50000 if (x.create_role_roi >= 0.025) & (x.create_role_pay_cost <= 6000) else
                 np.nan), axis=1)
            result_df = result_df.append(data_4)
        if data_5.shape[0] != 0:
            data_5['budget'] = data_5.apply(
                lambda x: 95000 if (x.create_role_roi >= 0.02) & (x.create_role_pay_cost <= 4200) else
                (95000 if (x.create_role_roi >= 0.025) & (x.create_role_pay_cost <= 5500) else
                 np.nan), axis=1)
            result_df = result_df.append(data_5)
        if data_6.shape[0] != 0:
            data_6['budget'] = data_6.apply(
                lambda x: 148000 if (x.create_role_roi >= 0.02) & (x.create_role_pay_cost <= 4000) else
                (148000 if (x.create_role_roi >= 0.025) & (x.create_role_pay_cost <= 3500) else
                 np.nan), axis=1)
            result_df = result_df.append(data_6)
        if data_7.shape[0] != 0:
            data_7['budget'] = data_7.apply(
                lambda x: 220000 if (x.create_role_roi >= 0.02) & (x.create_role_pay_cost <= 4000) else
                (220000 if (x.create_role_roi >= 0.025) & (x.create_role_pay_cost <= 3500) else
                 np.nan), axis=1)
            result_df = result_df.append(data_7)
        if data_8.shape[0] != 0:
            data_8['budget'] = data_8.apply(
                lambda x: 275000 if (x.create_role_roi >= 0.02) & (x.create_role_pay_cost <= 3800) else
                (275000 if (x.create_role_roi >= 0.025) & (x.create_role_pay_cost <= 3300) else
                 np.nan), axis=1)
            result_df = result_df.append(data_8)
        if data_9.shape[0] != 0:
            data_9['budget'] = data_9.apply(
                lambda x: 320000 if (x.create_role_roi >= 0.02) & (x.create_role_pay_cost <= 3600) else
                (320000 if (x.create_role_roi >= 0.025) & (x.create_role_pay_cost <= 3100) else
                 np.nan), axis=1)
            result_df = result_df.append(data_9)
        if data_10.shape[0] != 0:
            data_10['budget'] = data_10.apply(
                lambda x: 450000 if (x.create_role_roi >= 0.02) & (x.create_role_pay_cost <= 3500) else
                (450000 if (x.create_role_roi >= 0.025) & (x.create_role_pay_cost <= 3000) else
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
                lambda x: 8000 if (x.create_role_pay_sum >= 24) & (x.create_role_pay_num >= 2) else
                (8000 if x.create_role_pay_sum >= 48 else np.nan), axis=1)
            result_df = result_df.append(data_1_ios)
        if data_2_ios.shape[0] != 0:
            # data_2_ios['budget'] = data_2_ios.apply(
            #     lambda x: 13000 if (x.create_role_roi >= 0.028) & (x.create_role_pay_num >= 2) else
            #     (13000 if x.create_role_roi >= 0.04 else np.nan), axis=1)
            data_2_ios['budget'] = data_2_ios.apply(
                lambda x: 13000 if (x.create_role_roi >= 0.02) & (x.create_role_pay_num >= 2) else np.nan, axis=1)
            result_df = result_df.append(data_2_ios)
        if data_3_ios.shape[0] != 0:
            # data_3_ios['budget'] = data_3_ios.apply(
            #     lambda x: 23000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_num >= 2) else
            #     np.nan, axis=1)
            data_3_ios['budget'] = data_3_ios.apply(
                lambda x: 23000 if (x.create_role_roi >= 0.02) & (x.create_role_pay_num >= 3) else
                (23000 if x.create_role_roi >= 0.025 else np.nan), axis=1)
            result_df = result_df.append(data_3_ios)
        if data_4_ios.shape[0] != 0:
            data_4_ios['budget'] = data_4_ios.apply(
                lambda x: 50000 if (x.create_role_roi >= 0.02) & (x.create_role_pay_cost <= 6000) else
                (50000 if (x.create_role_roi >= 0.025) & (x.create_role_pay_cost <= 7500) else
                 np.nan), axis=1)
            result_df = result_df.append(data_4_ios)
        if data_5_ios.shape[0] != 0:
            data_5_ios['budget'] = data_5_ios.apply(
                lambda x: 95000 if (x.create_role_roi >= 0.02) & (x.create_role_pay_cost <= 5500) else
                (95000 if (x.create_role_roi >= 0.025) & (x.create_role_pay_cost <= 7000) else
                 np.nan), axis=1)
            result_df = result_df.append(data_5_ios)
        if data_6_ios.shape[0] != 0:
            data_6_ios['budget'] = data_6_ios.apply(
                lambda x: 148000 if (x.create_role_roi >= 0.02) & (x.create_role_pay_cost <= 5000) else
                (148000 if (x.create_role_roi >= 0.025) & (x.create_role_pay_cost <= 6500) else
                 np.nan), axis=1)
            result_df = result_df.append(data_6_ios)
        if data_7_ios.shape[0] != 0:
            data_7_ios['budget'] = data_7_ios.apply(
                lambda x: 220000 if (x.create_role_roi >= 0.02) & (x.create_role_pay_cost <= 4500) else
                (220000 if (x.create_role_roi >= 0.25) & (x.create_role_pay_cost <= 6000) else
                 np.nan), axis=1)
            result_df = result_df.append(data_7_ios)
        if data_8_ios.shape[0] != 0:
            data_8_ios['budget'] = data_8_ios.apply(
                lambda x: 275000 if (x.create_role_roi >= 0.02) & (x.create_role_pay_cost <= 4500) else
                (275000 if (x.create_role_roi >= 0.025) & (x.create_role_pay_cost <= 6000) else
                 np.nan), axis=1)
            result_df = result_df.append(data_8_ios)
        if data_9_ios.shape[0] != 0:
            data_9_ios['budget'] = data_9_ios.apply(
                lambda x: 320000 if (x.create_role_roi >= 0.02) & (x.create_role_pay_cost <= 4500) else
                (320000 if (x.create_role_roi >= 0.025) & (x.create_role_pay_cost <= 6000) else
                 np.nan), axis=1)
            result_df = result_df.append(data_9_ios)
        if data_10_ios.shape[0] != 0:
            data_10_ios['budget'] = data_10_ios.apply(
                lambda x: 450000 if (x.create_role_roi >= 0.02) & (x.create_role_pay_cost <= 4000) else
                (450000 if (x.create_role_roi >= 0.025) & (x.create_role_pay_cost <= 5500) else
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


# 帝国快手模型1
def source_predict_state_1_dg_ks(jsondata):
    '''
    :param jsondata: {"columns": ["channel_id","source_id","plan_name", "model_run_datetime","create_time","media_id",
    "game_id","platform","source_run_date_amount","create_role_num","create_role_cost","create_role_pay_num",
    "create_role_pay_cost","create_role_pay_sum","create_role_roi","create_role_pay_rate","create_role_pay_num_cum",
    "learning_type","learning_time_dt","learning_time_hr","deep_bid_type"]}
    :return:'label','budget'
    '''

    # 该模型只能接受计划状态plan_run_state=1且data_win=0 且media_id=32 且 model_run_datetime 等于当日的数据，即只接受当日快手实时数据
    # label为1时关计划，为0时开计划，如果模型输出label结果与计划当前状况（开/关）相同，则不操作。例如，计划当前是关停状况，当模型输出为1时，不操作

    # 早上7：55分，将所有在运行的计划状态plan_run_state改为2。调用模型2进行判断，是否达标。达标则状态改为1，开计划，预算由模型1计算决定。不达标，则为关停状态，且plan_run_state为2

    data = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])
    data = data[data['media_id'] == 32]
    # 数据预处理
    data = data[data['source_run_date_amount'] > 0]  # 删除消耗为0的数据

    # 对消耗300元以下的不作判断，对数据按平台、win进行分类处理
    data_not_ignore = data[data['source_run_date_amount'] >= 300]

    data_win_0 = data_not_ignore[data_not_ignore['platform'] == 1]
    data_win_0_ios = data_not_ignore[data_not_ignore['platform'] == 2]

    result_df = pd.DataFrame()

    # 获取近15天平均创角成本
    def get_an_cost():
        conn = pymysql.connect(host=dicParam['DB_SLAVE_FENXI_HOST'], port=int(dicParam['DB_SLAVE_FENXI_PORT']), user=dicParam['DB_SLAVE_FENXI_USERNAME'],
                               passwd=dicParam['DB_SLAVE_FENXI_PASSWORD'])
        cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
        sql = '''
                SELECT
                    sum( amount ) / sum( create_role_num ) AS cost 
                FROM
                    db_stdata.st_lauch_report 
                WHERE
                    tdate_type = 'day' 
                    AND tdate >= DATE_SUB( date( NOW()), INTERVAL 15 DAY ) 
                    AND media_id = 32 
                    AND platform = 1 
                    AND game_id IN (
                    SELECT
                        dev_game_id AS game_id 
                    FROM
                        db_data.t_game_config 
                    WHERE
                        game_id = 1112 
                    AND dev_game_id IS NOT NULL 
                    )
        '''
        cur.execute(sql)
        result_df = pd.read_sql(sql, conn)
        cur.close()
        conn.close()
        try:
            result = int(result_df['cost'].values)
        except:
            result = 100
        return result

    def get_ios_cost():
        conn = pymysql.connect(host=dicParam['DB_SLAVE_FENXI_HOST'], port=int(dicParam['DB_SLAVE_FENXI_PORT']), user=dicParam['DB_SLAVE_FENXI_USERNAME'],
                               passwd=dicParam['DB_SLAVE_FENXI_PASSWORD'])
        cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
        sql = '''
                SELECT
                    sum( amount ) / sum( create_role_num ) AS cost 
                FROM
                    db_stdata.st_lauch_report 
                WHERE
                    tdate_type = 'day' 
                    AND tdate >= DATE_SUB( date( NOW()), INTERVAL 15 DAY ) 
                    AND media_id = 32 
                    AND platform = 2 
                    AND game_id IN (
                    SELECT
                        dev_game_id AS game_id 
                    FROM
                        db_data.t_game_config 
                    WHERE
                        game_id = 1112 
                    AND dev_game_id IS NOT NULL 
                    )
        '''
        cur.execute(sql)
        result_df = pd.read_sql(sql, conn)
        cur.close()
        conn.close()
        try:
            result = int(result_df['cost'].values)
        except:
            result = 200
        return result
    # 获取该游戏该媒体的双端创角成本
    cost_an = get_an_cost()
    cost_ios = get_ios_cost()

    # 1:关; 0:开； 2：保持原状
    # win=0预判
    if data_win_0.shape[0] != 0:
        data_win_0_1 = data_win_0[data_win_0['source_run_date_amount'] <= 3000]
        data_win_0_2 = data_win_0[(data_win_0['source_run_date_amount'] > 3000) & (data_win_0['source_run_date_amount'] <= 6000)]
        data_win_0_3 = data_win_0[(data_win_0['source_run_date_amount'] > 6000) & (data_win_0['source_run_date_amount'] <= 10000)]
        data_win_0_4 = data_win_0[(data_win_0['source_run_date_amount'] > 10000) & (data_win_0['source_run_date_amount'] <= 20000)]
        data_win_0_5 = data_win_0[data_win_0['source_run_date_amount'] > 20000]
        if data_win_0_1.shape[0] != 0:
            data_win_0_1['label'] = data_win_0_1.apply(
                lambda x: 1 if (((x.create_role_cost >= 2.5 * cost_an) & (x.source_run_date_amount <= 800) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 3.5 * cost_an) & (x.source_run_date_amount <= 800) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_cost >= 1.5 * cost_an) & (x.source_run_date_amount > 800) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 2.5 * cost_an) & (x.source_run_date_amount > 800) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_roi < 0.005) & (x.source_run_date_amount >= 2000))) else (0
                             if ((( x.create_role_cost <= (2.5 * cost_an - 10)) & (x.source_run_date_amount <= 800) & (x.create_role_pay_sum == 0)) |
                                 (( x.create_role_cost <= (3.5 * cost_an - 10)) & (x.source_run_date_amount <= 800) & (x.create_role_pay_sum != 0)) |
                                 (( x.create_role_cost <= (1.5 * cost_an - 10)) & (x.source_run_date_amount > 800) & (x.create_role_pay_sum == 0)) |
                                 (( x.create_role_cost <= (2.5 * cost_an - 10)) & (x.source_run_date_amount > 800) & (x.create_role_pay_sum != 0)) |
                                 (x.create_role_roi >= 0.01)) else 2), axis=1)

            result_df = result_df.append(data_win_0_1)

        if data_win_0_2.shape[0] != 0:
            data_win_0_2['label'] = data_win_0_2.apply(
                lambda x: 1 if (x.create_role_cost >= 3 * cost_an)
                        else (1 if (x.create_role_roi <= 0.018) | (x.create_role_pay_cost >= 5000)
                              else (0 if (x.create_role_roi >= 0.022) else 2)), axis=1)
            result_df = result_df.append(data_win_0_2)

        if data_win_0_3.shape[0] != 0:
            data_win_0_3['label'] = data_win_0_3.apply(
                lambda x: 1 if (x.create_role_cost >= 3 * cost_an)
                        else (1 if (x.create_role_roi <= 0.02) | (x.create_role_pay_cost >= 4000)
                              else (0 if (x.create_role_roi >= 0.025) else 2)), axis=1)
            result_df = result_df.append(data_win_0_3)

        if data_win_0_4.shape[0] != 0:
            data_win_0_4['label'] = data_win_0_4.apply(
                lambda x: 1 if (x.create_role_cost >= 2.8 * cost_an)
                        else (1 if (x.create_role_roi <= 0.024) | (x.create_role_pay_cost >= 3500)
                              else (0 if (x.create_role_roi >= 0.03) else 2)), axis=1)
            result_df = result_df.append(data_win_0_4)

        if data_win_0_5.shape[0] != 0:
            data_win_0_5['label'] = data_win_0_5.apply(
                lambda x: 1 if (x.create_role_cost >= 2.8 * cost_an)
                        else (1 if (x.create_role_roi <= 0.028) | (x.create_role_pay_cost >= 3000)
                              else (0 if (x.create_role_roi >= 0.038) else 2)), axis=1)
            result_df = result_df.append(data_win_0_5)

    # ISO
    if data_win_0_ios.shape[0] != 0:
        data_win_0_ios_1 = data_win_0_ios[data_win_0_ios['source_run_date_amount'] <= 8000]
        data_win_0_ios_2 = data_win_0_ios[
            (data_win_0_ios['source_run_date_amount'] > 8000) & (data_win_0_ios['source_run_date_amount'] <= 14000)]
        data_win_0_ios_3 = data_win_0_ios[
            (data_win_0_ios['source_run_date_amount'] > 14000) & (data_win_0_ios['source_run_date_amount'] <= 28000)]
        data_win_0_ios_4 = data_win_0_ios[
            (data_win_0_ios['source_run_date_amount'] > 28000) & (data_win_0_ios['source_run_date_amount'] <= 48000)]
        data_win_0_ios_5 = data_win_0_ios[data_win_0_ios['source_run_date_amount'] > 48000]
        if data_win_0_ios_1.shape[0] != 0:
            data_win_0_ios_1['label'] = data_win_0_ios_1.apply(
                lambda x: 1 if (((x.create_role_cost >= 2.5 * cost_ios) & (x.source_run_date_amount <= 1500) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 3.5 * cost_ios) & (x.source_run_date_amount <= 1500) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_cost >= 2 * cost_ios) & (x.source_run_date_amount > 1500) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 2.5 * cost_ios) & (x.source_run_date_amount > 1500) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_roi < 0.01) & (x.source_run_date_amount >= 5000))) else (0
                          if (((x.create_role_cost <= (2.5 * cost_ios - 10)) & (x.source_run_date_amount <= 1300) & (x.create_role_pay_sum == 0)) |
                              ((x.create_role_cost <= (2.5 * cost_ios - 10)) & (x.source_run_date_amount <= 1300) & (x.create_role_pay_sum != 0)) |
                              ((x.create_role_cost <= (2.5 * cost_ios - 10)) & (x.source_run_date_amount > 1300) & (x.create_role_pay_sum == 0)) |
                              ((x.create_role_cost <= (2.5 * cost_ios - 10)) & (x.source_run_date_amount > 1300) & (x.create_role_pay_sum != 0)) |
                              (x.create_role_roi >= 0.01)) else 2), axis=1)

            result_df = result_df.append(data_win_0_ios_1)

        if data_win_0_ios_2.shape[0] != 0:
            data_win_0_ios_2['label'] = data_win_0_ios_2.apply(
                lambda x: 1 if (x.create_role_cost >= 3 * cost_ios)
                        else (1 if (x.create_role_roi <= 0.011) | (x.create_role_pay_cost >= 5000)
                              else (0 if (x.create_role_roi >= 0.012) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_2)

        if data_win_0_ios_3.shape[0] != 0:
            data_win_0_ios_3['label'] = data_win_0_ios_3.apply(
                lambda x: 1 if (x.create_role_cost >= 3 * cost_ios)
                        else (1 if (x.create_role_roi <= 0.012) | (x.create_role_pay_cost >= 5000)
                              else (0 if (x.create_role_roi >= 0.013) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_3)

        if data_win_0_ios_4.shape[0] != 0:
            data_win_0_ios_4['label'] = data_win_0_ios_4.apply(
                lambda x: 1 if (x.create_role_cost >= 2.8 * cost_ios)
                        else (1 if (x.create_role_roi <= 0.013) | (x.create_role_pay_cost >= 5000)
                              else (0 if (x.create_role_roi >= 0.014) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_4)

        if data_win_0_ios_5.shape[0] != 0:
            data_win_0_ios_5['label'] = data_win_0_ios_5.apply(
                lambda x: 1 if (x.create_role_cost >= 2.8 * cost_ios)
                        else (1 if (x.create_role_roi <= 0.014) | (x.create_role_pay_cost >= 5000)
                              else (0 if (x.create_role_roi >= 0.015) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_5)

    if result_df.shape[0] != 0:
        result_df_an = result_df[result_df['platform'] == 1]
        result_df_ios = result_df[result_df['platform'] == 2]
        result_df = pd.DataFrame()

        # 安卓加预算
        data_1 = result_df_an[result_df_an['source_run_date_amount'] <= 3000]
        data_2 = result_df_an[
            (result_df_an['source_run_date_amount'] > 3000) & (result_df_an['source_run_date_amount'] <= 7000)]
        data_3 = result_df_an[
            (result_df_an['source_run_date_amount'] > 7000) & (result_df_an['source_run_date_amount'] <= 10000)]
        data_4 = result_df_an[
            (result_df_an['source_run_date_amount'] > 10000) & (result_df_an['source_run_date_amount'] <= 15000)]
        data_5 = result_df_an[
            (result_df_an['source_run_date_amount'] > 15000) & (result_df_an['source_run_date_amount'] <= 20000)]
        data_6 = result_df_an[
            (result_df_an['source_run_date_amount'] > 20000) & (result_df_an['source_run_date_amount'] <= 28000)]
        data_7 = result_df_an[
            (result_df_an['source_run_date_amount'] > 28000) & (result_df_an['source_run_date_amount'] <= 38000)]
        data_8 = result_df_an[
            (result_df_an['source_run_date_amount'] > 38000) & (result_df_an['source_run_date_amount'] <= 48000)]
        data_9 = result_df_an[
            (result_df_an['source_run_date_amount'] > 48000) & (result_df_an['source_run_date_amount'] <= 68000)]
        data_10 = result_df_an[result_df_an['source_run_date_amount'] > 68000]

        if data_1.shape[0] != 0:
            data_1['budget'] = data_1.apply(
                lambda x: 5000 if (x.create_role_pay_sum >= 60) & (x.create_role_pay_num >= 2) else
                (5000 if x.create_role_roi >= 0.023 else np.nan), axis=1)
            result_df = result_df.append(data_1)
        if data_2.shape[0] != 0:
            data_2['budget'] = data_2.apply(
                lambda x: 8000 if (x.create_role_roi >= 0.028) & (x.create_role_pay_num >= 2) else np.nan, axis=1)
            result_df = result_df.append(data_2)
        if data_3.shape[0] != 0:
            data_3['budget'] = data_3.apply(
                lambda x: 12000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_num >= 2) else
                (12000 if x.create_role_roi >= 0.032 else np.nan), axis=1)
            result_df = result_df.append(data_3)
        if data_4.shape[0] != 0:
            data_4['budget'] = data_4.apply(
                lambda x: 17000 if (x.create_role_roi >= 0.034) & (x.create_role_pay_cost <= 3000) else
                (17000 if (x.create_role_roi >= 0.036) & (x.create_role_pay_cost <= 4000) else
                 np.nan), axis=1)
            result_df = result_df.append(data_4)
        if data_5.shape[0] != 0:
            data_5['budget'] = data_5.apply(
                lambda x: 23000 if (x.create_role_roi >= 0.038) & (x.create_role_pay_cost <= 2600) else
                (23000 if (x.create_role_roi >= 0.04) & (x.create_role_pay_cost <= 3600) else
                 np.nan), axis=1)
            result_df = result_df.append(data_5)
        if data_6.shape[0] != 0:
            data_6['budget'] = data_6.apply(
                lambda x: 30000 if (x.create_role_roi >= 0.043) & (x.create_role_pay_cost <= 2400) else
                (30000 if (x.create_role_roi >= 0.045) & (x.create_role_pay_cost <= 3400) else
                 np.nan), axis=1)
            result_df = result_df.append(data_6)
        if data_7.shape[0] != 0:
            data_7['budget'] = data_7.apply(
                lambda x: 40000 if (x.create_role_roi >= 0.047) & (x.create_role_pay_cost <= 2200) else
                (40000 if (x.create_role_roi >= 0.05) & (x.create_role_pay_cost <= 3200) else
                 np.nan), axis=1)
            result_df = result_df.append(data_7)
        if data_8.shape[0] != 0:
            data_8['budget'] = data_8.apply(
                lambda x: 50000 if (x.create_role_roi >= 0.053) & (x.create_role_pay_cost <= 2000) else
                (50000 if (x.create_role_roi >= 0.056) & (x.create_role_pay_cost <= 3000) else
                 np.nan), axis=1)
            result_df = result_df.append(data_8)
        if data_9.shape[0] != 0:
            data_9['budget'] = data_9.apply(
                lambda x: 70000 if (x.create_role_roi >= 0.06) & (x.create_role_pay_cost <= 1500) else
                (70000 if (x.create_role_roi >= 0.065) & (x.create_role_pay_cost <= 2000) else
                 np.nan), axis=1)
            result_df = result_df.append(data_9)
        if data_10.shape[0] != 0:
            data_10['budget'] = data_10.apply(
                lambda x: 100000 if (x.create_role_roi >= 0.08) & (x.create_role_pay_cost <= 1100) else
                (100000 if (x.create_role_roi >= 0.088) & (x.create_role_pay_cost <= 1200) else
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
                lambda x: 8000 if (x.create_role_pay_sum >= 24) & (x.create_role_pay_num >= 2) else
                (8000 if x.create_role_pay_sum >= 48 else np.nan), axis=1)
            result_df = result_df.append(data_1_ios)
        if data_2_ios.shape[0] != 0:
            # data_2_ios['budget'] = data_2_ios.apply(
            #     lambda x: 13000 if (x.create_role_roi >= 0.028) & (x.create_role_pay_num >= 2) else
            #     (13000 if x.create_role_roi >= 0.04 else np.nan), axis=1)
            data_2_ios['budget'] = data_2_ios.apply(
                lambda x: 13000 if (x.create_role_roi >= 0.02) & (x.create_role_pay_num >= 2) else np.nan, axis=1)
            result_df = result_df.append(data_2_ios)
        if data_3_ios.shape[0] != 0:
            # data_3_ios['budget'] = data_3_ios.apply(
            #     lambda x: 23000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_num >= 2) else
            #     np.nan, axis=1)
            data_3_ios['budget'] = data_3_ios.apply(
                lambda x: 23000 if (x.create_role_roi >= 0.02) & (x.create_role_pay_num >= 3) else
                (23000 if x.create_role_roi >= 0.025 else np.nan), axis=1)
            result_df = result_df.append(data_3_ios)
        if data_4_ios.shape[0] != 0:
            data_4_ios['budget'] = data_4_ios.apply(
                lambda x: 50000 if (x.create_role_roi >= 0.02) & (x.create_role_pay_cost <= 6000) else
                (50000 if (x.create_role_roi >= 0.025) & (x.create_role_pay_cost <= 7500) else
                 np.nan), axis=1)
            result_df = result_df.append(data_4_ios)
        if data_5_ios.shape[0] != 0:
            data_5_ios['budget'] = data_5_ios.apply(
                lambda x: 95000 if (x.create_role_roi >= 0.02) & (x.create_role_pay_cost <= 5500) else
                (95000 if (x.create_role_roi >= 0.025) & (x.create_role_pay_cost <= 7000) else
                 np.nan), axis=1)
            result_df = result_df.append(data_5_ios)
        if data_6_ios.shape[0] != 0:
            data_6_ios['budget'] = data_6_ios.apply(
                lambda x: 148000 if (x.create_role_roi >= 0.02) & (x.create_role_pay_cost <= 5000) else
                (148000 if (x.create_role_roi >= 0.025) & (x.create_role_pay_cost <= 6500) else
                 np.nan), axis=1)
            result_df = result_df.append(data_6_ios)
        if data_7_ios.shape[0] != 0:
            data_7_ios['budget'] = data_7_ios.apply(
                lambda x: 220000 if (x.create_role_roi >= 0.02) & (x.create_role_pay_cost <= 4500) else
                (220000 if (x.create_role_roi >= 0.25) & (x.create_role_pay_cost <= 6000) else
                 np.nan), axis=1)
            result_df = result_df.append(data_7_ios)
        if data_8_ios.shape[0] != 0:
            data_8_ios['budget'] = data_8_ios.apply(
                lambda x: 275000 if (x.create_role_roi >= 0.02) & (x.create_role_pay_cost <= 4500) else
                (275000 if (x.create_role_roi >= 0.025) & (x.create_role_pay_cost <= 6000) else
                 np.nan), axis=1)
            result_df = result_df.append(data_8_ios)
        if data_9_ios.shape[0] != 0:
            data_9_ios['budget'] = data_9_ios.apply(
                lambda x: 320000 if (x.create_role_roi >= 0.02) & (x.create_role_pay_cost <= 4500) else
                (320000 if (x.create_role_roi >= 0.025) & (x.create_role_pay_cost <= 6000) else
                 np.nan), axis=1)
            result_df = result_df.append(data_9_ios)
        if data_10_ios.shape[0] != 0:
            data_10_ios['budget'] = data_10_ios.apply(
                lambda x: 450000 if (x.create_role_roi >= 0.02) & (x.create_role_pay_cost <= 4000) else
                (450000 if (x.create_role_roi >= 0.025) & (x.create_role_pay_cost <= 5500) else
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


# 帝国广点通模型2
def dg_source_predict_state_2_gdt(jsondata):
    '''
    :param jsondata: {"columns": ["channel_id","source_id","plan_name","model_run_datetime","create_time",
    "media_id","game_id","platform","data_win","source_run_date_amount","create_role_num",
    "create_role_cost","create_role_pay_num","create_role_pay_cost","create_role_pay_sum","create_role_roi",
    "create_role_retain_1d","create_role_pay_rate","create_role_pay_num_cum","learning_type","learning_time_dt",
    "learning_time_hr","deep_bid_type","cum_amount","cum_day","cum_role_cost"]}
    :return:
    '''
    data = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])

    # 数据预处理
    data = data[data['source_run_date_amount'] > 0]  # 删除消耗为0的数据
    # 去重，处理某天没有消耗的情况
    data.sort_values(by='data_win', inplace=True)
    data.drop_duplicates(subset=['channel_id', 'source_id', 'source_run_date_amount', 'create_role_num'], keep='first'
                         , inplace=True)

    # 计划7天内总消耗不足一定额度，且总成本合格的计划，让他继续跑，先不做判断
    data_0 = data[(data['cum_amount'] >= 100) & (data['cum_amount'] <= 5000) & (data['cum_day'] <= 7) & (
            data['cum_role_cost'] <= 400) & (data['platform'] == 1)]
    data_1 = data[(data['cum_amount'] >= 100) & (data['cum_amount'] <= 6000) & (data['cum_day'] <= 7) & (
            data['cum_role_cost'] <= 650) & (data['platform'] == 2)]
    data_0 = data_0.append(data_1)
    data_0['label'] = data_0['cum_amount'].apply(lambda x: 0)

    data = data.drop(data_0.index).reset_index(drop=True)
    # 对窗口期消耗200元以下的不作判断，对数据按平台、win进行分类处理
    data_not_ignore = data[(data['source_run_date_amount'] >= 200) | (data['data_win'] == 1)]
    data_ignore = data.drop(data_not_ignore.index)

    # 对最近1日\2日 消耗过6000，且没有付费的计划进行强制关停 data_2
    data_2 = data_not_ignore[
        (data_not_ignore['data_win'] <= 2) & (data_not_ignore['source_run_date_amount'] >= 6000) & (
                    data_not_ignore['create_role_roi'] == 0)]
    data_not_ignore = data_not_ignore.drop(data_2.index)

    # 对data_not_ignore进行周期筛选,1-3窗口期，保留最大周期
    data_not_ignore = data_not_ignore[data_not_ignore['data_win'] <= 3]
    data_not_ignore.sort_values('data_win', ascending=False, inplace=True)
    data_not_ignore = data_not_ignore.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')

    def get_source_data():
        conn = connect(host=dicParam['HIVE_HOST'], port=int(dicParam['HIVE_PORT']), auth_mechanism=dicParam['HIVE_AUTH_MECHANISM'], user=dicParam['HIVE_USERNAME'],
                       password=dicParam['HIVE_PASSWORD'], database=dicParam['HIVE_DATABASE'])
        cursor = conn.cursor()
        sql = '''
            SELECT
                channel_id,
                source_id,
                7_amount as amount_7,
                7_roi as roi_7,
                data_win 
            FROM
                tmp_data.tmp_ra_media_source 
            WHERE
                dt = `current_date` () 
                AND mgame_id = 1112 
                AND media_id = 16
        '''
        cursor.execute(sql)
        result = as_pandas(cursor)

        # 关闭链接
        cursor.close()
        conn.close()
        return result

    # 计划评分数据获取
    source_data = get_source_data()
    source_data_3 = source_data[source_data['data_win'] == 3]
    source_data_7 = source_data[source_data['data_win'] == 7]

    # 计划评分数据与源数据进行拼接
    data_not_ignore = pd.merge(data_not_ignore, source_data_3.drop(['data_win'], axis=1),
                               on=['channel_id', 'source_id'], how='left')
    data_not_ignore = data_not_ignore.rename(columns={'amount_7': 'amount_3', 'roi_7': 'roi_3'})
    data_not_ignore = pd.merge(data_not_ignore, source_data_7.drop(['data_win'], axis=1),
                               on=['channel_id', 'source_id'], how='left')

    # 依据3_roi和7_roi对计划进行判断是否关停
    if data_not_ignore.shape[0] != 0:
        data_not_ignore['label'] = data_not_ignore.apply(lambda x: 1 if (x.amount_3 >= 6000) & ((x.create_role_pay_cost >= 7000) | (x.create_role_roi == 0))
                                                else (0 if x.roi_3 >= 0.025
                                                      else (0 if (x.roi_3 >= 0.02) & (x.amount_3 <= 3000)
                                                            else (0 if (x.roi_7 >= 0.055) & (x.roi_3 >= 0.013) else 1))), axis=1)
        data_not_ignore.drop(['amount_3', 'roi_3', 'amount_7', 'roi_7'], axis=1, inplace=True)
    else:
        data_not_ignore = pd.DataFrame(
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
        data_ignore['data_win'] = 9  # 9 代表是被ignore的计划，消耗太小

    # 1\2日消耗过4000，充值为0，赋值为1，同时data_win变为-1，以便后续按datawin大小筛选优先级
    if data_2.shape[0] != 0:
        data_2['label'] = 1
        data_2.sort_values('data_win', ascending=False, inplace=True)
        data_2 = data_2.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
        data_2['data_win'] = -1  # -1 代表是被data_0的计划，累积消耗太小

    # 7天内总消耗不足，成本合格的计划，直接开计划，账值为0，同时data_win变为-2，以便后续按datawin大小筛选优先级
    if data_0.shape[0] != 0:
        data_0.sort_values('data_win', ascending=False, inplace=True)
        data_0 = data_0.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
        data_0['data_win'] = -2  # -1 代表是被data_0的计划，累积消耗太小

    source_predict = pd.concat([data_not_ignore, data_ignore], axis=0)
    source_predict = pd.concat([source_predict, data_0], axis=0)
    source_predict = pd.concat([source_predict, data_2], axis=0)

    # 综合结果优先级： data_0   data_2  大于 data   大于  data_ignore
    source_predict.sort_values('data_win', ascending=True, inplace=True)
    source_predict = source_predict.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
    #     print(source_predict.head())
    source_predict['label'] = source_predict['label'].astype(int)
    # print(source_predict.columns.values.tolist())
    source_predict = source_predict.fillna('null')

    return {"columns": source_predict.columns.values.tolist(), "list": source_predict.values.tolist()}


# 帝国百度模型2
def dg_source_predict_state_2_bd(jsondata):
    '''
    :param jsondata: {"columns": ["channel_id","source_id","plan_name","model_run_datetime","create_time",
    "media_id","game_id","platform","data_win","source_run_date_amount","create_role_num",
    "create_role_cost","create_role_pay_num","create_role_pay_cost","create_role_pay_sum","create_role_roi",
    "create_role_retain_1d","create_role_pay_rate","create_role_pay_num_cum","learning_type","learning_time_dt",
    "learning_time_hr","deep_bid_type","cum_amount","cum_day","cum_role_cost"]}
    :return:
    '''
    data = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])

    # 数据预处理
    data = data[data['source_run_date_amount'] > 0]  # 删除消耗为0的数据
    # 去重，处理某天没有消耗的情况
    data.sort_values(by='data_win', inplace=True)
    data.drop_duplicates(subset=['channel_id', 'source_id', 'source_run_date_amount', 'create_role_num'], keep='first'
                         , inplace=True)

    # 计划7天内总消耗不足一定额度，且总成本合格的计划，让他继续跑，先不做判断
    data_0 = data[(data['cum_amount'] >= 100) & (data['cum_amount'] <= 3000) & (data['cum_day'] <= 7) & (
            data['cum_role_cost'] <= 250) & (data['platform'] == 1)]
    data_1 = data[(data['cum_amount'] >= 100) & (data['cum_amount'] <= 4000) & (data['cum_day'] <= 7) & (
            data['cum_role_cost'] <= 400) & (data['platform'] == 2)]
    data_0 = data_0.append(data_1)
    data_0['label'] = data_0['cum_amount'].apply(lambda x: 0)

    data = data.drop(data_0.index).reset_index(drop=True)
    # 对窗口期消耗200元以下的不作判断，对数据按平台、win进行分类处理
    data_not_ignore = data[(data['source_run_date_amount'] >= 200) | (data['data_win'] == 1)]
    data_ignore = data.drop(data_not_ignore.index)

    # 对最近1日\2日 消耗过6000，且没有付费的计划进行强制关停 data_2
    data_2 = data_not_ignore[
        (data_not_ignore['data_win'] <= 2) & (data_not_ignore['source_run_date_amount'] >= 4000) & (
                    data_not_ignore['create_role_roi'] == 0)]
    data_not_ignore = data_not_ignore.drop(data_2.index)

    # 对data_not_ignore进行周期筛选,1-3窗口期，保留最大周期
    data_not_ignore = data_not_ignore[data_not_ignore['data_win'] <= 3]
    data_not_ignore.sort_values('data_win', ascending=False, inplace=True)
    data_not_ignore = data_not_ignore.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')

    def get_source_data():
        conn = connect(host=dicParam['HIVE_HOST'], port=int(dicParam['HIVE_PORT']), auth_mechanism=dicParam['HIVE_AUTH_MECHANISM'], user=dicParam['HIVE_USERNAME'],
                       password=dicParam['HIVE_PASSWORD'], database=dicParam['HIVE_DATABASE'])
        cursor = conn.cursor()
        sql = '''
            SELECT
                channel_id,
                source_id,
                7_amount as amount_7,
                7_roi as roi_7,
                data_win 
            FROM
                tmp_data.tmp_ra_media_source 
            WHERE
                dt = `current_date` () 
                AND mgame_id = 1112 
                AND media_id = 45
        '''
        cursor.execute(sql)
        result = as_pandas(cursor)

        # 关闭链接
        cursor.close()
        conn.close()
        return result

    # 计划评分数据获取
    source_data = get_source_data()
    source_data_3 = source_data[source_data['data_win'] == 3]
    source_data_7 = source_data[source_data['data_win'] == 7]

    # 计划评分数据与源数据进行拼接
    data_not_ignore = pd.merge(data_not_ignore, source_data_3.drop(['data_win'], axis=1),
                               on=['channel_id', 'source_id'], how='left')
    data_not_ignore = data_not_ignore.rename(columns={'amount_7': 'amount_3', 'roi_7': 'roi_3'})
    data_not_ignore = pd.merge(data_not_ignore, source_data_7.drop(['data_win'], axis=1),
                               on=['channel_id', 'source_id'], how='left')

    # 依据3_roi和7_roi对计划进行判断是否关停
    if data_not_ignore.shape[0] != 0:
        data_not_ignore['label'] = data_not_ignore.apply(lambda x: 1 if (x.amount_3 >= 4500) & ((x.create_role_pay_cost >= 5000) | (x.create_role_roi == 0))
                                                else (0 if x.roi_3 >= 0.025
                                                      else (0 if (x.roi_3 >= 0.02) & (x.amount_3 <= 3000)
                                                            else (0 if (x.roi_7 >= 0.055) & (x.roi_3 >= 0.013) else 1))), axis=1)
        data_not_ignore.drop(['amount_3', 'roi_3', 'amount_7', 'roi_7'], axis=1, inplace=True)
    else:
        data_not_ignore = pd.DataFrame(
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
        data_ignore['data_win'] = 9  # 9 代表是被ignore的计划，消耗太小

    # 1\2日消耗过4000，充值为0，赋值为1，同时data_win变为-1，以便后续按datawin大小筛选优先级
    if data_2.shape[0] != 0:
        data_2['label'] = 1
        data_2.sort_values('data_win', ascending=False, inplace=True)
        data_2 = data_2.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
        data_2['data_win'] = -1  # -1 代表是被data_0的计划，累积消耗太小

    # 7天内总消耗不足，成本合格的计划，直接开计划，账值为0，同时data_win变为-2，以便后续按datawin大小筛选优先级
    if data_0.shape[0] != 0:
        data_0.sort_values('data_win', ascending=False, inplace=True)
        data_0 = data_0.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
        data_0['data_win'] = -2  # -1 代表是被data_0的计划，累积消耗太小

    source_predict = pd.concat([data_not_ignore, data_ignore], axis=0)
    source_predict = pd.concat([source_predict, data_0], axis=0)
    source_predict = pd.concat([source_predict, data_2], axis=0)

    # 综合结果优先级： data_0   data_2  大于 data   大于  data_ignore
    source_predict.sort_values('data_win', ascending=True, inplace=True)
    source_predict = source_predict.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
    #     print(source_predict.head())
    source_predict['label'] = source_predict['label'].astype(int)
    # print(source_predict.columns.values.tolist())
    source_predict = source_predict.fillna('null')

    return {"columns": source_predict.columns.values.tolist(), "list": source_predict.values.tolist()}


# 帝国头条模型2
# def dg_source_predict_state_2(jsondata):
#     '''
#     :param jsondata: {"columns": ["channel_id","source_id","plan_name","model_run_datetime","create_time",
#     "media_id","game_id","platform","data_win","source_run_date_amount","create_role_num",
#     "create_role_cost","create_role_pay_num","create_role_pay_cost","create_role_pay_sum","create_role_roi",
#     "create_role_retain_1d","create_role_pay_rate","create_role_pay_num_cum","learning_type","learning_time_dt",
#     "learning_time_hr","deep_bid_type","cum_amount","cum_day","cum_role_cost"]}
#     :return:
#     '''
#     data = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])
#
#     # 数据预处理
#     data = data[data['source_run_date_amount'] > 0]  # 删除消耗为0的数据
#     # 去重，处理某天没有消耗的情况
#     data.sort_values(by='data_win', inplace=True)
#     data.drop_duplicates(subset=['channel_id', 'source_id', 'source_run_date_amount', 'create_role_num'], keep='first'
#                          , inplace=True)
#
#     # 计划7天内总消耗不足4000，且总成本合格的计划，让他继续跑，先不做判断
#     data_0 = data[(data['cum_amount'] >= 100) & (data['cum_amount'] <= 4500) & (data['cum_day'] <= 7) & (
#                 data['cum_role_cost'] <= 230) & (data['platform'] == 1)]
#     data_1 = data[(data['cum_amount'] >= 100) & (data['cum_amount'] <= 5500) & (data['cum_day'] <= 7) & (
#                 data['cum_role_cost'] <= 500) & (data['platform'] == 2)]
#     data_0 = data_0.append(data_1)
#     data_0['label'] = data_0['cum_amount'].apply(lambda x: 0)
#
#     data = data.drop(data_0.index).reset_index(drop=True)
#     # 对窗口期消耗200元以下的不作判断，对数据按平台、win进行分类处理
#     data_not_ignore = data[(data['source_run_date_amount'] >= 200) | (data['data_win'] == 1)]
#     data_ignore = data.drop(data_not_ignore.index)
#
#     data_win_1 = data_not_ignore[(data_not_ignore['data_win'] == 1) & (data_not_ignore['platform'] == 1)]
#     data_win_2 = data_not_ignore[(data_not_ignore['data_win'] == 2) & (data_not_ignore['platform'] == 1)]
#     data_win_3 = data_not_ignore[(data_not_ignore['data_win'] == 3) & (data_not_ignore['platform'] == 1)]
#
#     data_win_1_ios = data_not_ignore[(data_not_ignore['data_win'] == 1) & (data_not_ignore['platform'] == 2)]
#     data_win_2_ios = data_not_ignore[(data_not_ignore['data_win'] == 2) & (data_not_ignore['platform'] == 2)]
#     data_win_3_ios = data_not_ignore[(data_not_ignore['data_win'] == 3) & (data_not_ignore['platform'] == 2)]
#
#     # win=1预判
#     result_win1 = pd.DataFrame()
#     temp_win1 = pd.DataFrame()
#
#     # data_win_1中再分deep_bid_type
#     data_win_1_pre_action = data_win_1[data_win_1['deep_bid_type'] == 'BID_PER_ACTION']
#     data_win_1 = data_win_1[data_win_1['deep_bid_type'] != 'BID_PER_ACTION']
#
#     if data_win_1_pre_action.shape[0] != 0:
#         data_win_1_pre_action['label'] = data_win_1_pre_action.apply(lambda x: 1 if x.create_role_num == 0
#         else (1 if (x.create_role_pay_cost > 6000) | (x.create_role_cost > 260)
#               else (1 if x.create_role_roi < 0.008
#                     else (0 if x.create_role_roi >= 0.008 else 2))), axis=1)
#
#         result_win1 = result_win1.append(
#             data_win_1_pre_action[(data_win_1_pre_action['label'] == 1) | (data_win_1_pre_action['label'] == 0)])
#         temp_win1 = temp_win1.append(data_win_1_pre_action[data_win_1_pre_action['label'] == 2])
#
#     if data_win_1.shape[0] != 0:
#         data_win_1['label'] = data_win_1.apply(lambda x: 1 if x.create_role_num == 0
#         else (1 if (x.create_role_pay_cost > 6000) | (x.create_role_cost > 150)
#               else (1 if x.create_role_roi < 0.008
#                     else (0 if x.create_role_roi >= 0.008 else 2))), axis=1)
#
#         result_win1 = result_win1.append(data_win_1[(data_win_1['label'] == 1) | (data_win_1['label'] == 0)])
#         temp_win1 = temp_win1.append(data_win_1[data_win_1['label'] == 2])
#
#     data_win_1_ios_pre_action = data_win_1_ios[data_win_1_ios['deep_bid_type'] == 'BID_PER_ACTION']
#     data_win_1_ios = data_win_1_ios[data_win_1_ios['deep_bid_type'] != 'BID_PER_ACTION']
#
#     if data_win_1_ios_pre_action.shape[0] != 0:
#         data_win_1_ios_pre_action['label'] = data_win_1_ios_pre_action.apply(lambda x: 1 if x.create_role_num == 0
#         else (1 if (x.create_role_pay_cost > 8000) | (x.create_role_cost > 500)
#               else (1 if x.create_role_roi < 0.008
#                     else (0 if x.create_role_roi >= 0.008 else 2))), axis=1)
#
#         result_win1 = result_win1.append(data_win_1_ios_pre_action[(data_win_1_ios_pre_action['label'] == 1) | (
#                 data_win_1_ios_pre_action['label'] == 0)])
#         temp_win1 = temp_win1.append(data_win_1_ios_pre_action[data_win_1_ios_pre_action['label'] == 2])
#
#     if data_win_1_ios.shape[0] != 0:
#         data_win_1_ios['label'] = data_win_1_ios.apply(lambda x: 1 if x.create_role_num == 0
#         else (1 if (x.create_role_pay_cost > 8000) | (x.create_role_cost > 400)
#               else (1 if x.create_role_roi < 0.008
#                     else (0 if x.create_role_roi >= 0.008 else 2))), axis=1)
#
#         result_win1 = result_win1.append(
#             data_win_1_ios[(data_win_1_ios['label'] == 1) | (data_win_1_ios['label'] == 0)])
#         temp_win1 = temp_win1.append(data_win_1_ios[data_win_1_ios['label'] == 2])
#
#     # win=1 模型预测
#     if temp_win1.shape[0] != 0:
#         temp_win1['label'] = gbdt_b_win1.predict(
#             temp_win1[['create_role_cost', 'create_role_pay_cost', 'create_role_roi',
#                        'create_role_pay_rate']])
#     result_win1_data = pd.concat([result_win1, temp_win1], axis=0)
#
#     # win=2预判
#     result_win2 = pd.DataFrame()
#     temp_win2 = pd.DataFrame()
#
#     if data_win_2.shape[0] != 0:
#         data_win_2['label'] = data_win_2.apply(lambda x: 1 if x.create_role_num == 0
#         else (1 if (x.create_role_pay_cost > 6000) | (x.create_role_cost > 250)
#               else (3 if (x.create_role_roi == 0) & (x.source_run_date_amount >= 6000)
#                     else (1 if x.create_role_roi <= 0.013
#                           else (0 if x.create_role_roi > 0.013 else 2)))), axis=1)
#         result_win2 = result_win2.append(
#             data_win_2[(data_win_2['label'] == 1) | (data_win_2['label'] == 0) | (data_win_2['label'] == 3)])
#         temp_win2 = temp_win2.append(data_win_2[data_win_2['label'] == 2])
#
#     if data_win_2_ios.shape[0] != 0:
#         data_win_2_ios['label'] = data_win_2_ios.apply(lambda x: 1 if x.create_role_num == 0
#         else (1 if (x.create_role_pay_cost > 8000) | (x.create_role_cost > 500)
#               else (3 if (x.create_role_roi == 0) & (x.source_run_date_amount >= 7000)
#                     else (1 if x.create_role_roi <= 0.013
#                           else (0 if x.create_role_roi > 0.013 else 2)))), axis=1)
#
#         result_win2 = result_win2.append(
#             data_win_2_ios[(data_win_2_ios['label'] == 1) | (data_win_2_ios['label'] == 0)])
#         temp_win2 = temp_win2.append(data_win_2_ios[data_win_2_ios['label'] == 2])
#
#     # win=2 模型预测
#     if temp_win2.shape[0] != 0:
#         temp_win2['label'] = gbdt_b_win2.predict(
#             temp_win2[['create_role_cost', 'create_role_pay_cost', 'create_role_roi',
#                        'create_role_pay_rate', 'create_role_retain_1d']])
#     result_win2_data = pd.concat([result_win2, temp_win2], axis=0)
#
#     # win=3预判
#     result_win3 = pd.DataFrame()
#     temp_win3 = pd.DataFrame()
#     if data_win_3.shape[0] != 0:
#         data_win_3['label'] = data_win_3.apply(lambda x: 1 if x.create_role_num == 0
#         else (1 if (x.create_role_pay_cost > 6000) | (x.create_role_cost > 220)
#               else (0 if (x.cum_day <= 7) & (x.source_run_date_amount <= 3300) & (x.cum_role_cost <= 220)
#                     else (0 if (x.create_role_pay_cost <= 3500) & (x.create_role_pay_cost > 0)
#                     else (0 if x.create_role_roi >= 0.02
#                           else (1 if x.create_role_roi < 0.02 else 2))))), axis=1)
#
#         result_win3 = result_win3.append(data_win_3[(data_win_3['label'] == 1) | (data_win_3['label'] == 0)])
#         temp_win3 = temp_win3.append(data_win_3[data_win_3['label'] == 2])
#
#     if data_win_3_ios.shape[0] != 0:
#         data_win_3_ios['label'] = data_win_3_ios.apply(lambda x: 1 if x.create_role_num == 0
#         else (1 if (x.create_role_pay_cost > 8000) | (x.create_role_cost > 500)
#               else (0 if (x.cum_day <= 7) & (x.source_run_date_amount <= 4500) & (x.cum_role_cost <= 500)
#                     else (0 if (x.create_role_pay_cost <= 4000) & (x.create_role_pay_cost > 0)
#                     else (0 if x.create_role_roi >= 0.02
#                           else (1 if x.create_role_roi < 0.02 else 2))))), axis=1)
#
#         result_win3 = result_win3.append(
#             data_win_3_ios[(data_win_3_ios['label'] == 1) | (data_win_3_ios['label'] == 0)])
#         temp_win3 = temp_win3.append(data_win_3_ios[data_win_3_ios['label'] == 2])
#
#     # win=3 模型预测
#     if temp_win3.shape[0] != 0:
#         temp_win3['label'] = gbdt_b_win3.predict(
#             temp_win3[['create_role_cost', 'create_role_pay_cost', 'create_role_roi',
#                        'create_role_pay_rate', 'create_role_retain_1d']])
#     result_win3_data = pd.concat([result_win3, temp_win3], axis=0)
#
#     result = pd.concat([result_win1_data, result_win2_data, result_win3_data], axis=0)
#     if result.shape[0] != 0:
#         result.sort_values('data_win', ascending=True, inplace=True)
#
#     # 结果输出
#     if result.shape[0] != 0:
#         # 1\2\3有一个开，则开，除非win2=3
#         if (result[result['data_win'] == 3].shape[0] != 0) & (result[result['data_win'] == 2].shape[0] != 0) & (
#                 result[result['data_win'] == 1].shape[0] != 0):
#             result_1_2_3 = result[(result['data_win'] == 1) | (result['data_win'] == 2) | (result['data_win'] == 3)]
#             result_1_2_3_label = result_1_2_3[['channel_id', 'source_id', 'model_run_datetime', 'data_win', 'label']]
#             result_1_2_3_label['data_win'] = result_1_2_3_label['data_win'].astype(int)
#             result_1_2_3_piv = pd.pivot_table(result_1_2_3_label,
#                                               index=['channel_id', 'source_id', 'model_run_datetime'],
#                                               columns='data_win')
#             result_1_2_3_piv.columns = result_1_2_3_piv.columns.droplevel()
#             result_1_2_3_piv = result_1_2_3_piv.rename(columns={1: 'label_1', 2: 'label_2', 3: 'label_3'})
#
#             result_1_2_3_piv = result_1_2_3_piv.reset_index()
#
#             result_1_2_3_piv['label'] = result_1_2_3_piv.apply(lambda x: 0 if x.label_1 == 0
#                                     else (0 if x.label_2 == 0
#                                           else (1 if x.label_2 == 3
#                                                 else (0 if x.label_3 == 0 else 1))), axis=1)
#             result_1_2_3 = result_1_2_3.drop('label', axis=1)
#             #             result_1_2 = result_1_2[result_1_2['data_win'] == 1]
#             result_1_2_3 = result_1_2_3.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='last')
#
#             result_1_2_3_piv.drop(['label_1', 'label_2', 'label_3'], axis=1, inplace=True)
#             source_predict = pd.merge(result_1_2_3, result_1_2_3_piv,
#                                       on=['channel_id', 'source_id', 'model_run_datetime'],
#                                       how='left')
#             # 1\2有一个为开，则开
#         elif (result[result['data_win'] == 2].shape[0] != 0) & (result[result['data_win'] == 1].shape[0] != 0):
#             result_1_2 = result[(result['data_win'] == 1) | (result['data_win'] == 2)]
#             result_1_2_label = result_1_2[['channel_id', 'source_id', 'model_run_datetime', 'data_win', 'label']]
#             result_1_2_label['data_win'] = result_1_2_label['data_win'].astype(int)
#             result_1_2_piv = pd.pivot_table(result_1_2_label, index=['channel_id', 'source_id', 'model_run_datetime'],
#                                             columns='data_win')
#             result_1_2_piv.columns = result_1_2_piv.columns.droplevel()
#             result_1_2_piv = result_1_2_piv.rename(columns={1: 'label_1', 2: 'label_2'})
#
#             result_1_2_piv = result_1_2_piv.reset_index()
#             result_1_2_piv['label'] = result_1_2_piv.apply(lambda x: 0 if x.label_1 == 0
#                                                             else (0 if x.label_2 == 0 else 1), axis=1)
#             result_1_2 = result_1_2.drop('label', axis=1)
#             #             result_1_2 = result_1_2[result_1_2['data_win'] == 1]
#             result_1_2 = result_1_2.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='last')
#
#             result_1_2_piv.drop(['label_1', 'label_2'], axis=1, inplace=True)
#             source_predict = pd.merge(result_1_2, result_1_2_piv, on=['channel_id', 'source_id', 'model_run_datetime'],
#                                       how='left')
#         else:
#             source_predict = result
#
#     else:
#         source_predict = pd.DataFrame(
#             columns=["channel_id", "source_id", "plan_name", "model_run_datetime", "create_time",
#                      "media_id", "game_id", "platform", "data_win", "source_run_date_amount", "create_role_num",
#                      "create_role_cost", "create_role_pay_num", "create_role_pay_cost", "create_role_pay_sum",
#                      "create_role_roi",
#                      "create_role_retain_1d", "create_role_pay_rate", "create_role_pay_num_cum", "learning_type",
#                      "learning_time_dt",
#                      "learning_time_hr", "deep_bid_type", "cum_amount", "cum_day", "cum_role_cost", "label"])
#
#     # 消耗小的ignore，直接关停计划，赋值1，同时data_win变为9，以便后续按datawin大小筛选优先级
#     if data_ignore.shape[0] != 0:
#         data_ignore['label'] = 1
#         data_ignore.sort_values('data_win', ascending=False, inplace=True)
#         data_ignore = data_ignore.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
#         data_ignore['data_win'] = 9  # 9 代表是被ignore的计划，消耗太小
#
#     # 7天内总消耗不足4000，成本合格的计划，直接开计划，账值为0，同时data_win变为-1，以便后续按datawin大小筛选优先级
#     if data_0.shape[0] != 0:
#         data_0.sort_values('data_win', ascending=False, inplace=True)
#         data_0 = data_0.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
#         data_0['data_win'] = -1  # -1 代表是被data_0的计划，累积消耗太小
#
#     source_predict = pd.concat([source_predict, data_ignore], axis=0)
#     source_predict = pd.concat([source_predict, data_0], axis=0)
#
#     # 综合结果优先级： data_0    大于 data   大于  data_ignore
#     source_predict.sort_values('data_win', ascending=True, inplace=True)
#     source_predict = source_predict.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
#     source_predict['label'] = source_predict['label'].astype(int)
#     source_predict = source_predict.fillna('null')
#
#     return {"columns": source_predict.columns.values.tolist(), "list": source_predict.values.tolist()}


# 帝国头条模型2
def dg_source_predict_state_2(jsondata):
    '''
    :param jsondata: {"columns": ["channel_id","source_id","plan_name","model_run_datetime","create_time",
    "media_id","game_id","platform","data_win","source_run_date_amount","create_role_num",
    "create_role_cost","create_role_pay_num","create_role_pay_cost","create_role_pay_sum","create_role_roi",
    "create_role_retain_1d","create_role_pay_rate","create_role_pay_num_cum","learning_type","learning_time_dt",
    "learning_time_hr","deep_bid_type","cum_amount","cum_day","cum_role_cost"]}
    :return:
    '''
    data = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])

    # 数据预处理
    data = data[data['source_run_date_amount'] > 0]  # 删除消耗为0的数据
    # 去重，处理某天没有消耗的情况
    data.sort_values(by='data_win', inplace=True)
    data.drop_duplicates(subset=['channel_id', 'source_id', 'source_run_date_amount', 'create_role_num'], keep='first'
                         , inplace=True)

    # 计划7天内总消耗不足4000，且总成本合格的计划，让他继续跑，先不做判断
    data_0 = data[(data['cum_amount'] >= 100) & (data['cum_amount'] <= 4500) & (data['cum_day'] <= 7) & (
            data['cum_role_cost'] <= 400) & (data['platform'] == 1)]
    data_1 = data[(data['cum_amount'] >= 100) & (data['cum_amount'] <= 5500) & (data['cum_day'] <= 7) & (
            data['cum_role_cost'] <= 600) & (data['platform'] == 2)]
    data_0 = data_0.append(data_1)
    data_0['label'] = data_0['cum_amount'].apply(lambda x: 0)

    data = data.drop(data_0.index).reset_index(drop=True)
    # 对窗口期消耗200元以下的不作判断，对数据按平台、win进行分类处理
    data_not_ignore = data[(data['source_run_date_amount'] >= 200) | (data['data_win'] == 1)]
    data_ignore = data.drop(data_not_ignore.index)

    # 对最近1日\2日 消耗过6000，且没有付费的计划进行强制关停 data_2
    data_2 = data_not_ignore[
        (data_not_ignore['data_win'] <= 2) & (data_not_ignore['source_run_date_amount'] >= 6000) & (
                    data_not_ignore['create_role_roi'] == 0)]
    data_not_ignore = data_not_ignore.drop(data_2.index)

    # 对data_not_ignore进行周期筛选,1-3窗口期，保留最大周期
    data_not_ignore = data_not_ignore[data_not_ignore['data_win'] <= 3]
    data_not_ignore.sort_values('data_win', ascending=False, inplace=True)
    data_not_ignore = data_not_ignore.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')

    def get_source_data():
        conn = connect(host=dicParam['HIVE_HOST'], port=int(dicParam['HIVE_PORT']), auth_mechanism=dicParam['HIVE_AUTH_MECHANISM'], user=dicParam['HIVE_USERNAME'],
                       password=dicParam['HIVE_PASSWORD'], database=dicParam['HIVE_DATABASE'])
        cursor = conn.cursor()
        sql = '''
            SELECT
                channel_id,
                source_id,
                7_amount as amount_7,
                7_roi as roi_7,
                data_win 
            FROM
                tmp_data.tmp_ra_media_source 
            WHERE
                dt = `current_date` () 
                AND mgame_id = 1112 
                AND media_id = 10
        '''
        cursor.execute(sql)
        result = as_pandas(cursor)

        # 关闭链接
        cursor.close()
        conn.close()
        return result

    # 计划评分数据获取
    source_data = get_source_data()
    source_data_3 = source_data[source_data['data_win'] == 3]
    source_data_7 = source_data[source_data['data_win'] == 7]

    # 计划评分数据与源数据进行拼接
    data_not_ignore = pd.merge(data_not_ignore, source_data_3.drop(['data_win'], axis=1),
                               on=['channel_id', 'source_id'], how='left')
    data_not_ignore = data_not_ignore.rename(columns={'amount_7': 'amount_3', 'roi_7': 'roi_3'})
    data_not_ignore = pd.merge(data_not_ignore, source_data_7.drop(['data_win'], axis=1),
                               on=['channel_id', 'source_id'], how='left')

    # 依据3_roi和7_roi对计划进行判断是否关停
    if data_not_ignore.shape[0] != 0:
        data_not_ignore['label'] = data_not_ignore.apply(lambda x: 1 if (x.amount_3 >= 5000) & ((x.create_role_pay_cost >= 6500) | (x.create_role_roi == 0))
                                                else (0 if x.roi_3 >= 0.025
                                                      else (0 if (x.roi_3 >= 0.02) & (x.amount_3 <= 4000)
                                                            else (0 if (x.roi_7 >= 0.055) & (x.roi_3 >= 0.013) else 1))), axis=1)
        data_not_ignore.drop(['amount_3', 'roi_3', 'amount_7', 'roi_7'], axis=1, inplace=True)
    else:
        data_not_ignore = pd.DataFrame(
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
        data_ignore['data_win'] = 9  # 9 代表是被ignore的计划，消耗太小

    # 1\2日消耗过6000，充值为0，赋值为1，同时data_win变为-1，以便后续按datawin大小筛选优先级
    if data_2.shape[0] != 0:
        data_2['label'] = 1
        data_2.sort_values('data_win', ascending=False, inplace=True)
        data_2 = data_2.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
        data_2['data_win'] = -1  # -1 代表是被data_0的计划，累积消耗太小

    # 7天内总消耗不足4000，成本合格的计划，直接开计划，账值为0，同时data_win变为-2，以便后续按datawin大小筛选优先级
    if data_0.shape[0] != 0:
        data_0.sort_values('data_win', ascending=False, inplace=True)
        data_0 = data_0.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
        data_0['data_win'] = -2  # -1 代表是被data_0的计划，累积消耗太小

    source_predict = pd.concat([data_not_ignore, data_ignore], axis=0)
    source_predict = pd.concat([source_predict, data_0], axis=0)
    source_predict = pd.concat([source_predict, data_2], axis=0)

    # 综合结果优先级： data_0   data_2  大于 data   大于  data_ignore
    source_predict.sort_values('data_win', ascending=True, inplace=True)
    source_predict = source_predict.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
    #     print(source_predict.head())
    source_predict['label'] = source_predict['label'].astype(int)
    # print(source_predict.columns.values.tolist())
    source_predict = source_predict.fillna('null')

    return {"columns": source_predict.columns.values.tolist(), "list": source_predict.values.tolist()}


# 帝国快手模型2
def dg_source_predict_state_2_ks(jsondata):
    '''
    :param jsondata: {"columns": ["channel_id","source_id","plan_name","model_run_datetime","create_time",
    "media_id","game_id","platform","data_win","source_run_date_amount","create_role_num",
    "create_role_cost","create_role_pay_num","create_role_pay_cost","create_role_pay_sum","create_role_roi",
    "create_role_retain_1d","create_role_pay_rate","create_role_pay_num_cum","learning_type","learning_time_dt",
    "learning_time_hr","deep_bid_type","cum_amount","cum_day","cum_role_cost"]}
    :return:
    '''
    data = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])

    # 数据预处理
    data = data[data['source_run_date_amount'] > 0]  # 删除消耗为0的数据
    # 去重，处理某天没有消耗的情况
    data.sort_values(by='data_win', inplace=True)
    data.drop_duplicates(subset=['channel_id', 'source_id', 'source_run_date_amount', 'create_role_num'], keep='first'
                         , inplace=True)

    # 计划7天内总消耗不足4000，且总成本合格的计划，让他继续跑，先不做判断
    data_0 = data[(data['cum_amount'] >= 100) & (data['cum_amount'] <= 3000) & (data['cum_day'] <= 7) & (
            data['cum_role_cost'] <= 100) & (data['platform'] == 1)]
    data_1 = data[(data['cum_amount'] >= 100) & (data['cum_amount'] <= 4500) & (data['cum_day'] <= 7) & (
            data['cum_role_cost'] <= 300) & (data['platform'] == 2)]
    data_0 = data_0.append(data_1)
    data_0['label'] = data_0['cum_amount'].apply(lambda x: 0)

    data = data.drop(data_0.index).reset_index(drop=True)
    # 对窗口期消耗200元以下的不作判断，对数据按平台、win进行分类处理
    data_not_ignore = data[(data['source_run_date_amount'] >= 200) | (data['data_win'] == 1)]
    data_ignore = data.drop(data_not_ignore.index)

    # 对最近1日\2日 消耗过6000，且没有付费的计划进行强制关停 data_2
    data_2 = data_not_ignore[
        (data_not_ignore['data_win'] <= 2) & (data_not_ignore['source_run_date_amount'] >= 4500) & (
                    data_not_ignore['create_role_roi'] == 0)]
    data_not_ignore = data_not_ignore.drop(data_2.index)

    # 对data_not_ignore进行周期筛选,1-3窗口期，保留最大周期
    data_not_ignore = data_not_ignore[data_not_ignore['data_win'] <= 3]
    data_not_ignore.sort_values('data_win', ascending=False, inplace=True)
    data_not_ignore = data_not_ignore.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')

    def get_source_data():
        conn = connect(host=dicParam['HIVE_HOST'], port=int(dicParam['HIVE_PORT']), auth_mechanism=dicParam['HIVE_AUTH_MECHANISM'], user=dicParam['HIVE_USERNAME'],
                       password=dicParam['HIVE_PASSWORD'], database=dicParam['HIVE_DATABASE'])
        cursor = conn.cursor()
        sql = '''
            SELECT
                channel_id,
                source_id,
                7_amount as amount_7,
                7_roi as roi_7,
                data_win 
            FROM
                tmp_data.tmp_ra_media_source 
            WHERE
                dt = `current_date` () 
                AND mgame_id = 1112 
                AND media_id = 32
        '''
        cursor.execute(sql)
        result = as_pandas(cursor)

        # 关闭链接
        cursor.close()
        conn.close()
        return result

    # 计划评分数据获取
    source_data = get_source_data()
    source_data_3 = source_data[source_data['data_win'] == 3]
    source_data_7 = source_data[source_data['data_win'] == 7]

    # 计划评分数据与源数据进行拼接
    data_not_ignore = pd.merge(data_not_ignore, source_data_3.drop(['data_win'], axis=1),
                               on=['channel_id', 'source_id'], how='left')
    data_not_ignore = data_not_ignore.rename(columns={'amount_7': 'amount_3', 'roi_7': 'roi_3'})
    data_not_ignore = pd.merge(data_not_ignore, source_data_7.drop(['data_win'], axis=1),
                               on=['channel_id', 'source_id'], how='left')

    # 依据3_roi和7_roi对计划进行判断是否关停
    if data_not_ignore.shape[0] != 0:
        data_not_ignore['label'] = data_not_ignore.apply(lambda x: 1 if (x.amount_3 >= 3000) & ((x.create_role_pay_cost >= 5500) | (x.create_role_roi == 0))
                                                else (0 if x.roi_3 >= 0.03
                                                      else (0 if (x.roi_3 >= 0.025) & (x.amount_3 <= 4000)
                                                            else (0 if (x.roi_7 >= 0.065) & (x.roi_3 >= 0.018) else 1))), axis=1)
        data_not_ignore.drop(['amount_3', 'roi_3', 'amount_7', 'roi_7'], axis=1, inplace=True)
    else:
        data_not_ignore = pd.DataFrame(
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
        data_ignore['data_win'] = 9  # 9 代表是被ignore的计划，消耗太小

    # 1\2日消耗过6000，充值为0，赋值为1，同时data_win变为-1，以便后续按datawin大小筛选优先级
    if data_2.shape[0] != 0:
        data_2['label'] = 1
        data_2.sort_values('data_win', ascending=False, inplace=True)
        data_2 = data_2.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
        data_2['data_win'] = -1  # -1 代表是被data_0的计划，累积消耗太小

    # 7天内总消耗不足4000，成本合格的计划，直接开计划，账值为0，同时data_win变为-2，以便后续按datawin大小筛选优先级
    if data_0.shape[0] != 0:
        data_0.sort_values('data_win', ascending=False, inplace=True)
        data_0 = data_0.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
        data_0['data_win'] = -2  # -1 代表是被data_0的计划，累积消耗太小

    source_predict = pd.concat([data_not_ignore, data_ignore], axis=0)
    source_predict = pd.concat([source_predict, data_0], axis=0)
    source_predict = pd.concat([source_predict, data_2], axis=0)

    # 综合结果优先级： data_0   data_2  大于 data   大于  data_ignore
    source_predict.sort_values('data_win', ascending=True, inplace=True)
    source_predict = source_predict.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
    #     print(source_predict.head())
    source_predict['label'] = source_predict['label'].astype(int)
    # print(source_predict.columns.values.tolist())
    source_predict = source_predict.fillna('null')

    return {"columns": source_predict.columns.values.tolist(), "list": source_predict.values.tolist()}


"""
金牌模型
"""

def jp_source_predict_state_1_gdt(jsondata, ):
    """ 金牌：模型1（广点通）"""
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
        data_win_0_1 = data_win_0[data_win_0['source_run_date_amount'] <= 5000]
        data_win_0_2 = data_win_0[
            (data_win_0['source_run_date_amount'] > 5000) & (data_win_0['source_run_date_amount'] <= 10000)]
        data_win_0_3 = data_win_0[
            (data_win_0['source_run_date_amount'] > 10000) & (data_win_0['source_run_date_amount'] <= 18000)]
        data_win_0_4 = data_win_0[
            (data_win_0['source_run_date_amount'] > 18000) & (data_win_0['source_run_date_amount'] <= 50000)]
        data_win_0_5 = data_win_0[data_win_0['source_run_date_amount'] > 50000]
        if data_win_0_1.shape[0] != 0:
            # data_win_0_1['label'] = data_win_0_1.apply(
            #     lambda x: 1 if (x.create_role_cost >= 200) & (x.deep_bid_type == 'BID_PER_ACTION') |
            #                    (x.create_role_cost >= 100) & (x.deep_bid_type != 'BID_PER_ACTION') else (
            #         1 if (x.create_role_roi <= 0.02)
            #              & (x.source_run_date_amount >= 3500) else (0 if (x.create_role_roi >= 0.03) else 2)), axis=1)

            data_win_0_1['label'] = data_win_0_1.apply(
                lambda x: 1 if (((x.create_role_cost >= 250) & (x.source_run_date_amount <= 1000) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 300) & (x.source_run_date_amount <= 1000) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_cost >= 210) & (x.source_run_date_amount > 1000) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 250) & (x.source_run_date_amount > 1000) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_roi < 0.03) & (x.source_run_date_amount >= 3000))) else (0
                          if (((x.create_role_cost <= 200) & (x.source_run_date_amount <= 1000) & (x.create_role_pay_sum == 0)) |
                              ((x.create_role_cost <= 220) & (x.source_run_date_amount <= 1000) & (x.create_role_pay_sum != 0)) |
                              ((x.create_role_cost <= 150) & (x.source_run_date_amount > 1000) & (x.create_role_pay_sum == 0)) |
                              ((x.create_role_cost <= 180) & (x.source_run_date_amount > 1000) & (x.create_role_pay_sum != 0)) |
                              (x.create_role_roi >= 0.05)) else 2), axis=1)

            result_df = result_df.append(data_win_0_1)

        if data_win_0_2.shape[0] != 0:
            data_win_0_2['label'] = data_win_0_2.apply(
                lambda x: 1 if (x.create_role_cost >= 180) else (1 if (x.create_role_roi <= 0.036)
                                                                      | (x.create_role_pay_cost >= 2500) else (
                    0 if (x.create_role_roi >= 0.042) else 2)), axis=1)
            result_df = result_df.append(data_win_0_2)

        if data_win_0_3.shape[0] != 0:
            data_win_0_3['label'] = data_win_0_3.apply(
                lambda x: 1 if (x.create_role_cost >= 180) else (1 if (x.create_role_roi <= 0.042)
                                                                      | (x.create_role_pay_cost >= 3500) else (
                    0 if (x.create_role_roi >= 0.048) else 2)), axis=1)
            result_df = result_df.append(data_win_0_3)

        if data_win_0_4.shape[0] != 0:
            data_win_0_4['label'] = data_win_0_4.apply(
                lambda x: 1 if (x.create_role_cost >= 160) else (1 if (x.create_role_roi <= 0.048)
                                                                      | (x.create_role_pay_cost >= 5000) else (
                    0 if (x.create_role_roi >= 0.055) else 2)), axis=1)
            result_df = result_df.append(data_win_0_4)

        if data_win_0_5.shape[0] != 0:
            data_win_0_5['label'] = data_win_0_5.apply(
                lambda x: 1 if (x.create_role_cost >= 150) else (1 if (x.create_role_roi <= 0.60)
                                                                      | (x.create_role_pay_cost >= 5500) else (
                    0 if (x.create_role_roi >= 0.80) else 2)), axis=1)
            result_df = result_df.append(data_win_0_5)

    # TODO:修改ISO
    if data_win_0_ios.shape[0] != 0:
        data_win_0_ios_1 = data_win_0_ios[data_win_0_ios['source_run_date_amount'] <= 12000]
        data_win_0_ios_2 = data_win_0_ios[
            (data_win_0_ios['source_run_date_amount'] > 12000) & (data_win_0_ios['source_run_date_amount'] <= 18000)]
        data_win_0_ios_3 = data_win_0_ios[
            (data_win_0_ios['source_run_date_amount'] > 18000) & (data_win_0_ios['source_run_date_amount'] <= 30000)]
        data_win_0_ios_4 = data_win_0_ios[
            (data_win_0_ios['source_run_date_amount'] > 30000) & (data_win_0_ios['source_run_date_amount'] <= 55000)]
        data_win_0_ios_5 = data_win_0_ios[data_win_0_ios['source_run_date_amount'] > 55000]
        if data_win_0_ios_1.shape[0] != 0:
            data_win_0_ios_1['label'] = data_win_0_ios_1.apply(
                lambda x: 1 if (((x.create_role_cost >= 650) & (x.source_run_date_amount <= 1000) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 750) & (x.source_run_date_amount <= 1000) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_cost >= 400) & (x.source_run_date_amount > 1000) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 500) & (x.source_run_date_amount > 1000) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_roi < 0.01) & (x.source_run_date_amount >= 10000))) else (0
                          if (((x.create_role_cost <= 640) & (x.source_run_date_amount <= 1000) & (x.create_role_pay_sum == 0)) |
                              ((x.create_role_cost <= 740) & (x.source_run_date_amount <= 1000) & (x.create_role_pay_sum != 0)) |
                              ((x.create_role_cost <= 390) & (x.source_run_date_amount > 1000) & (x.create_role_pay_sum == 0)) |
                              ((x.create_role_cost <= 490) & (x.source_run_date_amount > 1000) & (x.create_role_pay_sum != 0)) |
                              (x.create_role_roi >= 0.01)) else 2), axis=1)
            result_df = result_df.append(data_win_0_ios_1)

        if data_win_0_ios_2.shape[0] != 0:
            data_win_0_ios_2['label'] = data_win_0_ios_2.apply(
                lambda x: 1 if (x.create_role_cost >= 400) else (1 if (x.create_role_roi <= 0.01)
                                                                      | (x.create_role_pay_cost >= 15000) else (
                    0 if (x.create_role_roi >= 0.01) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_2)

        if data_win_0_ios_3.shape[0] != 0:
            data_win_0_ios_3['label'] = data_win_0_ios_3.apply(
                lambda x: 1 if (x.create_role_cost >= 400) else (1 if (x.create_role_roi <= 0.01)
                                                                      | (x.create_role_pay_cost >= 15000) else (
                    0 if (x.create_role_roi >= 0.01) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_3)

        if data_win_0_ios_4.shape[0] != 0:
            data_win_0_ios_4['label'] = data_win_0_ios_4.apply(
                lambda x: 1 if (x.create_role_cost >= 400) else (1 if (x.create_role_roi <= 0.015)
                                                                      | (x.create_role_pay_cost >= 12000) else (
                    0 if (x.create_role_roi >= 0.015) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_4)

        if data_win_0_ios_5.shape[0] != 0:
            data_win_0_ios_5['label'] = data_win_0_ios_5.apply(
                lambda x: 1 if (x.create_role_cost >= 400) else (1 if (x.create_role_roi <= 0.02)
                                                                      | (x.create_role_pay_cost >= 10000) else (
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

def jp_source_predict_state_2_gdt(jsondata, ):
    """ 金牌：模型2（广点通）"""
    '''
    :param jsondata: {"columns": ["channel_id","source_id","plan_name","model_run_datetime","create_time",
    "media_id","game_id","platform","data_win","source_run_date_amount","create_role_num",
    "create_role_cost","create_role_pay_num","create_role_pay_cost","create_role_pay_sum","create_role_roi",
    "create_role_retain_1d","create_role_pay_rate","create_role_pay_num_cum","learning_type","learning_time_dt",
    "learning_time_hr","deep_bid_type","cum_amount","cum_day","cum_role_cost"]}
    :return:
    '''
    data = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])

    # 数据预处理
    data = data[data['source_run_date_amount'] > 0]  # 删除消耗为0的数据
    # 去重，处理某天没有消耗的情况
    data.sort_values(by='data_win', inplace=True)
    data.drop_duplicates(subset=['channel_id', 'source_id', 'source_run_date_amount', 'create_role_num'], keep='first'
                         , inplace=True)

    # 计划7天内总消耗不足4000，且总成本合格的计划，让他继续跑，先不做判断
    data_0 = data[(data['cum_amount'] >= 200) & (data['cum_amount'] <= 4000) & (data['cum_day'] <= 7) & (
                data['cum_role_cost'] <= 100) & (data['platform'] == 1)]
    data_1 = data[(data['cum_amount'] >= 200) & (data['cum_amount'] <= 10000) & (data['cum_day'] <= 7) & (
                data['cum_role_cost'] <= 700) & (data['platform'] == 2)]
    data_0 = data_0.append(data_1)
    data_0['label'] = data_0['cum_amount'].apply(lambda x: 0)

    data = data.drop(data_0.index).reset_index(drop=True)
    # 对窗口期消耗200元以下的不作判断，对数据按平台、win进行分类处理
    data_not_ignore = data[(data['source_run_date_amount'] >= 200) | (data['data_win'] == 1)]
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

    if data_win_1.shape[0] != 0:
        data_win_1['label'] = data_win_1.apply(lambda x: 1 if x.create_role_num == 0
        else (1 if (x.create_role_pay_cost > 2000) | (x.create_role_cost > 220)
              else (1 if x.create_role_roi < 0.04
                    else (0 if x.create_role_roi >= 0.045 else 2))), axis=1)

        result_win1 = result_win1.append(data_win_1[(data_win_1['label'] == 1) | (data_win_1['label'] == 0)])
        temp_win1 = temp_win1.append(data_win_1[data_win_1['label'] == 2])
    # TODO:IOS
    if data_win_1_ios.shape[0] != 0:
        data_win_1_ios['label'] = data_win_1_ios.apply(lambda x: 1 if x.create_role_num == 0
        else (1 if (x.create_role_pay_cost > 4500) | (x.create_role_cost > 700)
              else (1 if x.create_role_roi <= 0.026
                    else (0 if x.create_role_roi >= 0.026 else 2))), axis=1)

        result_win1 = result_win1.append(
            data_win_1_ios[(data_win_1_ios['label'] == 1) | (data_win_1_ios['label'] == 0)])
        temp_win1 = temp_win1.append(data_win_1_ios[data_win_1_ios['label'] == 2])

    # win=1 模型预测
    if temp_win1.shape[0] != 0:
        temp_win1['label'] = gbdt_b_win1.predict(temp_win1[['create_role_cost', 'create_role_pay_cost', 'create_role_roi',
                       'create_role_pay_rate']])
    result_win1_data = pd.concat([result_win1, temp_win1], axis=0)

    # win=2预判
    result_win2 = pd.DataFrame()
    temp_win2 = pd.DataFrame()

    if data_win_2.shape[0] != 0:
        data_win_2['label'] = data_win_2.apply(lambda x: 1 if x.create_role_num == 0
        else (1 if (x.create_role_pay_cost > 1500) | (x.create_role_cost > 200)
              else (3 if (x.create_role_roi == 0) & (x.source_run_date_amount >= 3000)
                    else (1 if x.create_role_roi <= 0.062
                          else (0 if x.create_role_roi > 0.068 else 2)))), axis=1)
        result_win2 = result_win2.append(
            data_win_2[(data_win_2['label'] == 1) | (data_win_2['label'] == 0) | (data_win_2['label'] == 3)])
        temp_win2 = temp_win2.append(data_win_2[data_win_2['label'] == 2])
    # TODO:IOS
    if data_win_2_ios.shape[0] != 0:
        data_win_2_ios['label'] = data_win_2_ios.apply(lambda x: 1 if x.create_role_num == 0
        else (1 if (x.create_role_pay_cost > 4000) | (x.create_role_cost > 700)
              else (3 if (x.create_role_roi == 0) & (x.source_run_date_amount >= 10000)
                    else (1 if x.create_role_roi <= 0.048
                          else (0 if x.create_role_roi > 0.048 else 2)))), axis=1)

        result_win2 = result_win2.append(
            data_win_2_ios[(data_win_2_ios['label'] == 1) | (data_win_2_ios['label'] == 0)])
        temp_win2 = temp_win2.append(data_win_2_ios[data_win_2_ios['label'] == 2])

    # win=2 模型预测
    if temp_win2.shape[0] != 0:
        temp_win2['label'] = gbdt_b_win2.predict(
            temp_win2[['create_role_cost', 'create_role_pay_cost', 'create_role_roi',
                       'create_role_pay_rate', 'create_role_retain_1d']])
    result_win2_data = pd.concat([result_win2, temp_win2], axis=0)

    # win=3预判
    result_win3 = pd.DataFrame()
    temp_win3 = pd.DataFrame()
    if data_win_3.shape[0] != 0:
        data_win_3['label'] = data_win_3.apply(lambda x: 1 if x.create_role_num == 0
        else (1 if (x.create_role_pay_cost > 1500) | (x.create_role_cost > 180)
              else (0 if (x.cum_day <= 7) & (x.source_run_date_amount <= 6000) & (x.cum_role_cost <= 80)
                    else (0 if (x.create_role_pay_cost <= 550) & (x.create_role_pay_cost > 0)
                    else (0 if x.create_role_roi >= 0.093
                          else (1 if x.create_role_roi < 0.085 else 2))))), axis=1)

        result_win3 = result_win3.append(data_win_3[(data_win_3['label'] == 1) | (data_win_3['label'] == 0)])
        temp_win3 = temp_win3.append(data_win_3[data_win_3['label'] == 2])
    # TODO:IOS
    if data_win_3_ios.shape[0] != 0:
        data_win_3_ios['label'] = data_win_3_ios.apply(lambda x: 1 if x.create_role_num == 0
        else (1 if (x.create_role_pay_cost > 4000) | (x.create_role_cost > 600)
              else (0 if (x.cum_day <= 7) & (x.source_run_date_amount <= 6000) & (x.cum_role_cost <= 600)
                    else (0 if (x.create_role_pay_cost <= 2500) & (x.create_role_pay_cost > 0)
                    else (0 if x.create_role_roi >= 0.06
                          else (1 if x.create_role_roi < 0.06 else 2))))), axis=1)

        result_win3 = result_win3.append(
            data_win_3_ios[(data_win_3_ios['label'] == 1) | (data_win_3_ios['label'] == 0)])
        temp_win3 = temp_win3.append(data_win_3_ios[data_win_3_ios['label'] == 2])

    # win=3 模型预测
    if temp_win3.shape[0] != 0:
        temp_win3['label'] = gbdt_b_win3.predict(
            temp_win3[['create_role_cost', 'create_role_pay_cost', 'create_role_roi',
                       'create_role_pay_rate', 'create_role_retain_1d']])
    result_win3_data = pd.concat([result_win3, temp_win3], axis=0)

    result = pd.concat([result_win1_data, result_win2_data, result_win3_data], axis=0)
    if result.shape[0] != 0:
        result.sort_values('data_win', ascending=True, inplace=True)

    # 结果输出
    if result.shape[0] != 0:
        # 1\2\3有一个开，则开，除非win2=3
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
            #             result_1_2 = result_1_2[result_1_2['data_win'] == 1]
            result_1_2_3 = result_1_2_3.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='last')

            result_1_2_3_piv.drop(['label_1', 'label_2', 'label_3'], axis=1, inplace=True)
            source_predict = pd.merge(result_1_2_3, result_1_2_3_piv,
                                      on=['channel_id', 'source_id', 'model_run_datetime'],
                                      how='left')
            # 1\2有一个为开，则开
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
            #             result_1_2 = result_1_2[result_1_2['data_win'] == 1]
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

    # 消耗小的ignore，直接关停计划，赋值1，同时data_win变为9，以便后续按datawin大小筛选优先级
    if data_ignore.shape[0] != 0:
        data_ignore['label'] = 1
        data_ignore.sort_values('data_win', ascending=False, inplace=True)
        data_ignore = data_ignore.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
        data_ignore['data_win'] = 9  # 9 代表是被ignore的计划，消耗太小

    # 7天内总消耗不足4000，成本合格的计划，直接开计划，账值为0，同时data_win变为-1，以便后续按datawin大小筛选优先级
    if data_0.shape[0] != 0:
        data_0.sort_values('data_win', ascending=False, inplace=True)
        data_0 = data_0.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
        data_0['data_win'] = -1  # -1 代表是被data_0的计划，累积消耗太小

    source_predict = pd.concat([source_predict, data_ignore], axis=0)
    source_predict = pd.concat([source_predict, data_0], axis=0)

    # 综合结果优先级： data_0    大于 data   大于  data_ignore
    source_predict.sort_values('data_win', ascending=True, inplace=True)
    source_predict = source_predict.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
    #     print(source_predict.head())
    source_predict['label'] = source_predict['label'].astype(int)
    # print(source_predict.columns.values.tolist())
    source_predict = source_predict.fillna('null')

    return {"columns": source_predict.columns.values.tolist(), "list": source_predict.values.tolist()}


def jp_source_predict_state_1(jsondata):
    """ 金牌：模型1（头条）"""
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
    # 每次付费安卓（跑较少以防炸为主）
    if data_win_0_pre.shape[0] != 0:
        data_win_0_pre['label'] = data_win_0_pre.apply(
            lambda x: 1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount <= 1000) & (x.create_role_cost >= 220)
               else (1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount > 1000) & (x.create_role_cost >= 180)
                     else (1 if (x.create_role_roi > 0) & (x.create_role_roi <= 0.036) & (x.create_role_cost >= 300)
                           else (1 if x.create_role_cost >= 400 else 0))), axis=1)

        result_df = result_df.append(data_win_0_pre)

    # TODO:每次付费ISO
    if data_win_0_ios_pre.shape[0] != 0:
        data_win_0_ios_pre['label'] = data_win_0_ios_pre.apply(
            lambda x: 1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount <= 1000) & (x.create_role_cost >= 300)
                else (1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount > 1000) & (x.create_role_cost >= 250)
                      else (1 if (x.create_role_roi > 0) & (x.create_role_roi <= 0.02) & (x.create_role_cost >= 300)
                            else (1 if x.create_role_cost >= 350 else 0))), axis=1)
        result_df = result_df.append(data_win_0_ios_pre)

    # ROI系数安卓（严格监控）
    if data_win_0.shape[0] != 0:
        data_win_0['label'] = data_win_0.apply(
            lambda x: 1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount <= 1000) & (x.create_role_cost >= 120)
                else (1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount > 1000) & (x.create_role_cost >= 100)
                      else (1 if (x.create_role_roi > 0) & (x.create_role_roi <= 0.038) & (x.create_role_cost >= 200)
                            else (1 if x.create_role_cost >= 250 else 0))), axis=1)

        result_df = result_df.append(data_win_0)

    # TODO:其它付费方式IOS
    if data_win_0_ios.shape[0] != 0:
        data_win_0_ios['label'] = data_win_0_ios.apply(
            lambda x: 1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount <= 1000) & (x.create_role_cost >= 200)
                else (1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount > 1000) & (x.create_role_cost >= 150)
                      else (1 if (x.create_role_roi > 0) & (x.create_role_roi <= 0.02) & (x.create_role_cost >= 250)
                            else (1 if x.create_role_cost >= 250 else 0))), axis=1)
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
        data_1 = result_df_an[result_df_an['source_run_date_amount'] <= 3600]
        data_2 = result_df_an[
            (result_df_an['source_run_date_amount'] > 3600) & (result_df_an['source_run_date_amount'] <= 7000)]
        data_3 = result_df_an[
            (result_df_an['source_run_date_amount'] > 7000) & (result_df_an['source_run_date_amount'] <= 13000)]
        data_4 = result_df_an[
            (result_df_an['source_run_date_amount'] > 13000) & (result_df_an['source_run_date_amount'] <= 28000)]
        data_5 = result_df_an[
            (result_df_an['source_run_date_amount'] > 28000) & (result_df_an['source_run_date_amount'] <= 76000)]
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
                lambda x: 6600 if (x.create_role_pay_sum >= 136) & (x.create_role_pay_num >= 2) else
                (6600 if x.create_role_pay_sum >= 168 else np.nan), axis=1)
            result_df = result_df.append(data_1)
        if data_2.shape[0] != 0:
            # data_2['budget'] = data_2.apply(
            #     lambda x: 13000 if (x.create_role_roi >= 0.028) & (x.create_role_pay_num >= 2) else
            #     (13000 if x.create_role_roi >= 0.038 else np.nan), axis=1)
            data_2['budget'] = data_2.apply(
                lambda x: 12000 if (x.create_role_roi >= 0.06) & (x.create_role_pay_num >= 2) else np.nan, axis=1)
            result_df = result_df.append(data_2)
        if data_3.shape[0] != 0:
            data_3['budget'] = data_3.apply(
                lambda x: 23000 if (x.create_role_roi >= 0.1) & (x.create_role_pay_num >= 3) else
                (23000 if x.create_role_roi >= 0.12 else np.nan), axis=1)
            # data_3['budget'] = data_3.apply(
            #     lambda x: 23000 if (x.create_role_roi >= 0.035) & (x.create_role_pay_num >= 2) else
            #     np.nan, axis=1)
            result_df = result_df.append(data_3)
        if data_4.shape[0] != 0:
            data_4['budget'] = data_4.apply(
                lambda x: 50000 if (x.create_role_roi >= 0.2) & (x.create_role_pay_cost <= 550) else
                (50000 if (x.create_role_roi >= 0.23) & (x.create_role_pay_cost <= 600) else
                 np.nan), axis=1)
            result_df = result_df.append(data_4)
        if data_5.shape[0] != 0:
            data_5['budget'] = data_5.apply(
                lambda x: 95000 if (x.create_role_roi >= 0.3) & (x.create_role_pay_cost <= 500) else
                (95000 if (x.create_role_roi >= 0.34) & (x.create_role_pay_cost <= 500) else
                 np.nan), axis=1)
            result_df = result_df.append(data_5)
        if data_6.shape[0] != 0:
            data_6['budget'] = data_6.apply(
                lambda x: 148000 if (x.create_role_roi >= 0.4) & (x.create_role_pay_cost <= 450) else
                (148000 if (x.create_role_roi >= 0.45) & (x.create_role_pay_cost <= 400) else
                 np.nan), axis=1)
            result_df = result_df.append(data_6)
        if data_7.shape[0] != 0:
            data_7['budget'] = data_7.apply(
                lambda x: 220000 if (x.create_role_roi >= 0.5) & (x.create_role_pay_cost <= 400) else
                (220000 if (x.create_role_roi >= 0.56) & (x.create_role_pay_cost <= 350) else
                 np.nan), axis=1)
            result_df = result_df.append(data_7)
        if data_8.shape[0] != 0:
            data_8['budget'] = data_8.apply(
                lambda x: 275000 if (x.create_role_roi >= 0.6) & (x.create_role_pay_cost <= 360) else
                (275000 if (x.create_role_roi >= 0.67) & (x.create_role_pay_cost <= 330) else
                 np.nan), axis=1)
            result_df = result_df.append(data_8)
        if data_9.shape[0] != 0:
            data_9['budget'] = data_9.apply(
                lambda x: 320000 if (x.create_role_roi >= 0.7) & (x.create_role_pay_cost <= 320) else
                (320000 if (x.create_role_roi >= 0.78) & (x.create_role_pay_cost <= 300) else
                 np.nan), axis=1)
            result_df = result_df.append(data_9)
        if data_10.shape[0] != 0:
            data_10['budget'] = data_10.apply(
                lambda x: 450000 if (x.create_role_roi >= 0.8) & (x.create_role_pay_cost <= 300) else
                (450000 if (x.create_role_roi >= 0.89) & (x.create_role_pay_cost <= 280) else
                 np.nan), axis=1)
            result_df = result_df.append(data_10)

        # TODO:ios加预算
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
                lambda x: 13000 if (x.create_role_roi >= 0.021) & (x.create_role_pay_num >= 2) else np.nan, axis=1)
            result_df = result_df.append(data_2_ios)
        if data_3_ios.shape[0] != 0:
            # data_3_ios['budget'] = data_3_ios.apply(
            #     lambda x: 23000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_num >= 2) else
            #     np.nan, axis=1)
            data_3_ios['budget'] = data_3_ios.apply(
                lambda x: 23000 if (x.create_role_roi >= 0.022) & (x.create_role_pay_num >= 3) else
                (23000 if x.create_role_roi >= 0.035 else np.nan), axis=1)
            result_df = result_df.append(data_3_ios)
        if data_4_ios.shape[0] != 0:
            data_4_ios['budget'] = data_4_ios.apply(
                lambda x: 50000 if (x.create_role_roi >= 0.023) & (x.create_role_pay_cost <= 6000) else
                (50000 if (x.create_role_roi >= 0.028) & (x.create_role_pay_cost <= 7500) else
                 np.nan), axis=1)
            result_df = result_df.append(data_4_ios)
        if data_5_ios.shape[0] != 0:
            data_5_ios['budget'] = data_5_ios.apply(
                lambda x: 95000 if (x.create_role_roi >= 0.024) & (x.create_role_pay_cost <= 5500) else
                (95000 if (x.create_role_roi >= 0.029) & (x.create_role_pay_cost <= 7000) else
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

def jp_source_predict_state_2(jsondata, ):
    """ 金牌：模型2（头条）"""
    '''
    :param jsondata: {"columns": ["channel_id","source_id","plan_name","model_run_datetime","create_time",
    "media_id","game_id","platform","data_win","source_run_date_amount","create_role_num",
    "create_role_cost","create_role_pay_num","create_role_pay_cost","create_role_pay_sum","create_role_roi",
    "create_role_retain_1d","create_role_pay_rate","create_role_pay_num_cum","learning_type","learning_time_dt",
    "learning_time_hr","deep_bid_type", "cum_amount", "cum_day", "cum_role_cost"]}
    :return:
    '''
    data = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])
    data = data[data['media_id'] == 10]

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
                                                                                                                 x.create_role_roi <= 0.04 else (
            0 if (x.create_role_pay_num > 1) & (
                    x.create_role_roi >= 0.043) & (x.source_run_date_amount >= 500) else (
                0 if (x.create_role_roi >= 0.048) & (x.source_run_date_amount >= 500) else 1 if
                (x.create_role_pay_cost > 1000) | (x.create_role_cost > 250) else 2))), axis=1)

        result_win1 = result_win1.append(
            data_win_1_pre_action[(data_win_1_pre_action['label'] == 1) | (data_win_1_pre_action['label'] == 0)])
        temp_win1 = temp_win1.append(data_win_1_pre_action[data_win_1_pre_action['label'] == 2])

    if data_win_1.shape[0] != 0:
        data_win_1['label'] = data_win_1.apply(lambda x: 1 if x.create_role_num == 0 else (1 if
                                                                                           x.create_role_roi <= 0.045 else (
            0 if (x.create_role_pay_num > 1) & (
                    x.create_role_roi >= 0.048) & (x.source_run_date_amount >= 500) else (
                0 if (x.create_role_roi >= 0.052) & (x.source_run_date_amount >= 500) else 1 if
                (x.create_role_pay_cost > 900) | (x.create_role_cost > 220) else 2))), axis=1)

        result_win1 = result_win1.append(data_win_1[(data_win_1['label'] == 1) | (data_win_1['label'] == 0)])
        temp_win1 = temp_win1.append(data_win_1[data_win_1['label'] == 2])

    data_win_1_ios_pre_action = data_win_1_ios[data_win_1_ios['deep_bid_type'] == 'BID_PER_ACTION']
    data_win_1_ios = data_win_1_ios[data_win_1_ios['deep_bid_type'] != 'BID_PER_ACTION']

    # TODO:IOS开关计划
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
                                                                                           x.create_role_roi <= 0.068 else (
            0 if (x.create_role_pay_num > 2) & (
                    x.create_role_roi >= 0.070) & (x.source_run_date_amount >= 500) else (
                0 if (x.create_role_roi >= 0.073) & (x.source_run_date_amount >= 500) else 1 if
                (x.create_role_pay_cost > 880) | (x.create_role_cost > 200) else 2))), axis=1)

        result_win2 = result_win2.append(data_win_2[(data_win_2['label'] == 1) | (data_win_2['label'] == 0)])
        temp_win2 = temp_win2.append(data_win_2[data_win_2['label'] == 2])

    # TODO:IOS开关计划
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

    # win=3 预判
    result_win3 = pd.DataFrame()
    temp_win3 = pd.DataFrame()
    if data_win_3.shape[0] != 0:
        data_win_3['label'] = data_win_3.apply(lambda x: 1 if x.create_role_num == 0 else (1 if
                                                                                           x.create_role_roi <= 0.088 else (
            0 if (x.create_role_pay_num > 2) & (
                    x.create_role_roi >= 0.092) & (x.source_run_date_amount >= 500) else (
                0 if (x.create_role_roi >= 0.098) & (x.source_run_date_amount >= 500) else 1 if
                (x.create_role_pay_cost > 850) | (x.create_role_cost > 180) else 2))), axis=1)

        result_win3 = result_win3.append(data_win_3[(data_win_3['label'] == 1) | (data_win_3['label'] == 0)])
        temp_win3 = temp_win3.append(data_win_3[data_win_3['label'] == 2])

    # TODO:IOS开关计划
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
                     "learning_time_hr", "deep_bid_type", "cum_amount", "cum_day", "cum_role_cost", "label"])

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


# 金牌百度模型1
def source_predict_state_1_jp_bd(jsondata):
    '''
    :param jsondata: {"columns": ["channel_id","source_id","plan_name", "model_run_datetime","create_time","media_id",
    "game_id","platform","source_run_date_amount","create_role_num","create_role_cost","create_role_pay_num",
    "create_role_pay_cost","create_role_pay_sum","create_role_roi","create_role_pay_rate","create_role_pay_num_cum",
    "learning_type","learning_time_dt","learning_time_hr","deep_bid_type"]}
    :return:'label','budget'
    '''

    # 该模型只能接受计划状态plan_run_state=1且data_win=0 且media_id=45 且 model_run_datetime 等于当日的数据，即只接受当日广点通实时数据
    # label为1时关计划，为0时开计划，如果模型输出label结果与计划当前状况（开/关）相同，则不操作。例如，计划当前是关停状况，当模型输出为1时，不操作

    # 早上7：55分，将所有在运行的计划状态plan_run_state改为2。调用模型2进行判断，是否达标。达标则状态改为1，开计划，预算由模型1计算决定。不达标，则为关停状态，且plan_run_state为2

    data = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])
    data = data[data['media_id'] == 45]
    # 数据预处理
    data = data[data['source_run_date_amount'] > 0]  # 删除消耗为0的数据

    # 对消耗300元以下的不作判断，对数据按平台、win进行分类处理
    data_not_ignore = data[data['source_run_date_amount'] >= 300]

    data_win_0 = data_not_ignore[data_not_ignore['platform'] == 1]
    data_win_0_ios = data_not_ignore[data_not_ignore['platform'] == 2]

    result_df = pd.DataFrame()

    # 获取近15天平均创角成本
    def get_an_cost():
        conn = pymysql.connect(host=dicParam['DB_SLAVE_FENXI_HOST'], port=int(dicParam['DB_SLAVE_FENXI_PORT']), user=dicParam['DB_SLAVE_FENXI_USERNAME'],
                               passwd=dicParam['DB_SLAVE_FENXI_PASSWORD'])
        cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
        sql = '''
                SELECT
                    sum( amount ) / sum( create_role_num ) AS cost 
                FROM
                    db_stdata.st_lauch_report 
                WHERE
                    tdate_type = 'day' 
                    AND tdate >= DATE_SUB( date( NOW()), INTERVAL 15 DAY ) 
                    AND media_id = 45 
                    AND platform = 1 
                    AND game_id IN (
                    SELECT
                        dev_game_id AS game_id 
                    FROM
                        db_data.t_game_config 
                    WHERE
                        game_id = 1051 
                    AND dev_game_id IS NOT NULL 
                    )
        '''
        cur.execute(sql)
        result_df = pd.read_sql(sql, conn)
        cur.close()
        conn.close()
        try:
            result = int(result_df['cost'].values)
        except:
            result = 45
        return result

    def get_ios_cost():
        conn = pymysql.connect(host=dicParam['DB_SLAVE_FENXI_HOST'], port=int(dicParam['DB_SLAVE_FENXI_PORT']), user=dicParam['DB_SLAVE_FENXI_USERNAME'],
                               passwd=dicParam['DB_SLAVE_FENXI_PASSWORD'])
        cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
        sql = '''
                SELECT
                    sum( amount ) / sum( create_role_num ) AS cost 
                FROM
                    db_stdata.st_lauch_report 
                WHERE
                    tdate_type = 'day' 
                    AND tdate >= DATE_SUB( date( NOW()), INTERVAL 15 DAY ) 
                    AND media_id = 45 
                    AND platform = 2 
                    AND game_id IN (
                    SELECT
                        dev_game_id AS game_id 
                    FROM
                        db_data.t_game_config 
                    WHERE
                        game_id = 1051 
                    AND dev_game_id IS NOT NULL 
                    )
        '''
        cur.execute(sql)
        result_df = pd.read_sql(sql, conn)
        cur.close()
        conn.close()
        try:
            result = int(result_df['cost'].values)
        except:
            result = 120
        return result
    # 获取该游戏该媒体的双端创角成本
    cost_an = get_an_cost()
    cost_ios = get_ios_cost()

    # 1:关; 0:开； 2：保持原状
    # win=0预判
    if data_win_0.shape[0] != 0:
        data_win_0_1 = data_win_0[data_win_0['source_run_date_amount'] <= 3000]
        data_win_0_2 = data_win_0[(data_win_0['source_run_date_amount'] > 3000) & (data_win_0['source_run_date_amount'] <= 9000)]
        data_win_0_3 = data_win_0[(data_win_0['source_run_date_amount'] > 9000) & (data_win_0['source_run_date_amount'] <= 18000)]
        data_win_0_4 = data_win_0[(data_win_0['source_run_date_amount'] > 18000) & (data_win_0['source_run_date_amount'] <= 40000)]
        data_win_0_5 = data_win_0[data_win_0['source_run_date_amount'] > 40000]
        if data_win_0_1.shape[0] != 0:
            data_win_0_1['label'] = data_win_0_1.apply(
                lambda x: 1 if (((x.create_role_cost >= 2.5 * cost_an) & (x.source_run_date_amount <= 800) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 3.5 * cost_an) & (x.source_run_date_amount <= 800) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_cost >= 2 * cost_an) & (x.source_run_date_amount > 800) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 2.5 * cost_an) & (x.source_run_date_amount > 800) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_roi < 0.035) & (x.source_run_date_amount >= 2000))) else (0
                             if ((( x.create_role_cost <= (2.5 * cost_an - 10)) & (x.source_run_date_amount <= 800) & (x.create_role_pay_sum == 0)) |
                                 (( x.create_role_cost <= (3.5 * cost_an - 10)) & (x.source_run_date_amount <= 800) & (x.create_role_pay_sum != 0)) |
                                 (( x.create_role_cost <= (2 * cost_an - 10)) & (x.source_run_date_amount > 800) & (x.create_role_pay_sum == 0)) |
                                 (( x.create_role_cost <= (2.5 * cost_an - 10)) & (x.source_run_date_amount > 800) & (x.create_role_pay_sum != 0)) |
                                 (x.create_role_roi >= 0.035)) else 2), axis=1)

            result_df = result_df.append(data_win_0_1)

        if data_win_0_2.shape[0] != 0:
            data_win_0_2['label'] = data_win_0_2.apply(
                lambda x: 1 if (x.create_role_cost >= 3 * cost_an)
                        else (1 if (x.create_role_roi <= 0.035) | (x.create_role_pay_cost >= 2000)
                              else (0 if (x.create_role_roi >= 0.036) else 2)), axis=1)
            result_df = result_df.append(data_win_0_2)

        if data_win_0_3.shape[0] != 0:
            data_win_0_3['label'] = data_win_0_3.apply(
                lambda x: 1 if (x.create_role_cost >= 3 * cost_an)
                        else (1 if (x.create_role_roi <= 0.035) | (x.create_role_pay_cost >= 2000)
                              else (0 if (x.create_role_roi >= 0.036) else 2)), axis=1)
            result_df = result_df.append(data_win_0_3)

        if data_win_0_4.shape[0] != 0:
            data_win_0_4['label'] = data_win_0_4.apply(
                lambda x: 1 if (x.create_role_cost >= 2.8 * cost_an)
                        else (1 if (x.create_role_roi <= 0.035) | (x.create_role_pay_cost >= 1800)
                              else (0 if (x.create_role_roi >= 0.036) else 2)), axis=1)
            result_df = result_df.append(data_win_0_4)

        if data_win_0_5.shape[0] != 0:
            data_win_0_5['label'] = data_win_0_5.apply(
                lambda x: 1 if (x.create_role_cost >= 2.8 * cost_an)
                        else (1 if (x.create_role_roi <= 0.04) | (x.create_role_pay_cost >= 1500)
                              else (0 if (x.create_role_roi >= 0.041) else 2)), axis=1)
            result_df = result_df.append(data_win_0_5)

    # ISO
    if data_win_0_ios.shape[0] != 0:
        data_win_0_ios_1 = data_win_0_ios[data_win_0_ios['source_run_date_amount'] <= 6000]
        data_win_0_ios_2 = data_win_0_ios[
            (data_win_0_ios['source_run_date_amount'] > 6000) & (data_win_0_ios['source_run_date_amount'] <= 14000)]
        data_win_0_ios_3 = data_win_0_ios[
            (data_win_0_ios['source_run_date_amount'] > 14000) & (data_win_0_ios['source_run_date_amount'] <= 28000)]
        data_win_0_ios_4 = data_win_0_ios[
            (data_win_0_ios['source_run_date_amount'] > 28000) & (data_win_0_ios['source_run_date_amount'] <= 48000)]
        data_win_0_ios_5 = data_win_0_ios[data_win_0_ios['source_run_date_amount'] > 48000]
        if data_win_0_ios_1.shape[0] != 0:
            data_win_0_ios_1['label'] = data_win_0_ios_1.apply(
                lambda x: 1 if (((x.create_role_cost >= 2.5 * cost_ios) & (x.source_run_date_amount <= 1500) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 3.5 * cost_ios) & (x.source_run_date_amount <= 1500) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_cost >= 2 * cost_ios) & (x.source_run_date_amount > 1500) & (x.create_role_pay_sum == 0)) |
                                ((x.create_role_cost >= 2.5 * cost_ios) & (x.source_run_date_amount > 1500) & (x.create_role_pay_sum != 0)) |
                                ((x.create_role_roi < 0.03) & (x.source_run_date_amount >= 4000))) else (0
                          if (((x.create_role_cost <= (2.5 * cost_ios - 10)) & (x.source_run_date_amount <= 1300) & (x.create_role_pay_sum == 0)) |
                              ((x.create_role_cost <= (2.5 * cost_ios - 10)) & (x.source_run_date_amount <= 1300) & (x.create_role_pay_sum != 0)) |
                              ((x.create_role_cost <= (2.5 * cost_ios - 10)) & (x.source_run_date_amount > 1300) & (x.create_role_pay_sum == 0)) |
                              ((x.create_role_cost <= (2.5 * cost_ios - 10)) & (x.source_run_date_amount > 1300) & (x.create_role_pay_sum != 0)) |
                              (x.create_role_roi >= 0.03)) else 2), axis=1)

            result_df = result_df.append(data_win_0_ios_1)

        if data_win_0_ios_2.shape[0] != 0:
            data_win_0_ios_2['label'] = data_win_0_ios_2.apply(
                lambda x: 1 if (x.create_role_cost >= 3 * cost_ios)
                        else (1 if (x.create_role_roi <= 0.03) | (x.create_role_pay_cost >= 3000)
                              else (0 if (x.create_role_roi >= 0.031) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_2)

        if data_win_0_ios_3.shape[0] != 0:
            data_win_0_ios_3['label'] = data_win_0_ios_3.apply(
                lambda x: 1 if (x.create_role_cost >= 3 * cost_ios)
                        else (1 if (x.create_role_roi <= 0.03) | (x.create_role_pay_cost >= 3000)
                              else (0 if (x.create_role_roi >= 0.031) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_3)

        if data_win_0_ios_4.shape[0] != 0:
            data_win_0_ios_4['label'] = data_win_0_ios_4.apply(
                lambda x: 1 if (x.create_role_cost >= 2.8 * cost_ios)
                        else (1 if (x.create_role_roi <= 0.03) | (x.create_role_pay_cost >= 2500)
                              else (0 if (x.create_role_roi >= 0.031) else 2)), axis=1)
            result_df = result_df.append(data_win_0_ios_4)

        if data_win_0_ios_5.shape[0] != 0:
            data_win_0_ios_5['label'] = data_win_0_ios_5.apply(
                lambda x: 1 if (x.create_role_cost >= 2.8 * cost_ios)
                        else (1 if (x.create_role_roi <= 0.03) | (x.create_role_pay_cost >= 2000)
                              else (0 if (x.create_role_roi >= 0.031) else 2)), axis=1)
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


# 金牌百度模型2
def source_predict_state_2_jp_bd(jsondata):
    '''
    :param jsondata: {"columns": ["channel_id","source_id","plan_name","model_run_datetime","create_time",
    "media_id","game_id","platform","data_win","source_run_date_amount","create_role_num",
    "create_role_cost","create_role_pay_num","create_role_pay_cost","create_role_pay_sum","create_role_roi",
    "create_role_retain_1d","create_role_pay_rate","create_role_pay_num_cum","learning_type","learning_time_dt",
    "learning_time_hr","deep_bid_type","cum_amount","cum_day","cum_role_cost"]}
    :return:
    '''
    data = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])

    # 数据预处理
    data = data[data['source_run_date_amount'] > 0]  # 删除消耗为0的数据
    # 去重，处理某天没有消耗的情况
    data.sort_values(by='data_win', inplace=True)
    data.drop_duplicates(subset=['channel_id', 'source_id', 'source_run_date_amount', 'create_role_num'], keep='first'
                         , inplace=True)

    # 计划7天内总消耗不足4000，且总成本合格的计划，让他继续跑，先不做判断
    data_0 = data[(data['cum_amount'] >= 200) & (data['cum_amount'] <= 2000) & (data['cum_day'] <= 7) & (
                data['cum_role_cost'] <= 120) & (data['platform'] == 1)]
    data_1 = data[(data['cum_amount'] >= 200) & (data['cum_amount'] <= 4000) & (data['cum_day'] <= 7) & (
                data['cum_role_cost'] <= 300) & (data['platform'] == 2)]
    data_0 = data_0.append(data_1)
    data_0['label'] = data_0['cum_amount'].apply(lambda x: 0)

    data = data.drop(data_0.index).reset_index(drop=True)
    # 对窗口期消耗200元以下的不作判断，对数据按平台、win进行分类处理
    data_not_ignore = data[(data['source_run_date_amount'] >= 200) | (data['data_win'] == 1)]
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

    if data_win_1.shape[0] != 0:
        data_win_1['label'] = data_win_1.apply(lambda x: 1 if x.create_role_num == 0
        else (1 if (x.create_role_pay_cost > 2000) | (x.create_role_cost > 120)
              else (1 if x.create_role_roi <= 0.03
                    else (0 if x.create_role_roi >= 0.03 else 2))), axis=1)

        result_win1 = result_win1.append(data_win_1[(data_win_1['label'] == 1) | (data_win_1['label'] == 0)])
        temp_win1 = temp_win1.append(data_win_1[data_win_1['label'] == 2])

    if data_win_1_ios.shape[0] != 0:
        data_win_1_ios['label'] = data_win_1_ios.apply(lambda x: 1 if x.create_role_num == 0
        else (1 if (x.create_role_pay_cost > 3000) | (x.create_role_cost > 300)
              else (1 if x.create_role_roi <= 0.025
                    else (0 if x.create_role_roi >= 0.025 else 2))), axis=1)

        result_win1 = result_win1.append(
            data_win_1_ios[(data_win_1_ios['label'] == 1) | (data_win_1_ios['label'] == 0)])
        temp_win1 = temp_win1.append(data_win_1_ios[data_win_1_ios['label'] == 2])

    # win=1 模型预测
    if temp_win1.shape[0] != 0:
        temp_win1['label'] = gbdt_b_win1.predict(
            temp_win1[['create_role_cost', 'create_role_pay_cost', 'create_role_roi',
                       'create_role_pay_rate']])
    result_win1_data = pd.concat([result_win1, temp_win1], axis=0)

    # win=2预判
    result_win2 = pd.DataFrame()
    temp_win2 = pd.DataFrame()

    if data_win_2.shape[0] != 0:
        data_win_2['label'] = data_win_2.apply(lambda x: 1 if x.create_role_num == 0
        else (1 if (x.create_role_pay_cost > 2000) | (x.create_role_cost > 120)
              else (3 if (x.create_role_roi == 0) & (x.source_run_date_amount >= 3000)
                    else (1 if x.create_role_roi <= 0.05
                          else (0 if x.create_role_roi > 0.05 else 2)))), axis=1)
        result_win2 = result_win2.append(
            data_win_2[(data_win_2['label'] == 1) | (data_win_2['label'] == 0) | (data_win_2['label'] == 3)])
        temp_win2 = temp_win2.append(data_win_2[data_win_2['label'] == 2])

    if data_win_2_ios.shape[0] != 0:
        data_win_2_ios['label'] = data_win_2_ios.apply(lambda x: 1 if x.create_role_num == 0
        else (1 if (x.create_role_pay_cost > 3000) | (x.create_role_cost > 280)
              else (3 if (x.create_role_roi == 0) & (x.source_run_date_amount >= 4000)
                    else (1 if x.create_role_roi <= 0.045
                          else (0 if x.create_role_roi > 0.045 else 2)))), axis=1)

        result_win2 = result_win2.append(
            data_win_2_ios[(data_win_2_ios['label'] == 1) | (data_win_2_ios['label'] == 0)])
        temp_win2 = temp_win2.append(data_win_2_ios[data_win_2_ios['label'] == 2])

    # win=2 模型预测
    if temp_win2.shape[0] != 0:
        temp_win2['label'] = gbdt_b_win2.predict(
            temp_win2[['create_role_cost', 'create_role_pay_cost', 'create_role_roi',
                       'create_role_pay_rate', 'create_role_retain_1d']])
    result_win2_data = pd.concat([result_win2, temp_win2], axis=0)

    # win=3预判
    result_win3 = pd.DataFrame()
    temp_win3 = pd.DataFrame()
    if data_win_3.shape[0] != 0:
        data_win_3['label'] = data_win_3.apply(lambda x: 1 if x.create_role_num == 0
        else (1 if (x.create_role_pay_cost > 2000) | (x.create_role_cost > 120)
                    else (0 if x.create_role_roi >= 0.08
                          else (1 if x.create_role_roi <= 0.08 else 2))), axis=1)

        result_win3 = result_win3.append(data_win_3[(data_win_3['label'] == 1) | (data_win_3['label'] == 0)])
        temp_win3 = temp_win3.append(data_win_3[data_win_3['label'] == 2])

    if data_win_3_ios.shape[0] != 0:
        data_win_3_ios['label'] = data_win_3_ios.apply(lambda x: 1 if x.create_role_num == 0
        else (1 if (x.create_role_pay_cost > 3000) | (x.create_role_cost > 280)
                    else (0 if x.create_role_roi >= 0.075
                          else (1 if x.create_role_roi <= 0.075 else 2))), axis=1)

        result_win3 = result_win3.append(
            data_win_3_ios[(data_win_3_ios['label'] == 1) | (data_win_3_ios['label'] == 0)])
        temp_win3 = temp_win3.append(data_win_3_ios[data_win_3_ios['label'] == 2])

    # win=3 模型预测
    if temp_win3.shape[0] != 0:
        temp_win3['label'] = gbdt_b_win3.predict(
            temp_win3[['create_role_cost', 'create_role_pay_cost', 'create_role_roi',
                       'create_role_pay_rate', 'create_role_retain_1d']])
    result_win3_data = pd.concat([result_win3, temp_win3], axis=0)

    result = pd.concat([result_win1_data, result_win2_data, result_win3_data], axis=0)
    if result.shape[0] != 0:
        result.sort_values('data_win', ascending=True, inplace=True)

    # 结果输出
    if result.shape[0] != 0:
        # 1\2\3有一个开，则开，除非win2=3
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
            #             result_1_2 = result_1_2[result_1_2['data_win'] == 1]
            result_1_2_3 = result_1_2_3.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='last')

            result_1_2_3_piv.drop(['label_1', 'label_2', 'label_3'], axis=1, inplace=True)
            source_predict = pd.merge(result_1_2_3, result_1_2_3_piv,
                                      on=['channel_id', 'source_id', 'model_run_datetime'],
                                      how='left')
            # 1\2有一个为开，则开
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
            #             result_1_2 = result_1_2[result_1_2['data_win'] == 1]
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

    # 消耗小的ignore，直接关停计划，赋值1，同时data_win变为9，以便后续按datawin大小筛选优先级
    if data_ignore.shape[0] != 0:
        data_ignore['label'] = 1
        data_ignore.sort_values('data_win', ascending=False, inplace=True)
        data_ignore = data_ignore.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
        data_ignore['data_win'] = 9  # 9 代表是被ignore的计划，消耗太小

    # 7天内总消耗不足4000，成本合格的计划，直接开计划，账值为0，同时data_win变为-1，以便后续按datawin大小筛选优先级
    if data_0.shape[0] != 0:
        data_0.sort_values('data_win', ascending=False, inplace=True)
        data_0 = data_0.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
        data_0['data_win'] = -1  # -1 代表是被data_0的计划，累积消耗太小

    source_predict = pd.concat([source_predict, data_ignore], axis=0)
    source_predict = pd.concat([source_predict, data_0], axis=0)

    # 综合结果优先级： data_0    大于 data   大于  data_ignore
    source_predict.sort_values('data_win', ascending=True, inplace=True)
    source_predict = source_predict.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
    #     print(source_predict.head())
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
