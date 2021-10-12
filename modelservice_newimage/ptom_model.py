# -*- coding:utf-8 -*-
"""
   File Name：     ptom_model.py
   Description :   测新计划监控：计划开关、增减与出价、预算的动态调节...
   Author :        royce.mao
   date：          2021/7/23 14:07
"""

import web
import time
import logging
import json
import pandas as pd
import numpy as np
import joblib
import datetime

from ptom_utils import *
from config import cur_config as cfg

logger = logging.getLogger('PtomPredictNew')

#
# 全局变量：list要求累计、模型要求加载一次即可避免每次POST请求重新加载耗时
#
pay_sign = []  ## todo 付费标记（接口服务启动后每次调用数值累计，重启服务清零）
bid_sign = []  ## todo 调价标记（接口服务启动后每次调用数值累计，重启服务清零）
gbdt_m2_win1 = joblib.load(cfg.GBDT_DATAWIN1_PATH)  ## 模型2监控1窗口期的分类模型
gbdt_m2_win2 = joblib.load(cfg.GBDT_DATAWIN1_PATH)  ## 模型2监控2窗口期的分类模型
gbdt_m2_win3 = joblib.load(cfg.GBDT_DATAWIN1_PATH)  ## 模型2监控3窗口期的分类模型

#
# 投放测新系统的监控模型
#
class PtomPredictNew:
    def __init__(self,): 
        logging.info("do PtomModel service")
        self.new_ptom_plan = NewPtomPlan()

    def GET(self):
        # 处理GET请求
        logging.info("do PtomPredict get service")
        timestamp = "%d" % int(round(time.time() * 1000))
        return json.dumps({"code": 500, "msg": "Not allow GET request!", "timestamp": timestamp, "data": ""})

    def POST(self):
        # 处理POST请求：定时任务
        logging.info("do PtomPredictNew post service")
        action = json.loads(web.data())['action']  ## 请求样例：{"action":"image_recommendation","media":"TT","game":"末日"}
        media = json.loads(web.data())['media']  ## 请求样例：{"action":"image_recommendation","media":"TT","game":"末日"}
        game = json.loads(web.data())['game']  ## 请求样例：{"action":"image_recommendation","media":"TT","game":"末日"}

        assert action in ["plan_pay_feedback", "plan_bid_feedback", "image_expamount_feedback", "plan_control_model1", "plan_control_model2", "image_recommendation"]
        try:
            ret = self.Process(action, media, game)
            logging.info(ret)
            return ret
        except Exception as e:
            logging.error(e)
            timestamp = "%d" % int(round(time.time() * 1000))
            ret = json.dumps({"code": 500, "msg": str(e), "timestamp": timestamp})
            return ret

    # 传参提取函数
    def Collect(self, action, media_id):
        # todo: 全逻辑改造
        conn1 = pymysql.connect(host='db-slave-modelfenxi-001.ch', port=3306, user='model_read',
                               passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8')
        conn2 = pymysql.connect(host='db-slave-modeltoufang-001.ch', port=3306, user='model_read',
                               passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8')
        ## 获取实时数据
        data = getRealData(conn1, conn2, action, media_id)
        conn1.close()
        conn2.close()

        jsondata = dict()
        jsondata['columns'] = data.columns
        jsondata['data'] = data.values.tolist()

        return jsondata
       
    # 任务处理函数
    def Process(self, action, MEDIA='头条', GAME='帝国'):
        # todo：全逻辑改造
        ## MEDIA
        if MEDIA == '头条':
            media_id = 10
        elif MEDIA == '广点通':
            media_id = 16
        else:
            raise ValueError("找不到该款媒体!")
        ## GAME
        if GAME == "帝国":
            mgame_id = cfg.DG_GAME_ID
        elif GAME == "末日":
            mgame_id = cfg.MR_GAME_ID
        else:
            raise ValueError("找不到该款游戏!")

        ## 现传参
        jsondata = self.Collect(action, media_id)  ## 采集数据暂不区分游戏只区分媒体
        data = {}
        # todo:目前因为监控模型跟账号绑定，"plan_control_model1"与"plan_control_model2"暂不用
        ## 函数1：付费计划的复制
        if action == "plan_pay_feedback":
            log = self.new_ptom_plan.plan_pay_copy(jsondata, media_id, mgame_id)  ## todo:复制付费计划
            # print(log)
            return json.dumps({"code": 200, "msg": "success!"})
        ## 函数2：跑不动计划的出价调整
        elif action ==  "plan_bid_feedback":
            data = self.new_ptom_plan.plan_nocanrun_bid(jsondata, media_id, mgame_id)  ## todo:计划调整出价
        ## 函数3：素材剩余期望消耗的计算与最佳计划复制
        elif action ==  "image_expamount_feedback":
            log = self.new_ptom_plan.image_expamount_copy(jsondata, media_id, mgame_id)  ## todo:复制最佳计划
            # print(log)
            return json.dumps({"code": 200, "msg": "success!"})
        ## 函数4：计划的模型1监控
        elif action ==  "plan_control_model1":
            data = self.new_ptom_plan.plan_control(jsondata, MODE=1)  ## todo:计划监控1
            # print(data)
        ## 函数4：计划的模型2监控
        elif action ==  "plan_control_model2":
            data = self.new_ptom_plan.plan_control(jsondata, MODE=2)  ## todo:计划监控2
            # print(data)
        ## 函数5：素材推送
        elif action ==  "image_recommendation":
            data = self.new_ptom_plan.image_recommendation(media_id, mgame_id, "markdown")  ## todo:素材推送
            return json.dumps({"code": 200, "msg": "success!"})
        else:
            data = {}
        ret = json.dumps({"code": 200, "msg": "success!", "timestamp": "", "data": data})
        return ret


class NewPtomPlan(object):
    """ 新素材测新类 """
    def __init__(self, ):
        self.image_amount_limit = 6000  ## todo：每个测新素材的初始预算限额，达到该限额的素材不再执行函数1、2、3
        self.plan_num_limit = 100  ## todo：测新计划数量限额
        self.cpa_bid_limit = 700  ## todo：测新计划出价限额
    
   # @staticmethod
    def plan_pay_copy(self, jsondata, media_id, mgame_id):
        """[函数1：频率1h]

        Args:
            jsondata ([dict]): [线上的测新计划报表数据字典]
        
        Ps：该模型只接受计划状态plan_run_state=1（在跑计划）且plan_name含有CESHI（测新计划），即所有在跑的测新计划
        {
        "columns": ["channel_id","source_id","image_id","plan_id","plan_name","model_run_datetime","create_time","media_id",
                    "game_id","platform","source_run_date_amount","create_role_num","create_role_cost","create_role_pay_num",
                    "create_role_pay_cost","create_role_pay_sum","create_role_pay_rate","create_role_roi","deep_bid_type"]
        }
        """
        global pay_sign
        ## 读取json
        df = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])
        # print("有支付的计划数量：", len(df[df['create_role_pay_num'] > 0]))
        df = amount_cum(df, media_id, self.image_amount_limit)  ## 未满初始预算限额的素材过滤
        ## 计划数
        df['plan_num'] = df.groupby(['image_id'])['plan_id'].transform('count')
        df = df[df['plan_num'] <= self.plan_num_limit]
        assert all(df['plan_num'] <= self.plan_num_limit)  ## 限制：每个候选素材的在跑测新计划数低于50

        ## 计划过滤：有付费且没复制过
        df = df[df.apply(lambda x: True if x['new_role_money'] > 0 and x['plan_id'] not in pay_sign else False, axis=1)]
        ## 付费计划：定向等详情匹配与复制
        plan_result, plan_id_list = plan_pay_sql(df, media_id, mgame_id)  ## utils付费计划采集
    
        if len(plan_result) > 0:
            plan_result = plan_result[plan_result['image_id'] > 38396]
            print(plan_result)
            # get_plan_online(plan_result, media_id)  ## 复制计划请求
            pay_sign += plan_id_list ## 付费标记更新
        
        return plan_result


    # @staticmethod
    def plan_nocanrun_bid(self, jsondata, media_id, mgame_id):
        """[函数2：频率2h]

        Args:
            json_data ([dict]): [线上的测新计划报表数据字典]
        
        Ps：该模型只能接受计划状态plan_run_state=1（在跑计划）且plan_name含有CESHI（测新计划），即所有在跑的测新计划
        {
        "columns": ["channel_id","source_id","image_id","plan_id","plan_name","model_run_datetime","create_time","media_id",
                    "game_id","platform","source_run_date_amount","create_role_num","create_role_cost","create_role_pay_num",
                    "create_role_pay_cost","create_role_pay_sum","create_role_pay_rate","create_role_roi","deep_bid_type"]
        }
        """
        global bid_sign
        ## 读取json
        df = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])
        df = amount_cum(df, media_id, self.image_amount_limit)  ## 未满初始预算限额的素材过滤

        ## 跑不动规则
        df = df[df.apply(lambda x: True if ((datetime.datetime.now() - x['create_time']).total_seconds() <= 7200 and x['source_run_date_amount'] < 500 and x['plan_id'] not in bid_sign) or
                                           ((x['source_run_date_amount'] / (datetime.datetime.now() - x['create_time']).seconds) < 0.05 and x['plan_id'] not in bid_sign)
                                           else False, axis=1)]
        ## 调整出价
        df = plan_bid_sql(df, media_id, mgame_id)  ## utils当前出价字段采集
        df = df[df['cpa_bid'] <= self.cpa_bid_limit]
        assert all(df['cpa_bid'] <= self.cpa_bid_limit)  ## 限制：最高出价低于700
        df['cpa_bid'] = df['cpa_bid'].apply(lambda x:1.1*x if 1.1*x <= self.cpa_bid_limit else x)  ## 10%调高出价

        ## 调价标记更新 - 已调价过的计划下次跑不动时不再调价
        bid_sign += df['plan_id'].values.tolist()

        df['cpa_bid'] = df['cpa_bid'].replace(np.nan, 'None')
        if len(df) > 0:
            df['create_time'] = df['create_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        print(df)
        return {"columns": df.columns.values.tolist(), "list": df.values.tolist()}

    
    # @staticmethod
    def image_expamount_copy(self, jsondata, media_id, mgame_id):
        """[函数3：频率5h]

        Args:
            json_data ([dict]): [线上的测新计划报表数据字典]

        Ps：该模型只能接受计划状态plan_run_state=1（在跑计划）且plan_name含有CESHI（测新计划），即所有在跑的测新计划
        {
        "columns": ["channel_id","source_id","image_id","plan_id","plan_name","model_run_datetime","create_time","media_id",
                    "game_id","platform","source_run_date_amount","create_role_num","create_role_cost","create_role_pay_num",
                    "create_role_pay_cost","create_role_pay_sum","create_role_pay_rate","create_role_roi","deep_bid_type"]
        }
        """
        ## 读取json
        df = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])
        df = amount_cum(df, media_id, self.image_amount_limit)  ## 未满初始预算限额的素材过滤
        ## 计数
        df['plan_num'] = df.groupby(['image_id'])['plan_id'].transform('count')
        df = df[df['plan_num'] <= self.plan_num_limit]
        assert all(df['plan_num'] <= self.plan_num_limit)  ## 限制：每个素材的在跑测新计划数低于50

        ## 素材期望消耗达标与否判断与最佳计划采集复制
        plan_result = image_plan_sql(df, jsondata, media_id, mgame_id)  ## utils达标规则与最佳计划规则
        
        if len(plan_result) > 0:
            plan_result = plan_result[plan_result['image_id'] > 38558]   ## 预发布环境请求前3条
            # plan_result.to_csv('./plan_result_mr.csv', index=0)
            print(plan_result)
            get_plan_online(plan_result, media_id)  ## 复制计划请求
        
        return plan_result

    # @staticmethod
    def plan_control(self, jsondata, MODE=1):
        """[函数4：频率模型一5min，模型二1h]

        Args:
            json_data ([dict]): [线上的测新计划报表数据字典]
            MODE ([int]): [1代表模型一，2代表模型二]

            MODE等于1时：{"columns": ["channel_id","source_id","plan_name", "model_run_datetime","create_time","media_id",
                        "game_id","platform","source_run_date_amount","create_role_num","create_role_cost","create_role_pay_num",
                        "create_role_pay_cost","create_role_pay_sum","create_role_roi","create_role_pay_rate","create_role_pay_num_cum",
                        "learning_type","learning_time_dt","learning_time_hr","deep_bid_type"]}
            
            MODE等于2时：{"columns": ["channel_id","source_id","plan_name","model_run_datetime","create_time",
                        "media_id","game_id","platform","data_win","source_run_date_amount","create_role_num",
                        "create_role_cost","create_role_pay_num","create_role_pay_cost","create_role_pay_sum","create_role_roi",
                        "create_role_retain_1d","create_role_pay_rate","create_role_pay_num_cum","learning_type","learning_time_dt",
                        "learning_time_hr","deep_bid_type"]}
        """
        global gbdt_m2_win1, gbdt_m2_win2, gbdt_m2_win3
        ## 读取json
        df = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])
        df = df[df['source_run_date_amount'] > 0]  # 删除消耗为0的计划表现

        ## 模型调用
        if MODE == 1:
            df = model_1(df)  ## 当天窗口期计划 - 调开关、预算
        elif MODE == 2:
            df = model_2(df, gbdt_m2_win1, gbdt_m2_win2, gbdt_m2_win3)  ## 非当天窗口期计划 - 调开关
        else:
            raise ValueError("MODE取值情况异常")

        return {"columns": df.columns.values.tolist(), "list": df.values.tolist()}

    
    # @staticmethod
    def image_recommendation(self, media_id, mgame_id, type='text'):
        """[函数N：素材推荐频率24小时一次]
        文案详情
        推送条件： 最近3天内，最新评分560分以上，且当前有在跑有消耗的新素材
        推送字段："[素材ID，素材名称，最新评分，在跑计划数，当日累计消耗，3日累计消耗，3日付费用户数，3日回款率]"
        """
        ## 推送爆款素材
        df1, df2 = image_rec_sql(media_id, mgame_id)  ## df1是推荐素材明细，df2是所有素材消耗明细
        df1['create_time'] = df1['create_time'].dt.strftime('%Y-%m-%d')
        print(df1)  ## 3天内所有价值素材的推送
        # print(df2)  ## 3天内测新素材的消耗明细
        data = {
                "msgtype" : "text",
                "text" : {
                    "content" : dingding_msg(df1, df2),
                    }
                } if type=='text' else \
                {
                "msgtype" : "markdown",
                "markdown" : {
                    "title": "新素材推荐（广点通）",
                    "text" : dingding_msg_markdown(df1, df2),
                    }
                }
        res = dingding_post(data, mgame_id)  ## 消息推送
        return res


if __name__ == "__main__":
    # todo:测试环境通过!
    pass
