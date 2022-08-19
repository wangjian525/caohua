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
import joblib

from ptom_utils import *

logger = logging.getLogger('PtomPredictNew')

#
# 全局变量：list要求累计、模型要求加载一次即可避免每次POST请求重新加载耗时
#
pay_sign = []  ## todo 付费标记（接口服务启动后每次调用数值累计，重启服务清零）
bid_sign = []  ## todo 调价标记（接口服务启动后每次调用数值累计，重启服务清零）
gbdt_m2_win1 = joblib.load("./aimodel/gbdt_m2_win1.pkl")  ## 模型2监控1窗口期的分类模型
gbdt_m2_win2 = joblib.load("./aimodel/gbdt_m2_win2.pkl")  ## 模型2监控2窗口期的分类模型
gbdt_m2_win3 = joblib.load("./aimodel/gbdt_m2_win3.pkl")  ## 模型2监控3窗口期的分类模型

from config import get_var
dicParam = get_var()

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
        # 样例：{"action":"image_recommendation","media":"TT","game":"末日"}
        logging.info("do PtomPredictNew post service")
        action = json.loads(web.data())['action']
        media = json.loads(web.data())['media']
        game = json.loads(web.data())['game']
        
        # try:
        #     ret = self.Process(action, media, game)
        #     logging.info(ret)
        #     return ret
        # except Exception as e:
        #     logging.error(e)
        #     timestamp = "%d" % int(round(time.time() * 1000))
        #     ret = json.dumps({"code": 500, "msg": str(e), "timestamp": timestamp})
        #     return ret
        ret = self.Process(action, media, game)
        logging.info(ret)
        return ret

    def Collect(self, media_id, mgame_id):
        ''' 测新明细 '''
        conn1 = pymysql.connect(host=dicParam['DB_SLAVE_FENXI_HOST'], port=int(dicParam['DB_SLAVE_FENXI_PORT']), user=dicParam['DB_SLAVE_FENXI_USERNAME'],
                                passwd=dicParam['DB_SLAVE_FENXI_PASSWORD'])
        ## 获取实时数据
        data = getRealData(conn1, media_id, mgame_id)  # 机器人自己的明细
        conn1.close()

        jsondata = dict()
        jsondata['columns'] = data.columns
        jsondata['data'] = data.values.tolist()

        return jsondata
       
    def Process(self, action_name, media_name, game_name):
        media_mapping = {'头条':10, '广点通':16}
        media_id = media_mapping[media_name]
        mgame_mapping = {'末日':1056, '帝国':1112, '大东家':1136}
        mgame_id = mgame_mapping[game_name]

        # 测新明细数据
        jsondata = self.Collect(media_id, mgame_id)  ## 采集数据暂不区分游戏只区分媒体

        data = {}
        ## TODO：优质计划的复制加持
        if action_name == "plan_pay_copy":
            plan_copy = self.new_ptom_plan.plan_pay_copy(jsondata, media_id, mgame_id)
            return plan_copy, json.dumps({"code": 200, "msg": "plan pay copy success!"})
        ## TODO：期望消耗的计算与优质计划的复制（暂不执行）
        elif action_name ==  "image_expamount_feedback":
            plan_copy = self.new_ptom_plan.image_expamount_copy(jsondata, media_id, mgame_id)
            return plan_copy, json.dumps({"code": 200, "msg": "image exp copy success!"})
        ## TODO：素材推送（钉钉消息）
        elif action_name ==  "image_recommendation":
            data = self.new_ptom_plan.image_recommendation(media_id, mgame_id)
            return data, json.dumps({"code": 200, "msg": "image recomm success!"})
        else:
            data = {}
        ret = json.dumps({"code": 200, "msg": "success!", "timestamp": "", "data": data})
        return ret


class NewPtomPlan(object):
    """ 新素材测新类 """
    def __init__(self, ):
        self.image_amo_limit = 10000  # 总预算限额，达到该限额的素材不再执行函数1、2
        self.plan_num_limit = 10  # 计划量限额（机器人），达到该限额的素材不再执行函数1、2
        
        self.ad_account_dict = {1001917:[11815], 1001888:[11816], 1001890:[11817],  # !!!!测新账号（大东家）
                                1001703:[11596,11597,11598,11599,11600]  # !!!!测新账号（末日）
                                }
    
    def plan_pay_copy(self, jsondata, media_id, mgame_id):
        """ [函数1：频率1天]
        Args:
            jsondata ([dict]): [测新明细]
        { ["plan_id","platform","plan_name","create_time","media_id","image_id","game_id","channel_id","source_id",
           "source_run_date_amount","create_role_num","create_role_cost","two_day_create_retain","new_role_money","new_role_rate",
           "create_role_pay_num","create_role_pay_cost","pay_role_rate","create_role_roi"]}
        """
        global pay_sign
        # 控制：消耗量
        df = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])
        # df = image_amount_limit(df, media_id, self.image_amo_limit)  # 不满足初始预算限额，素材过滤
        # 控制：计划量
        df['plan_num'] = df.groupby(['image_id'])['plan_id'].transform('count')
        df = df[df['plan_num'] <= self.plan_num_limit]
        
        # 过滤：没复制过
        df = df[df.apply(lambda x: True if x['source_run_date_amount']>=3000 and x['create_role_roi']>=0.07 and x['plan_id'] not in pay_sign else False, axis=1)]
        print("复制计划明细：", df[['plan_name','source_run_date_amount','new_role_money','create_role_roi']])
        # 新建：复制计划
        plan_result, plan_id_list = plan_pay_sql(df, media_id, mgame_id, self.ad_account_dict)
        if len(plan_result) > 0:
            plan_result.to_csv('./plan_result_copy.csv', index=0)
            # plan_result = plan_result.iloc[:1]
            # rsp = get_plan_online(plan_result, media_id)
            print("[INFO]: {}条复制计划创建完毕！".format(len(plan_result)))
            pay_sign += plan_id_list # 付费标记更新
        
        return plan_result

    
    # @staticmethod
    def image_expamount_copy(self, jsondata, media_id, mgame_id):
        """ [函数2：频率1天]
        Args:
            json_data ([dict]): [测新明细]
        {["plan_id","platform","plan_name","create_time","media_id","image_id","game_id","channel_id","source_id",
           "source_run_date_amount","create_role_num","create_role_cost","two_day_create_retain","new_role_money","new_role_rate",
           "create_role_pay_num","create_role_pay_cost","pay_role_rate","create_role_roi"]}
        """
        df = pd.DataFrame(data=jsondata['data'], columns=jsondata['columns'])
        df['plan_num'] = df.groupby(['image_id'])['plan_id'].transform('count')
        df = df[df['plan_num'] <= self.plan_num_limit]

        # 根据期望消耗复制优质计划
        plan_result = image_plan_sql(df, jsondata, media_id, mgame_id)
        
        if len(plan_result) > 0:
            # get_plan_online(plan_result, media_id)
            pass
        
        return plan_result

    
    # @staticmethod
    def image_recommendation(self, media_id, mgame_id):
        """ [函数3：频率24小时]
        文案详情
        推送条件： 前1天测新机器人所有在跑的素材
        推送字段："[素材ID、素材名称、上新时间、累计消耗、平均成本、平均首日ROI、累计ROI、素材评分]"
        """
        df_all, df_rec = image_rec_sql(media_id, mgame_id)  ## df1是推荐素材明细，df2是所有素材消耗明细
        df_all['create_time'] = df_all['create_time'].dt.strftime('%Y-%m-%d')
        data = {
                "msgtype" : "markdown",
                "markdown" : {
                    "title": "新素材推荐",
                    "text" : dingding_msg_markdown(df_all, df_rec, media_id, mgame_id),  # markdown格式✔
                    }
                }
        res = dingding_post(data, mgame_id)  ## 消息推送
        return res


if __name__ == "__main__":
    # 测试环境通过!
    pass
