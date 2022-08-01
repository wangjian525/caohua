# -*- coding:utf-8 -*-
"""
   File Name：     pay_7_predict.py
   Description :   接口函数：7日付费预测
   Author :        royce.mao
   date：          2021/12/27 11:53 
"""

import os
import web
import json
import time
import joblib
import logging
import datetime
from impala.dbapi import connect
from impala.util import as_pandas
import pandas as pd
pd.set_option('mode.chained_assignment', None)
import numpy as np
from sklearn.preprocessing import LabelEncoder
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

from modelservice.__myconf__ import get_var
dicParam = get_var()

logger = logging.getLogger('7PayPredict')
model_path = "./aimodel/"

def model_load(mgame_id):
    """ 模型加载 """
    global lgb_pay_1d,lgb_pay_2d,lgb_pay_3d,lgb_pay_4d,lgb_pay_5d,lgb_pay_6d
    lgb_pay_1d = joblib.load(model_path + 'lgb_pay_{}_1d.pkl'.format(mgame_id))
    lgb_pay_2d = joblib.load(model_path + 'lgb_pay_{}_2d.pkl'.format(mgame_id))
    lgb_pay_3d = joblib.load(model_path + 'lgb_pay_{}_3d.pkl'.format(mgame_id))
    lgb_pay_4d = joblib.load(model_path + 'lgb_pay_{}_4d.pkl'.format(mgame_id))
    lgb_pay_5d = joblib.load(model_path + 'lgb_pay_{}_5d.pkl'.format(mgame_id))
    lgb_pay_6d = joblib.load(model_path + 'lgb_pay_{}_6d.pkl'.format(mgame_id))

class Pay7Predict:
    """ 7日付费接口类 """
    def __init__(self, ):
        self.start_time = datetime.date.today()  ## 服务启动时间
        # self.df = self.GetData()  ## 提取一次数据

    def GET(self):
        # 处理POST请求
        logging.info("do 7PayPredict service")
        timestamp = "%d" % int(round(time.time() * 1000))
        return json.dumps({"code": 500, "msg": "Not allow GET request!", "timestamp": timestamp, "data": ""})

    def POST(self):
        # 处理POST请求
        logging.info("Doing 7PayPredict Post Service...")
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

    def Process(self, ):
        mgame_ids_json = web.data()
        mgame_ids_json = json.loads(mgame_ids_json)
        mgame_ids = mgame_ids_json['mgame_ids']
        out = pd.DataFrame()
        for mgame_id in mgame_ids:
            print("[INFO] {} Dataing...".format(mgame_id))
            data = get_role_info(mgame_id)
            print("[INFO] {} Loading...".format(mgame_id))
            model_load(mgame_id)
            print("[INFO] {} Modeling...".format(mgame_id))
            if len(data) > 0:
                df_out = do_7pay_predict(data, mgame_id)
                out = out.append(df_out)
            else:
                continue
        # 数据导出
        out.reset_index(drop=True, inplace=True)
        out.to_csv('./df_role_info.txt', index=0, encoding='utf_8_sig', header=None, sep='\t')
        print("[INFO] Hiveing...")
        load_to_hive()
        ret = json.dumps({"code": 200, "msg": "7 Pay Pred is success!"})
        print("[INFO] Finished!")
        return ret


def get_role_info(mgame_id):
    """ 函数:获取6日内的新注册角色信息 """
    conn = connect(host=dicParam['HIVE_HOST'], port=int(dicParam['HIVE_PORT']), auth_mechanism=dicParam['HIVE_AUTH_MECHANISM'], user=dicParam['HIVE_USERNAME'],
                   password=dicParam['HIVE_PASSWORD'], database=dicParam['HIVE_DATABASE'])
    cursor = conn.cursor()
    sql_engine = 'set hive.execution.engine=tez'
    sql_queque = 'set tez.queue.name=offline'
    sql = ''' 
          select * from tmp_data.tmp_roles_portrait_info_train2 where dt='{}' and created_role_day < 7 and game_id in (select game_id from dim.dim_mwgame_info where mgame_id in ({})) '''
    cursor.execute(sql_engine)
    cursor.execute(sql_queque)
    cursor.execute(sql.format(datetime.datetime.now().strftime('%Y-%m-%d'), str(mgame_id)))  # 当天比如'2021-12-30'
    # cursor.execute(sql.format('2022-01-06'))  # 当天比如'2021-12-30'
    role_info = as_pandas(cursor)
    role_info.columns = [col.split('.')[-1] for col in role_info.columns]
    columns_hive = ["role_id", "game_id", "user_id", "channel_id", "source_id", "platform", "media_id", 
                    "p_model", "model_money", "sys_ver", "city_num", "device_num", "ip_num", "login_num", "sum_nums",
                    "active_days", "max_role_level", "online_time", "pay_num", "pay_sum", "pay_grade",
                    "create_role_time", "created_role_day"]
    # 关闭链接
    cursor.close()
    conn.close()
    
    return role_info[columns_hive]


def do_7pay_predict(src_role_info, mgame_id):
    """ 函数:7日付费预测 """
    '''
    Arguments:
            src_role_info {[DataFrame]} -- [采集后的数据]
    '''
    # 预处理
    col_int = ["city_num", "device_num", "ip_num", "login_num", "active_days", "max_role_level", "online_time", "pay_num"]
    src_role_info.loc[:, col_int] = src_role_info[col_int].fillna(0)
    src_role_info.loc[:, col_int] = src_role_info[col_int].astype(int)

    src_role_info['pay_sum'] = src_role_info['pay_sum'].fillna(0)
    src_role_info_pay = src_role_info[src_role_info['pay_sum'] > 0]  # 分组：模型预测
    src_role_info_nopay = src_role_info[src_role_info['pay_sum'] == 0]  # 分组：规则处理
    src_role_info_nopay['pay_7_pred'] = 0  # 全规则部分

    if len(src_role_info_pay) > 0:
        df_role_info, columns = do_preprocess(src_role_info_pay, mgame_id)
        # 预测
        def infer(df):
            """ 类似闭包 """
            # if not isinstance(df, pd.DataFrame):
            #     df = pd.DataFrame(df.values, columns=df.index)
            day = df['created_role_day'].unique().item()
            try:
                pred = eval('lgb_pay_{}d'.format(day)).predict(df.iloc[:, len(columns):])
            except:
                raise ValueError("模型变量错误!")
            df = df.iloc[:, :len(columns)]
            df.columns = columns
            df['pay_7_pred'] = np.expm1(pred)
            return df
        
        df_role_info = df_role_info.groupby('created_role_day', as_index=False).apply(infer)  # 模型预测部分
        df_role_info = do_posprocess(df_role_info, mgame_id)  # 模型后处理部分
    else:
        df_role_info = pd.DataFrame(columns=src_role_info_nopay.columns)

    assert df_role_info.columns.tolist() == src_role_info_nopay.columns.tolist(), "列不对等!"
    df_role_info = df_role_info.append(src_role_info_nopay)
    df_role_info.reset_index(drop=True, inplace=True)

    return df_role_info


def load_to_hive():
    """ 写入hive """
    run_dt = datetime.datetime.now().strftime('%Y-%m-%d')
    os.system("hdfs dfs -rm -f /tmp/df_role_info.txt")
    os.system("hdfs dfs -put df_role_info.txt /tmp")
    os.system("beeline -u \"jdbc:hive2://gz-jf-bigdata-001.ch:2181,gz-jf-bigdata-002.ch:2181,gz-jf-bigdata-003.ch:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2\" -nhive -phive -e \"load data inpath '/tmp/df_role_info.txt' overwrite into table tmp_data.tmp_roles_portrait_info_predict partition(dt='" + run_dt + "');\"")


def do_posprocess(role_info, mgame_id):
    """ 预测后处理 """
    # 规则一
    tmp = role_info[role_info['pay_7_pred'] < role_info['pay_sum']]
    role_info.loc[role_info['pay_7_pred'] < role_info['pay_sum'], 'pay_7_pred'] = tmp['pay_sum']  # TODO:不符合逻辑调整

    # 规则二
    if mgame_id == 1056:
        mapping = {1:1.30, 2:1.25, 3:1.2, 4:1.12, 5:1.09, 6:1.05}  # 7日真实:预测的经验比例系数（偏小一点）
    elif mgame_id == 1043:
        mapping = {1:1.38, 2:1.28, 3:1.27, 4:1.21, 5:1.18, 6:1.15}  # 7日真实:预测的经验比例系数（偏小一点）
    elif mgame_id == 1112:
        mapping = {1:1.48, 2:1.28, 3:1.23, 4:1.18, 5:1.16, 6:1.11}  # 7日真实:预测的经验比例系数（偏小一点）
    elif mgame_id == 1136:
        mapping = {1:1.48, 2:1.34, 3:1.19, 4:1.17, 5:1.13, 6:1.09}  # 7日真实:预测的经验比例系数（偏小一点）
    elif mgame_id == 1051:
        mapping = {1:1.46, 2:1.34, 3:1.28, 4:1.22, 5:1.18, 6:1.15}  # 7日真实:预测的经验比例系数（偏小一点）
    role_info['mapping'] = role_info['created_role_day'].map(mapping)
    role_info['pay_7_pred'] = np.maximum(role_info['pay_7_pred'] * role_info['mapping'], role_info['pay_sum'] * role_info['mapping'])  # TODO:预测值和真实值的经验比例调整
    
    # 规则三
    if mgame_id != 1051:
        mapping = {1:3.6, 2:2.8, 3:2.2, 4:1.8, 5:1.6, 6:1.4}  # 7日真实:N日真实的经验比例系数上限（偏大一点）
    else:
        mapping = {1:2.2, 2:1.7, 3:1.3, 4:1.12, 5:1.1, 6:1.08}  # 7日真实:N日真实的经验比例系数上限（偏大一点）
    role_info['mapping'] = role_info['created_role_day'].map(mapping)
    tmp = role_info[role_info['pay_7_pred'] > role_info['pay_sum'] * role_info['mapping']]
    role_info.loc[role_info['pay_7_pred'] > role_info['pay_sum'] * role_info['mapping'], 'pay_7_pred'] = tmp['pay_sum'] * tmp['mapping']  # TODO:N日真实和7日真实的经验比例调整
    
    role_info.drop(['mapping'], axis=1, inplace=True)

    return role_info

def do_preprocess(role_info, mgame_id):
    """ 字段预处理 """
    role_info_copy = role_info.copy()
    columns = role_info_copy.columns
    role_info_copy.columns = [i+1 for i in range(len(role_info.columns))]  # 临时拼接准备
    # 机型
    role_info.loc[role_info['p_model'].str.contains('HUAWEI', na=False), 'p_model'] = 'HUAWEI'
    role_info.loc[role_info['p_model'].str.contains('vivo', na=False), 'p_model'] = 'VIVO'
    role_info.loc[role_info['p_model'].str.contains('Xiaomi', na=False), 'p_model'] = 'XIAOMI'
    role_info.loc[role_info['p_model'].str.contains('OPPO', na=False), 'p_model'] = 'OPPO'
    role_info.loc[role_info['p_model'].str.contains('Meizu', na=False), 'p_model'] = 'MEIZU'
    role_info.loc[role_info['p_model'].str.contains('samsung', na=False), 'p_model'] = 'SAMSUNG'
    role_info.loc[role_info['p_model'].str.contains('Lenovo', na=False), 'p_model'] = 'Lenovo'
    role_info.loc[role_info['p_model'].str.contains('meizu', na=False), 'p_model'] = 'MEIZU'
    role_info.loc[role_info['p_model'].str.contains('OnePlus', na=False), 'p_model'] = 'OnePlus'
    role_info.loc[role_info['p_model'].str.contains('xiaomi', na=False), 'p_model'] = 'XIAOMI'
    role_info['p_model'] = role_info['p_model'].apply(lambda x:x if x in ['HUAWEI','VIVO','XIAOMI','OPPO','MEIZU','SAMSUNG','Lenovo','OnePlus'] else 'unknown')

    if mgame_id == 1056:
        CLIP = {'login_num':(1,50),'max_role_level':(1,100),'online_time':(300,50000)}
    elif mgame_id == 1043:
        CLIP = {'login_num':(1,20),'max_role_level':(1,40),'online_time':(100,10000)}
    elif mgame_id == 1112:
        CLIP = {'login_num':(1,50),'max_role_level':(1,100),'online_time':(100,50000)}
    elif mgame_id == 1136:
        CLIP = {'login_num':(1,100),'max_role_level':(1,10),'online_time':(100,20000)}
    elif mgame_id == 1051:
        CLIP = {'login_num':(1,20),'max_role_level':(1,100),'online_time':(100,20000)}
    # 截断
    role_info['login_num'] = np.clip(role_info['login_num'],CLIP['login_num'][0],CLIP['login_num'][1])
    role_info['max_role_level'] = np.clip(role_info['max_role_level'],CLIP['max_role_level'][0],CLIP['max_role_level'][1])
    role_info['online_time'] = np.clip(role_info['online_time'],CLIP['online_time'][0],CLIP['online_time'][1])
    # login_num
    tmp = role_info[role_info['login_num'] < 2]
    tmp['login_num'] = tmp['max_role_level'] * (role_info['login_num'].sum()/role_info['max_role_level'].sum())
    role_info['login_num'].update(tmp['login_num'])
    role_info['login_num'].clip(CLIP['login_num'][0],CLIP['login_num'][1],inplace=True)
    # max_role_level
    tmp = role_info[role_info['max_role_level'] < 5] if mgame_id != 1136 else role_info[role_info['max_role_level'] < 1]
    tmp['max_role_level'] = tmp['online_time'] * (role_info['max_role_level'].sum()/role_info['online_time'].sum())
    role_info['max_role_level'].update(tmp['max_role_level'])
    role_info['max_role_level'].clip(CLIP['max_role_level'][0],CLIP['max_role_level'][1],inplace=True)
    # online_time
    tmp = role_info[role_info['online_time'] < 300]
    tmp['online_time'] = tmp['max_role_level'] * (role_info['online_time'].sum()/role_info['max_role_level'].sum())
    role_info['online_time'].update(tmp['online_time'])
    role_info['online_time'].clip(CLIP['online_time'][0],CLIP['online_time'][1],inplace=True)

    def rate_transform(df):
        """ 比例特征 """
        df['pay_price_rate'] = df['pay_sum'] / (df['pay_num'] + 1)
        df['price_online_minutes_rate'] = df['pay_sum'] / (df['online_time'] + 1)
        return df

    def get_time_feature(df,col):
        """ 时间特征 """
        df[col] = pd.to_datetime(df[col])
        # df['hour'] = df[col].dt.hour
        df['is_weekend'] = df[col].dt.weekday
        df['is_weekend'] = (df['is_weekend'] >= 5) + 0
        holidays = ['2021-01-01','2021-01-02','2021-01-03','2021-02-11','2021-02-12','2021-02-13','2021-02-14','2021-02-15',
                    '2021-02-16','2021-02-17','2021-04-03','2021-04-04','2021-04-05','2021-05-01','2021-05-02','2021-05-03',
                    '2021-05-04','2021-05-05','2021-06-12','2021-06-13','2021-06-14','2021-09-19','2021-09-20','2021-09-21',
                    '2021-10-01','2021-10-02','2021-10-03','2021-10-04','2021-10-05',
                    '2022-01-31','2022-02-01','2022-02-02','2022-02-03','2022-02-04','2022-02-05','2022-02-06',
                    '2022-04-03','2022-04-04','2022-04-05',
                    '2022-04-30','2022-05-01','2022-05-02','2022-05-03','2022-05-04',
                    '2022-06-03','2022-06-04','2022-06-05']
        df['timestamp'] = df[col].dt.date.apply(lambda x:str(x))
        df['is_holidays'] = (df['timestamp'].isin(holidays)).astype(int)
        df.drop(['timestamp'], axis=1, inplace=True)
        return df

    def encode_count(df,column_name):
        """ 离散特征 """
        lbl = LabelEncoder()
        lbl.fit(list(df[column_name].values))
        df[column_name] = lbl.transform(list(df[column_name].values))
        return df

    role_info = rate_transform(role_info)
    role_info = get_time_feature(role_info,'create_role_time')
    role_info['pay_num'] = role_info['pay_num'].fillna(0)
    role_info['pay_sum'] = role_info['pay_sum'].fillna(0)
    
    role_info['online_time_pre_login'] = role_info['online_time'] / role_info['login_num']
    role_info['level_pre_online'] = role_info['max_role_level'] / role_info['online_time']
    role_info['level_pre_login'] = role_info['max_role_level'] / role_info['login_num']
    role_info['pay_sum_pre_num'] = role_info.apply(lambda x: 0 if x.pay_num==0 else x.pay_sum/x.pay_num, axis=1)
    
    # role_info = encode_count(role_info,'platform')
    role_info = encode_count(role_info,'media_id')
    role_info = encode_count(role_info,'p_model')

    role_info = role_info.drop(['pay_grade','sum_nums','sys_ver','model_money','user_id',"role_id",'create_role_time','channel_id','source_id','platform'], axis=1)
    role_info.reset_index(drop=True, inplace=True)
    role_info_copy.reset_index(drop=True, inplace=True)
    assert len(role_info) == len(role_info_copy), "样本量不等!"
    
    return pd.concat([role_info_copy, role_info], axis=1), columns


if __name__ == "__main__":
    # 入口
    pay = Pay7Predict()
    pay.Process()
