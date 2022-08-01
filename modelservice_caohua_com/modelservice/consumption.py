# -*- coding:utf-8 -*-
"""
   File Name：     consumption.py
   Description :   接口函数：计算每日消耗
   Author :        wangjian
   date：          2022/6/30
"""

import os
import logging
import pandas as pd
import numpy as np
import json
import warnings

warnings.filterwarnings('ignore')
warnings.simplefilter(action='ignore', category=FutureWarning)
from impala.dbapi import connect
from impala.util import as_pandas
import pymysql
import datetime, time
import calendar

pd.set_option('mode.chained_assignment', None)

logger = logging.getLogger('Consumption')

from modelservice.__myconf__ import get_var

dicParam = get_var()


class Consumption:
    """ 推荐消耗量接口类 """

    def __init__(self, ):
        self.start_time = datetime.date.today()  ## 服务启动时间
        # self.df = self.GetData()  ## 提取一次数据

    def GET(self):
        # 处理POST请求
        logging.info("do Consumption service")
        timestamp = "%d" % int(round(time.time() * 1000))
        return json.dumps({"code": 500, "msg": "Not allow GET request!", "timestamp": timestamp, "data": ""})

    def POST(self):
        # 处理POST请求
        logging.info("Doing Consumption Post Service...")
        try:
            ret = self.Process()
            logging.info(ret)
            return ret
        except Exception as e:
            logging.error(e)
            timestamp = "%d" % int(round(time.time() * 1000))
            ret = json.dumps({"code": 500, "msg": str(e), "timestamp": timestamp})
            return ret

    # 任务处理函数
    def Process(self):
        main()
        load_to_hive()
        ret = json.dumps({"code": 200, "msg": "success!", "data": "hive load success123"})
        return ret


class global_var:
    """ 全局参数配置 """
    # HIVE地址
    m_mori_10_1 = 1500000
    m_mori_16_1 = 800000
    m_mori_45_1 = 1500000
    m_mori_16_2 = 800000
    m_mori_45_2 = 800000
    m_dg_10_1 = 1500000
    m_dg_16_1 = 1000000
    m_dg_45_1 = 1500000


def get_game_id(mgame_id):
    conn = pymysql.connect(host=dicParam['DB_SLAVE_FENXI_HOST'], port=int(dicParam['DB_SLAVE_FENXI_PORT']),
                           user=dicParam['DB_SLAVE_FENXI_USERNAME'],
                           passwd=dicParam['DB_SLAVE_FENXI_PASSWORD'])
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
        SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = {} AND dev_game_id IS NOT NULL 
    '''
    f_sql = sql.format(mgame_id)
    cur.execute(f_sql)
    result_df = cur.fetchall()
    cur.close()
    conn.close()
    return result_df


def get_m_info(mgame_id, media_id, plat_form):
    game_id = get_game_id(mgame_id)
    game_id = list(map(lambda x: x['game_id'], game_id))
    game_id = [str(i) for i in game_id]
    game_id = ','.join(game_id)
    conn = pymysql.connect(host='172.18.0.8', port=4242, user='wangjian',
                           passwd='lbfl}+Kgvzo)PeSZ)nGJm3UdQ2PPdf)7}E953', db='db_bi')
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
    /*手动查询*/
        SELECT
            sum(real_consume) as real_consume,
            sum(new_role_total_recharge) as  new_role_total_recharge  
        FROM
            project_team_report 
        WHERE
            date_type = 'month' 
            AND dt = DATE_ADD( curdate(), INTERVAL - DAY ( curdate())+ 1 DAY ) 
            AND media_id = {} 
            AND op_id = 13268 
            AND platform = {} 
            AND game_id in ({})
    '''
    finalSql = sql.format(media_id, plat_form, game_id)
    result_df = pd.read_sql(finalSql, conn)
    try:
        result_df['roi'] = round(result_df['new_role_total_recharge'] / result_df['real_consume'], 4)
    except:
        result_df['roi'] = 0
    cur.close()
    conn.close()
    return result_df


def get_w_info(mgame_id, media_id, plat_form):
    conn = connect(host=dicParam['HIVE_HOST'], port=int(dicParam['HIVE_PORT']),
                   auth_mechanism=dicParam['HIVE_AUTH_MECHANISM'], user=dicParam['HIVE_USERNAME'],
                   password=dicParam['HIVE_PASSWORD'], database=dicParam['HIVE_DATABASE'])
    cursor = conn.cursor()
    sql_engine = 'set hive.execution.engine=tez'
    sql_queue = 'set tez.queue.name=offline'
    sql = '''
        SELECT
            score 
        FROM
            tmp_data.tmp_ra_media_platform 
        WHERE
            mgame_id = {} 
            AND media_id = {} 
            AND platform = {} 
            AND dt = `current_date` ()
    '''
    finalSql = sql.format(mgame_id, media_id, plat_form)
    cursor.execute(sql_engine)
    cursor.execute(sql_queue)
    cursor.execute(finalSql)
    result = as_pandas(cursor)

    # 关闭链接
    cursor.close()
    conn.close()

    return result['score'].values[0]


# 周期收益系数  alpha
def cal_alpha(result, m_day):
    if m_day <= 3:
        if result['beta'].values[0] >= 2:
            alpha = 1.4
        elif (result['beta'].values[0] >= 1.5) & (result['beta'].values[0] < 2):
            alpha = 1.3
        elif (result['beta'].values[0] >= 1.2) & (result['beta'].values[0] < 1.5):
            alpha = 1.2
        elif (result['beta'].values[0] >= 1) & (result['beta'].values[0] < 1.2):
            alpha = 1.1
        elif (result['beta'].values[0] >= 0.7) & (result['beta'].values[0] < 1):
            alpha = 1
        elif (result['beta'].values[0] >= 0.5) & (result['beta'].values[0] < 0.7):
            alpha = 0.9
        else:
            alpha = 0.8

    if (m_day > 3) & (m_day <= 5):
        if result['beta'].values[0] >= 2:
            alpha = 1.5
        elif (result['beta'].values[0] >= 1.5) & (result['beta'].values[0] < 2):
            alpha = 1.3
        elif (result['beta'].values[0] >= 1.2) & (result['beta'].values[0] < 1.5):
            alpha = 1.2
        elif (result['beta'].values[0] >= 1) & (result['beta'].values[0] < 1.2):
            alpha = 1.1
        elif (result['beta'].values[0] >= 0.8) & (result['beta'].values[0] < 1):
            alpha = 1
        elif (result['beta'].values[0] >= 0.7) & (result['beta'].values[0] < 0.8):
            alpha = 0.9
        elif (result['beta'].values[0] >= 0.6) & (result['beta'].values[0] < 0.7):
            alpha = 0.8
        elif (result['beta'].values[0] >= 0.5) & (result['beta'].values[0] < 0.6):
            alpha = 0.7
        elif (result['beta'].values[0] >= 0.4) & (result['beta'].values[0] < 0.5):
            alpha = 0.6
        else:
            alpha = 0.5

    if (m_day > 5) & (m_day <= 7):
        if result['beta'].values[0] >= 2:
            alpha = 1.8
        elif (result['beta'].values[0] >= 1.5) & (result['beta'].values[0] < 2):
            alpha = 1.5
        elif (result['beta'].values[0] >= 1.2) & (result['beta'].values[0] < 1.5):
            alpha = 1.3
        elif (result['beta'].values[0] >= 1) & (result['beta'].values[0] < 1.2):
            alpha = 1.1
        elif (result['beta'].values[0] >= 0.8) & (result['beta'].values[0] < 1):
            alpha = 0.9
        elif (result['beta'].values[0] >= 0.7) & (result['beta'].values[0] < 0.8):
            alpha = 0.8
        elif (result['beta'].values[0] >= 0.6) & (result['beta'].values[0] < 0.7):
            alpha = 0.7
        elif (result['beta'].values[0] >= 0.5) & (result['beta'].values[0] < 0.6):
            alpha = 0.6
        elif (result['beta'].values[0] >= 0.4) & (result['beta'].values[0] < 0.5):
            alpha = 0.5
        else:
            alpha = 0.4

    if (m_day > 7) & (m_day <= 14):
        if result['beta'].values[0] >= 2:
            alpha = 2
        elif (result['beta'].values[0] >= 1.5) & (result['beta'].values[0] < 2):
            alpha = 1.8
        elif (result['beta'].values[0] >= 1.2) & (result['beta'].values[0] < 1.5):
            alpha = 1.5
        elif (result['beta'].values[0] >= 1) & (result['beta'].values[0] < 1.2):
            alpha = 1.2
        elif (result['beta'].values[0] >= 0.8) & (result['beta'].values[0] < 1):
            alpha = 0.8
        elif (result['beta'].values[0] >= 0.7) & (result['beta'].values[0] < 0.8):
            alpha = 0.7
        elif (result['beta'].values[0] >= 0.6) & (result['beta'].values[0] < 0.7):
            alpha = 0.6
        elif (result['beta'].values[0] >= 0.5) & (result['beta'].values[0] < 0.6):
            alpha = 0.5
        elif (result['beta'].values[0] >= 0.4) & (result['beta'].values[0] < 0.5):
            alpha = 0.4
        else:
            alpha = 0.3

    if (m_day > 14) & (m_day <= 21):
        if result['beta'].values[0] >= 2:
            alpha = 3
        elif (result['beta'].values[0] >= 1.5) & (result['beta'].values[0] < 2):
            alpha = 2.5
        elif (result['beta'].values[0] >= 1.2) & (result['beta'].values[0] < 1.5):
            alpha = 2
        elif (result['beta'].values[0] >= 1) & (result['beta'].values[0] < 1.2):
            alpha = 1.5
        elif (result['beta'].values[0] >= 0.8) & (result['beta'].values[0] < 1):
            alpha = 0.8
        elif (result['beta'].values[0] >= 0.7) & (result['beta'].values[0] < 0.8):
            alpha = 0.7
        elif (result['beta'].values[0] >= 0.6) & (result['beta'].values[0] < 0.7):
            alpha = 0.5
        elif (result['beta'].values[0] >= 0.5) & (result['beta'].values[0] < 0.6):
            alpha = 0.3
        else:
            alpha = 0.1

    if (m_day > 21) & (m_day <= 31):
        if result['beta'].values[0] >= 2:
            alpha = 4
        elif (result['beta'].values[0] >= 1.5) & (result['beta'].values[0] < 2):
            alpha = 3
        elif (result['beta'].values[0] >= 1.2) & (result['beta'].values[0] < 1.5):
            alpha = 2.5
        elif (result['beta'].values[0] >= 1) & (result['beta'].values[0] < 1.2):
            alpha = 2
        elif (result['beta'].values[0] >= 0.8) & (result['beta'].values[0] < 1):
            alpha = 0.7
        elif (result['beta'].values[0] >= 0.7) & (result['beta'].values[0] < 0.8):
            alpha = 0.6
        elif (result['beta'].values[0] >= 0.6) & (result['beta'].values[0] < 0.7):
            alpha = 0.4
        else:
            alpha = 0
    return alpha


# 7日短期周期收益系数  alpha_w
def cal_alpha_w(result, m_day):
    if m_day <= 7:
        if result['beta_w'].values[0] >= 90:
            alpha = 1.5
        elif (result['beta_w'].values[0] >= 75) & (result['beta_w'].values[0] < 90):
            alpha = 1.3
        elif (result['beta_w'].values[0] >= 60) & (result['beta_w'].values[0] < 75):
            alpha = 1.1
        elif (result['beta_w'].values[0] >= 50) & (result['beta_w'].values[0] < 60):
            alpha = 1
        elif (result['beta_w'].values[0] >= 40) & (result['beta_w'].values[0] < 50):
            alpha = 1
        elif (result['beta_w'].values[0] >= 30) & (result['beta_w'].values[0] < 40):
            alpha = 0.9
        else:
            alpha = 0.8

    if (m_day > 7) & (m_day <= 14):
        if result['beta_w'].values[0] >= 90:
            alpha = 1.3
        elif (result['beta_w'].values[0] >= 75) & (result['beta_w'].values[0] < 90):
            alpha = 1.2
        elif (result['beta_w'].values[0] >= 60) & (result['beta_w'].values[0] < 75):
            alpha = 1.1
        elif (result['beta_w'].values[0] >= 50) & (result['beta_w'].values[0] < 60):
            alpha = 1
        elif (result['beta_w'].values[0] >= 40) & (result['beta_w'].values[0] < 50):
            alpha = 0.9
        elif (result['beta_w'].values[0] >= 30) & (result['beta_w'].values[0] < 40):
            alpha = 0.7
        else:
            alpha = 0.65

    if (m_day > 14) & (m_day <= 21):
        if result['beta_w'].values[0] >= 90:
            alpha = 1.2
        elif (result['beta_w'].values[0] >= 75) & (result['beta_w'].values[0] < 90):
            alpha = 1.1
        elif (result['beta_w'].values[0] >= 60) & (result['beta_w'].values[0] < 75):
            alpha = 1
        elif (result['beta_w'].values[0] >= 50) & (result['beta_w'].values[0] < 60):
            alpha = 0.9
        elif (result['beta_w'].values[0] >= 40) & (result['beta_w'].values[0] < 50):
            alpha = 0.8
        elif (result['beta_w'].values[0] >= 30) & (result['beta_w'].values[0] < 40):
            alpha = 0.7
        else:
            alpha = 0.6

    if (m_day > 21) & (m_day <= 31):
        if result['beta_w'].values[0] >= 90:
            alpha = 1.1
        elif (result['beta_w'].values[0] >= 75) & (result['beta_w'].values[0] < 90):
            alpha = 1
        elif (result['beta_w'].values[0] >= 60) & (result['beta_w'].values[0] < 75):
            alpha = 0.9
        elif (result['beta_w'].values[0] >= 50) & (result['beta_w'].values[0] < 60):
            alpha = 0.8
        elif (result['beta_w'].values[0] >= 40) & (result['beta_w'].values[0] < 50):
            alpha = 0.7
        elif (result['beta_w'].values[0] >= 30) & (result['beta_w'].values[0] < 40):
            alpha = 0.6
        else:
            alpha = 0.5

    return alpha


def get_d_consume(mgame_id, media_id, plat_form, M):
    result = get_m_info(mgame_id, media_id, plat_form)
    # 计算7日回款预测评分系数
    try:
        result['beta_w'] = get_w_info(mgame_id, media_id, plat_form)
    except:
        result['beta_w'] = 50
    roi_std = pd.read_excel('./aimodel/回款模型.xlsx')
    roi_std.columns = range(0, 31)
    roi_std = roi_std[roi_std[0] == mgame_id]
    roi_std = roi_std.to_dict(orient='records')[0]
    # 已经跑了几天
    m_day = time.localtime(time.time()).tm_mday - 1
    # 本月还剩几天
    r_day = calendar.monthrange(datetime.datetime.today().year, datetime.datetime.today().month)[1] - m_day
    result['m_day'] = m_day
    result['r_day'] = r_day
    result['roi_std'] = roi_std[m_day]
    # 累积roi达标率beta
    result['beta'] = round(result['roi'] / result['roi_std'], 4)
    # 计算月累积系数+近7日系数
    result['alpha'] = cal_alpha(result, m_day)
    result['alpha_w'] = cal_alpha_w(result, m_day)

    # 初始日预算量级 idb
    idb = M / 30 * 1.3

    # 过程日预算
    if m_day == 0:
        result['d_consume'] = idb
    else:
        result['d_consume'] = round(idb * result['alpha'].values[0] * result['alpha_w'].values[0], 2)
    result['mgame_id'] = mgame_id
    result['media_id'] = media_id
    result['plat_form'] = plat_form
    result = result[['real_consume', 'new_role_total_recharge', 'roi', 'm_day', 'r_day', 'roi_std', 'beta', 'beta_w',
                     'alpha', 'alpha_w', 'd_consume', 'mgame_id', 'media_id', 'plat_form']]
    return result


def load_to_hive():
    """ 写入hive """
    run_dt = datetime.datetime.now().strftime('%Y-%m-%d')
    os.system("hdfs dfs -rm -f /tmp/consumption.txt")
    os.system("hdfs dfs -put consumption.txt /tmp")
    os.system(
        "beeline -u \"jdbc:hive2://gz-jf-bigdata-001.ch:2181,gz-jf-bigdata-002.ch:2181,gz-jf-bigdata-003.ch:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2\" -nhive -phive -e \"load data inpath '/tmp/consumption.txt' overwrite into table tmp_data.tmp_consumption partition(dt='" + run_dt + "');\"")


def main():
    # 末日
    result_1 = get_d_consume(1056, 10, 1, global_var.m_mori_10_1)
    result_2 = get_d_consume(1056, 16, 1, global_var.m_mori_16_1)
    result_3 = get_d_consume(1056, 45, 1, global_var.m_mori_45_1)
    result_4 = get_d_consume(1056, 45, 2, global_var.m_mori_45_2)
    result_5 = get_d_consume(1056, 16, 2, global_var.m_mori_16_2)
    result_6 = get_d_consume(1112, 10, 1, global_var.m_dg_10_1)
    result_7 = get_d_consume(1112, 45, 1, global_var.m_dg_45_1)
    result_8 = get_d_consume(1112, 16, 1, global_var.m_dg_16_1)

    result = pd.concat([result_1, result_2, result_3, result_4, result_5, result_6, result_7, result_8], axis=0)

    # 获取媒体+平台数据
    result.to_csv('./consumption.txt', index=0, encoding='utf_8_sig', header=None, sep='\t')
