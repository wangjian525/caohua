# -*- coding:utf-8 -*-
"""
   File Name：     recommended_amount.py
   Description :   接口函数：推荐消耗量级
   Author :        wangjian
   date：          2022/1/19
"""

import os
import logging
import pandas as pd
import numpy as np
import json
import time
import datetime
import warnings
warnings.filterwarnings('ignore')
warnings.simplefilter(action='ignore', category=FutureWarning)
from impala.dbapi import connect
from impala.util import as_pandas
import pymysql
import ast
pd.set_option('mode.chained_assignment', None)

logger = logging.getLogger('RecommendedAmount')

from modelservice.__myconf__ import get_var
dicParam = get_var()


class RecommendedAmount:
    """ 推荐消耗量接口类 """

    def __init__(self, ):
        self.start_time = datetime.date.today()  ## 服务启动时间
        # self.df = self.GetData()  ## 提取一次数据

    def GET(self):
        # 处理POST请求
        logging.info("do RecommendedAmount service")
        timestamp = "%d" % int(round(time.time() * 1000))
        return json.dumps({"code": 500, "msg": "Not allow GET request!", "timestamp": timestamp, "data": ""})

    def POST(self):
        # 处理POST请求
        logging.info("Doing RecommendedAmount Post Service...")
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


# 获取game_id
def get_game_id(mgame_id):
    conn = pymysql.connect(host=dicParam['DB_SLAVE_FENXI_HOST'], port=int(dicParam['DB_SLAVE_FENXI_PORT']), user=dicParam['DB_SLAVE_FENXI_USERNAME'],
                           passwd=dicParam['DB_SLAVE_FENXI_PASSWORD'])
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
        SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = {} AND dev_game_id IS NOT NULL 
    '''
    finalSql = sql.format(mgame_id)
    cur.execute(finalSql)
    result_df = cur.fetchall()
    cur.close()
    conn.close()
    return result_df


def get_game_info(mgame_id):
    game_id = get_game_id(mgame_id)
    game_id = list(map(lambda x: x['game_id'], game_id))
    game_id = [str(i) for i in game_id]
    game_id = ','.join(game_id)
    conn = pymysql.connect(host=dicParam['DB_SLAVE_FENXI_HOST'], port=int(dicParam['DB_SLAVE_FENXI_PORT']), user=dicParam['DB_SLAVE_FENXI_USERNAME'],
                           passwd=dicParam['DB_SLAVE_FENXI_PASSWORD'])
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
    /*手动查询*/
            SELECT
                game_id,
                game_name 
            FROM
                db_data.dim_game_info
            WHERE
                game_id IN ({}) 
    '''
    finalSql = sql.format(game_id)
    result_df = pd.read_sql(finalSql, conn)
    cur.close()
    conn.close()

    return result_df


# 获取数据
# 获取数据-1-3天
def get_data_1_3(mgame_id):
    game_id = get_game_id(mgame_id)
    game_id = list(map(lambda x: x['game_id'], game_id))
    game_id = [str(i) for i in game_id]
    game_id = ','.join(game_id)
    conn = connect(host=dicParam['HIVE_HOST'], port=int(dicParam['HIVE_PORT']), auth_mechanism=dicParam['HIVE_AUTH_MECHANISM'], user=dicParam['HIVE_USERNAME'],
                   password=dicParam['HIVE_PASSWORD'], database=dicParam['HIVE_DATABASE'])
    cursor = conn.cursor()
    sql = '''
            SELECT
                user_id,
                game_id,
                channel_id,
                source_id,
                role_id,
                platform,
                media_id,
                pay_num,
                pay_sum,
                create_role_time,
                created_role_day,
                pay_7_pred,
                dt 
            FROM
                tmp_data.tmp_roles_portrait_info_predict 
            WHERE
                dt = CURRENT_DATE
                AND created_role_day<=3
                AND game_id IN ({})

    '''
    finalSql = sql.format(game_id)
    cursor.execute(finalSql)
    result = as_pandas(cursor)

    # 关闭链接
    cursor.close()
    conn.close()

    return result


def get_data_1_6(mgame_id):
    game_id = get_game_id(mgame_id)
    game_id = list(map(lambda x: x['game_id'], game_id))
    game_id = [str(i) for i in game_id]
    game_id = ','.join(game_id)
    conn = connect(host=dicParam['HIVE_HOST'], port=int(dicParam['HIVE_PORT']), auth_mechanism=dicParam['HIVE_AUTH_MECHANISM'], user=dicParam['HIVE_USERNAME'],
                   password=dicParam['HIVE_PASSWORD'], database=dicParam['HIVE_DATABASE'])
    cursor = conn.cursor()
    sql = '''
            SELECT
                user_id,
                game_id,
                channel_id,
                source_id,
                role_id,
                platform,
                media_id,
                pay_num,
                pay_sum,
                create_role_time,
                created_role_day,
                pay_7_pred,
                dt 
            FROM
                tmp_data.tmp_roles_portrait_info_predict 
            WHERE
                dt = CURRENT_DATE
                AND game_id IN ({})

    '''
    finalSql = sql.format(game_id)
    cursor.execute(finalSql)
    result = as_pandas(cursor)

    # 关闭链接
    cursor.close()
    conn.close()

    return result


def get_data_7(mgame_id):
    game_id = get_game_id(mgame_id)
    game_id = list(map(lambda x: x['game_id'], game_id))
    game_id = [str(i) for i in game_id]
    game_id = ','.join(game_id)
    conn = connect(host=dicParam['HIVE_HOST'], port=int(dicParam['HIVE_PORT']), auth_mechanism=dicParam['HIVE_AUTH_MECHANISM'], user=dicParam['HIVE_USERNAME'],
                   password=dicParam['HIVE_PASSWORD'], database=dicParam['HIVE_DATABASE'])
    cursor = conn.cursor()
    sql = '''
        SELECT
            user_id,
            game_id,
            channel_id,
            source_id,
            role_id,
            platform,
            media_id,
            pay_num,
            pay_sum,
            create_role_time,
            created_role_day,
            pay_sum AS pay_7_pred,
            dt 
        FROM
            tmp_data.tmp_roles_portrait_info_train2 
        WHERE
            dt = CURRENT_DATE 
            AND created_role_day = 7
            AND game_id IN ({})
    '''
    finalSql = sql.format(game_id)
    cursor.execute(finalSql)
    result = as_pandas(cursor)

    # 关闭链接
    cursor.close()
    conn.close()

    return result


# 获取近期所有计划——提取版位、出价()
def get_plan_info(mgame_id):
    game_id = get_game_id(mgame_id)
    game_id = list(map(lambda x: x['game_id'], game_id))
    game_id = [str(i) for i in game_id]
    game_id = ','.join(game_id)
    conn = pymysql.connect(host=dicParam['DB_SLAVE_TOUFANG_HOST'], port=int(dicParam['DB_SLAVE_TOUFANG_PORT']), user=dicParam['DB_SLAVE_TOUFANG_USERNAME'],
                           passwd=dicParam['DB_SLAVE_TOUFANG_PASSWORD'], db=dicParam['DB_SLAVE_TOUFANG_DATABASE'])
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
    /*手动查询*/
        SELECT
            channel_id,
            source_id,
            media_id,
            ad_account_id,
            ad_info 
        FROM
            db_ptom.ptom_third_plan 
        WHERE
            game_id IN ({}) 
            AND create_time >= date( NOW() - INTERVAL 4320 HOUR )
    '''
    finalSql = sql.format(game_id)
    result_df = pd.read_sql(finalSql, conn)
    cur.close()
    conn.close()
    result_df = result_df.dropna(axis=0, how='any')
    result_df['channel_id'] = result_df['channel_id'].map(int)
    result_df['source_id'] = result_df['source_id'].map(int)
    result_df['media_id'] = result_df['media_id'].map(int)
    return result_df


# 获取近期所有计划的素材
def get_image_info(mgame_id):
    game_id = get_game_id(mgame_id)
    game_id = list(map(lambda x: x['game_id'], game_id))
    game_id = [str(i) for i in game_id]
    game_id = ','.join(game_id)
    conn = pymysql.connect(host=dicParam['DB_SLAVE_TOUFANG_HOST'], port=int(dicParam['DB_SLAVE_TOUFANG_PORT']), user=dicParam['DB_SLAVE_TOUFANG_USERNAME'],
                           passwd=dicParam['DB_SLAVE_TOUFANG_PASSWORD'], db=dicParam['DB_SLAVE_TOUFANG_DATABASE'])
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
    /*手动查询*/
        SELECT
            chl_user_id as channel_id,
            source_id,
            image_id
        FROM
            db_ptom.ptom_plan 
        WHERE
            game_id IN ({}) 
            AND create_time >= date( NOW() - INTERVAL 4320 HOUR )
    '''
    finalSql = sql.format(game_id)
    result_df = pd.read_sql(finalSql, conn)
    cur.close()
    conn.close()
    result_df = result_df.dropna(axis=0, how='any')
    result_df['channel_id'] = result_df['channel_id'].map(int)
    result_df['source_id'] = result_df['source_id'].map(int)
    result_df['image_id'] = result_df['image_id'].map(int)
    return result_df


# 获取近期所有计划的消耗情况
def get_amount_info(mgame_id):
    game_id = get_game_id(mgame_id)
    game_id = list(map(lambda x: x['game_id'], game_id))
    game_id = [str(i) for i in game_id]
    game_id = ','.join(game_id)
    conn = pymysql.connect(host=dicParam['DB_SLAVE_FENXI_HOST'], port=int(dicParam['DB_SLAVE_FENXI_PORT']), user=dicParam['DB_SLAVE_FENXI_USERNAME'],
                           passwd=dicParam['DB_SLAVE_FENXI_PASSWORD'])
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
    /*手动查询*/
            SELECT
                tdate,
                channel_id,
                source_id,
                game_id,
                media_id,
                platform,
                amount 
            FROM
                db_stdata.st_lauch_report 
            WHERE
                game_id IN ({}) 
                AND tdate_type = 'day' 
                AND tdate >= date( NOW() - INTERVAL 7 DAY )
                AND tdate <= date( NOW() - INTERVAL 1 DAY )
    '''
    finalSql = sql.format(game_id)
    result_df = pd.read_sql(finalSql, conn)
    cur.close()
    conn.close()

    return result_df


# 获取近期3天所有计划的消耗情况
def get_amount_info_3(mgame_id):
    game_id = get_game_id(mgame_id)
    game_id = list(map(lambda x: x['game_id'], game_id))
    game_id = [str(i) for i in game_id]
    game_id = ','.join(game_id)
    conn = pymysql.connect(host=dicParam['DB_SLAVE_FENXI_HOST'], port=int(dicParam['DB_SLAVE_FENXI_PORT']), user=dicParam['DB_SLAVE_FENXI_USERNAME'],
                           passwd=dicParam['DB_SLAVE_FENXI_PASSWORD'])
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
    /*手动查询*/
            SELECT
                tdate,
                channel_id,
                source_id,
                game_id,
                media_id,
                platform,
                amount 
            FROM
                db_stdata.st_lauch_report 
            WHERE
                game_id IN ({}) 
                AND tdate_type = 'day' 
                AND tdate >= date( NOW() - INTERVAL 3 DAY )
                AND tdate <= date( NOW() - INTERVAL 1 DAY )
    '''
    finalSql = sql.format(game_id)
    result_df = pd.read_sql(finalSql, conn)
    cur.close()
    conn.close()

    return result_df


# 聚合计划roi
def calculate_roi(data, groupby_list):
    df_pay_sum = pd.DataFrame({'7_pay_sum': data.groupby(groupby_list)['7_pay_sum'].sum()}).reset_index()
    df_amount = pd.DataFrame({'7_amount': data.groupby(groupby_list)['amount'].sum()}).reset_index()
    df = pd.merge(df_amount, df_pay_sum,on=groupby_list, how='left')
    df['7_pay_sum'] = df['7_pay_sum'].apply(lambda x: round(x, 2))
    df['7_amount'] = df['7_amount'].apply(lambda x: round(x, 2))
    df = df[df['7_amount'] > 0]
    df['7_roi'] = round(df['7_pay_sum'] / df['7_amount'], 4)
    if (('source_id' in groupby_list) | ('image_id' in groupby_list)) == False:
        df = df[df['7_amount'] > 3000].sort_values('7_roi', ascending=False).reset_index(drop=True)
    return df


def get_plan_data(mgame_id):
    # 获取预测数据
    df_1 = get_data_7(mgame_id)
    df_2 = get_data_1_6(mgame_id)
    df = df_1.append(df_2)
    df['create_role_time'] = pd.to_datetime(df['create_role_time']).dt.date
    # 聚合生成计划维度的回款预测数据
    source_df = pd.DataFrame({'7_pay_sum': df.groupby(
        ['game_id', 'channel_id', 'source_id', 'create_role_time', 'media_id', 'platform'])[
        'pay_7_pred'].sum()}).reset_index()
    source_df['media_id'] = source_df['media_id'].map(int)
    source_df = source_df[source_df['media_id'].isin([10, 16, 32, 45])]
    # 获取计划消耗数据，只取4个主流媒体
    amount_info = get_amount_info(mgame_id)
    amount_info = amount_info[amount_info['media_id'].isin([10, 16, 32, 45])]
    # 获取计划的版位和出价数据
    plan_info = get_plan_info(mgame_id)
    image_info = get_image_info(mgame_id)
    plan_info['ad_info'] = plan_info['ad_info'].apply(json.loads)
    plan_info['site_set'] = plan_info.apply(lambda x: x['ad_info']['inventory_type'] if x.media_id == 10
    else (x['ad_info']['site_set'] if x.media_id == 16
          else (x['ad_info']['scene_id'] if x.media_id == 32
                else (x['ad_info']['ftypes'] if x.media_id == 45 else np.nan))), axis=1)
    plan_info['bid_type'] = plan_info.apply(lambda x: x['ad_info']['deep_bid_type'] if x.media_id == 10
    else (x['ad_info']['optimization_goal'] if x.media_id == 16
          else (x['ad_info']['ocpx_action_type'] if x.media_id == 32
                else (x['ad_info']['ocpc']['optimizeDeepTrans'] if x.media_id == 45 else np.nan))), axis=1)
    # 拼接数据
    source_df.rename(columns={'create_role_time': 'tdate'}, inplace=True)
    source_df['tdate'] = pd.to_datetime(source_df['tdate'])
    source_df = pd.merge(source_df, amount_info,
                         on=['channel_id', 'source_id', 'tdate', 'media_id', 'platform', 'game_id'], how='outer')
    source_df['amount'] = source_df['amount'].fillna(0)
    source_df['7_pay_sum'] = source_df['7_pay_sum'].fillna(0)
    plan_info = plan_info.drop_duplicates(subset=['channel_id', 'source_id'])
    data = pd.merge(source_df, plan_info.drop(['media_id', 'ad_info'], axis=1), on=['channel_id', 'source_id'],
                    how='left')
    data = pd.merge(data, image_info, on=['channel_id', 'source_id'], how='left')
    data['site_set'] = data['site_set'].fillna("['未知']")
    data['bid_type'] = data['bid_type'].fillna('未知')
    data['site_set'] = data['site_set'].map(str)

    # 获取data_3
    df_3 = get_data_1_3(mgame_id)
    df_3['create_role_time'] = pd.to_datetime(df_3['create_role_time']).dt.date
    # 聚合生成计划维度的回款预测数据
    source_df_3 = pd.DataFrame({'7_pay_sum': df_3.groupby(
        ['game_id', 'channel_id', 'source_id', 'create_role_time', 'media_id', 'platform'])[
        'pay_7_pred'].sum()}).reset_index()
    source_df_3['media_id'] = source_df_3['media_id'].map(int)
    source_df_3 = source_df_3[source_df_3['media_id'].isin([10, 16, 32, 45])]
    # 获取计划消耗数据，只取4个主流媒体
    amount_info_3 = get_amount_info_3(mgame_id)
    amount_info_3 = amount_info_3[amount_info_3['media_id'].isin([10, 16, 32, 45])]
    # 拼接数据
    source_df_3.rename(columns={'create_role_time': 'tdate'}, inplace=True)
    source_df_3['tdate'] = pd.to_datetime(source_df_3['tdate'])
    source_df_3 = pd.merge(source_df_3, amount_info_3,
                           on=['channel_id', 'source_id', 'tdate', 'media_id', 'platform', 'game_id'], how='outer')
    source_df_3['amount'] = source_df_3['amount'].fillna(0)
    source_df_3['7_pay_sum'] = source_df_3['7_pay_sum'].fillna(0)

    data_3 = pd.merge(source_df_3, plan_info.drop(['media_id', 'ad_info'], axis=1), on=['channel_id', 'source_id'],
                      how='left')
    data_3 = pd.merge(data_3, image_info, on=['channel_id', 'source_id'], how='left')
    data_3['site_set'] = data_3['site_set'].fillna("['未知']")
    data_3['bid_type'] = data_3['bid_type'].fillna('未知')
    data_3['site_set'] = data_3['site_set'].map(str)

    return data, data_3


def load_to_hive():
    """ 写入hive """
    run_dt = datetime.datetime.now().strftime('%Y-%m-%d')
    os.system("hdfs dfs -rm -f /tmp/media_platfrom.txt")
    os.system("hdfs dfs -rm -f /tmp/media_site.txt")
    os.system("hdfs dfs -rm -f /tmp/media_game.txt")
    os.system("hdfs dfs -rm -f /tmp/media_bid.txt")
    os.system("hdfs dfs -rm -f /tmp/media_site_bid.txt")
    os.system("hdfs dfs -rm -f /tmp/media_platform_site_bid.txt")
    os.system("hdfs dfs -rm -f /tmp/media_platform_game_site_bid.txt")
    os.system("hdfs dfs -rm -f /tmp/media_account.txt")
    os.system("hdfs dfs -rm -f /tmp/media_platform_image.txt")
    os.system("hdfs dfs -rm -f /tmp/media_source.txt")

    os.system("hdfs dfs -put media_platfrom.txt /tmp")
    os.system("hdfs dfs -put media_site.txt /tmp")
    os.system("hdfs dfs -put media_game.txt /tmp")
    os.system("hdfs dfs -put media_bid.txt /tmp")
    os.system("hdfs dfs -put media_site_bid.txt /tmp")
    os.system("hdfs dfs -put media_platform_site_bid.txt /tmp")
    os.system("hdfs dfs -put media_platform_game_site_bid.txt /tmp")
    os.system("hdfs dfs -put media_account.txt /tmp")
    os.system("hdfs dfs -put media_platform_image.txt /tmp")
    os.system("hdfs dfs -put media_source.txt /tmp")

    os.system("beeline -u \"jdbc:hive2://gz-jf-bigdata-001.ch:2181,gz-jf-bigdata-002.ch:2181,gz-jf-bigdata-003.ch:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2\" -nhive -phive -e \"load data inpath '/tmp/media_platfrom.txt' overwrite into table tmp_data.tmp_ra_media_platform partition(dt='" + run_dt + "');\"")
    os.system("beeline -u \"jdbc:hive2://gz-jf-bigdata-001.ch:2181,gz-jf-bigdata-002.ch:2181,gz-jf-bigdata-003.ch:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2\" -nhive -phive -e \"load data inpath '/tmp/media_site.txt' overwrite into table tmp_data.tmp_ra_media_site partition(dt='" + run_dt + "');\"")
    os.system("beeline -u \"jdbc:hive2://gz-jf-bigdata-001.ch:2181,gz-jf-bigdata-002.ch:2181,gz-jf-bigdata-003.ch:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2\" -nhive -phive -e \"load data inpath '/tmp/media_game.txt' overwrite into table tmp_data.tmp_ra_media_game partition(dt='" + run_dt + "');\"")
    os.system("beeline -u \"jdbc:hive2://gz-jf-bigdata-001.ch:2181,gz-jf-bigdata-002.ch:2181,gz-jf-bigdata-003.ch:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2\" -nhive -phive -e \"load data inpath '/tmp/media_bid.txt' overwrite into table tmp_data.tmp_ra_media_bid partition(dt='" + run_dt + "');\"")
    os.system("beeline -u \"jdbc:hive2://gz-jf-bigdata-001.ch:2181,gz-jf-bigdata-002.ch:2181,gz-jf-bigdata-003.ch:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2\" -nhive -phive -e \"load data inpath '/tmp/media_site_bid.txt' overwrite into table tmp_data.tmp_ra_media_site_bid partition(dt='" + run_dt + "');\"")
    os.system("beeline -u \"jdbc:hive2://gz-jf-bigdata-001.ch:2181,gz-jf-bigdata-002.ch:2181,gz-jf-bigdata-003.ch:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2\" -nhive -phive -e \"load data inpath '/tmp/media_platform_site_bid.txt' overwrite into table tmp_data.tmp_ra_media_platform_site_bid partition(dt='" + run_dt + "');\"")
    os.system("beeline -u \"jdbc:hive2://gz-jf-bigdata-001.ch:2181,gz-jf-bigdata-002.ch:2181,gz-jf-bigdata-003.ch:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2\" -nhive -phive -e \"load data inpath '/tmp/media_platform_game_site_bid.txt' overwrite into table tmp_data.tmp_ra_media_platform_game_site_bid partition(dt='" + run_dt + "');\"")
    os.system("beeline -u \"jdbc:hive2://gz-jf-bigdata-001.ch:2181,gz-jf-bigdata-002.ch:2181,gz-jf-bigdata-003.ch:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2\" -nhive -phive -e \"load data inpath '/tmp/media_account.txt' overwrite into table tmp_data.tmp_ra_media_account partition(dt='" + run_dt + "');\"")
    os.system("beeline -u \"jdbc:hive2://gz-jf-bigdata-001.ch:2181,gz-jf-bigdata-002.ch:2181,gz-jf-bigdata-003.ch:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2\" -nhive -phive -e \"load data inpath '/tmp/media_platform_image.txt' overwrite into table tmp_data.tmp_ra_media_platform_image partition(dt='" + run_dt + "');\"")
    os.system("beeline -u \"jdbc:hive2://gz-jf-bigdata-001.ch:2181,gz-jf-bigdata-002.ch:2181,gz-jf-bigdata-003.ch:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2\" -nhive -phive -e \"load data inpath '/tmp/media_source.txt' overwrite into table tmp_data.tmp_ra_media_source partition(dt='" + run_dt + "');\"")

def get_recommended(data, groupby_list, mgame_id):
    result = calculate_roi(data, groupby_list)
    if mgame_id == 1056:
        roi_standard = 0.17
        roi_standard_gdt = 0.13
    elif mgame_id == 1043:
        roi_standard = 0.17
        roi_standard_gdt = 0.13
    elif mgame_id == 1112:
        roi_standard = 0.09
        roi_standard_gdt = 0.09
    elif mgame_id == 1136:
        roi_standard = 0.185
        roi_standard_gdt = 0.185

    result['score'] = result.apply(lambda x: round(x['7_roi'] / roi_standard * 100, 2) if x.media_id != 16 else round(
        x['7_roi'] / roi_standard_gdt * 100, 2), axis=1)
    result['score'] = np.clip(result['score'], 0, 100)
    result = result.sort_values('score', ascending=False)
    game_info = get_game_info(mgame_id)
    game_info = game_info.set_index("game_id").to_dict(orient='dict')["game_name"]
    site_dict = {'SITE_SET_MOBILE_UNION': '优量汇',
                 'SITE_SET_WECHAT': '微信公众号与小程序',
                 'SITE_SET_TENCENT_NEWS': '腾讯新闻',
                 'SITE_SET_TENCENT_VIDEO': '腾讯视频',
                 'SITE_SET_MOBILE_YYB': '应用宝',
                 'SITE_SET_PCQQ': 'PC QQ、QQ 空间、腾讯音乐',
                 'SITE_SET_KANDIAN': '腾讯看点',
                 'SITE_SET_QQ_MUSIC_GAME': 'QQ、腾讯音乐及游戏',
                 'SITE_SET_MOMENTS': '微信朋友圈',
                 'SITE_SET_MINI_GAME_WECHAT': '微信小游戏',
                 'SITE_SET_MINI_GAME_QQ': 'QQ 小游戏',
                 'SITE_SET_MOBILE_GAME': 'App 游戏',
                 'SITE_SET_QQSHOPPING': 'QQ 购物',
                 'INVENTORY_FEED': '头条信息流',
                 'INVENTORY_VIDEO_FEED': '西瓜信息流',
                 'INVENTORY_HOTSOON_FEED': '火山信息流',
                 'INVENTORY_AWEME_FEED': '抖音信息流',
                 'INVENTORY_UNION_SLOT': '穿山甲',
                 'UNION_BOUTIQUE_GAME': 'ohayoo精品游戏',
                 'INVENTORY_UNION_SPLASH_SLOT': '穿山甲开屏广告',
                 'INVENTORY_AWEME_SEARCH': '搜索广告——抖音位',
                 'INVENTORY_SEARCH': '搜索广告——头条位',
                 'INVENTORY_UNIVERSAL': '通投智选',
                 'INVENTORY_BEAUTY': '轻颜相机',
                 'INVENTORY_PIPIXIA': '皮皮虾',
                 'INVENTORY_AUTOMOBILE': '懂车帝',
                 'INVENTORY_STUDY': '好好学习',
                 'INVENTORY_FACE_U': 'faceu',
                 'INVENTORY_TOMATO_NOVEL': '番茄小说',
                 1: '优选广告位',
                 2: '按场景选择广告位-信息流广告',
                 6: '上下滑大屏广告',
                 7: '信息流广告',
                 11: '快看点场景',
                 24: '激励视频'}
    site_dict_45 = {1: '百度信息流',
                    2: '贴吧信息流',
                    4: '百青藤',
                    8: '好看视频',
                    64: '百度小说'}
    bid_dict = {'BID_PER_ACTION': '每次付费',
                'ROI_COEFFICIENT': 'roi系数',
                'DEEP_BID_DEFAULT': '无',
                'OPTIMIZATIONGOAL_APP_ACTIVATE': '激活',
                'OPTIMIZATIONGOAL_FIRST_PURCHASE': '首次付费',
                'OPTIMIZATIONGOAL_APP_PURCHASE': '每次付费',
                0: '未知', 2: '点击转化链接', 10: '曝光',
                11: '点击', 31: '下载完成', 53: '提交线索', 109: '电话卡激活',
                137: '量房', 180: '激活', 190: '付费',
                191: '首日 ROI', 348: '有效线索', 383: '授信', 384: '完件', 715: '微信复制', 739: '7日付费次数',
                True: '无',
                False: '付费ROI',
                '未知': '未知'}
    if 'site_set' in result.columns:
        result['site_set'] = result.apply(lambda x: "['百度优选广告位']" if (x.media_id == 45) & (x.site_set == "[]")
        else ("['自动版位']" if (x.media_id == 16) & (x.site_set == "[]")
              else ("['优选广告位']" if (x.media_id == 10) & (
                    x.site_set == "['INVENTORY_UNION_SLOT', 'INVENTORY_AWEME_FEED', 'INVENTORY_FEED', 'INVENTORY_UNION_SPLASH_SLOT', 'INVENTORY_VIDEO_FEED', 'INVENTORY_HOTSOON_FEED', 'INVENTORY_TOMATO_NOVEL']")
                    else x.site_set)), axis=1)
        result['site_set'] = result['site_set'].apply(ast.literal_eval)
        result['site_set'] = result.apply(
            lambda x: [site_dict[y] if y in site_dict else y for y in x.site_set] if x.media_id in [10, 16, 32]
            else [site_dict_45[y] if y in site_dict_45 else y for y in x.site_set], axis=1)
    if 'bid_type' in result.columns:
        result['bid_type'] = result['bid_type'].map(bid_dict)
    if 'game_id' in result.columns:
        result['game_id'] = result['game_id'].map(game_info)
    if 'ad_account_id' in result.columns:
        result['ad_account_id'] = result['ad_account_id'].map(int)
    if 'image_id' in result.columns:
        result['image_id'] = result['image_id'].map(int)
    result['score_bucket'] = result['score'].apply(lambda x: 1.3 if x >= 100
                                                        else 1.2 if x >= 90
                                                        else 1 if x >= 80
                                                        else 0.8 if x >= 70
                                                        else 0.7 if x >= 50
                                                        else 0.6)
    result['recommended_amount'] = round(((result['7_amount'] / 7) * 1 * result['score_bucket']), 2)
    result.drop('score_bucket', axis=1, inplace=True)
    result['mgame_id'] = mgame_id
    return result


def main():
    groupby_list_1 = ['media_id', 'platform']
    groupby_list_2 = ['media_id', 'site_set']
    groupby_list_3 = ['media_id', 'game_id']
    groupby_list_4 = ['media_id', 'bid_type']
    groupby_list_5 = ['media_id', 'site_set', 'bid_type']
    groupby_list_6 = ['media_id', 'platform', 'site_set', 'bid_type']
    groupby_list_7 = ['media_id', 'platform', 'game_id', 'site_set', 'bid_type']
    groupby_list_8 = ['media_id', 'ad_account_id']
    groupby_list_9 = ['media_id', 'platform', 'image_id']
    groupby_list_10 = ['media_id', 'channel_id', 'source_id']

    # 末日
    data_1056, data_1056_3 = get_plan_data(mgame_id=1056)

    result_1_mr = get_recommended(data_1056, groupby_list_1, mgame_id=1056)
    result_2_mr = get_recommended(data_1056, groupby_list_2, mgame_id=1056)
    result_3_mr = get_recommended(data_1056, groupby_list_3, mgame_id=1056)
    result_4_mr = get_recommended(data_1056, groupby_list_4, mgame_id=1056)
    result_5_mr = get_recommended(data_1056, groupby_list_5, mgame_id=1056)
    result_6_mr = get_recommended(data_1056, groupby_list_6, mgame_id=1056)
    result_7_mr = get_recommended(data_1056, groupby_list_7, mgame_id=1056)
    result_8_mr = get_recommended(data_1056, groupby_list_8, mgame_id=1056)
    result_9_mr = get_recommended(data_1056, groupby_list_9, mgame_id=1056)
    result_10_mr = get_recommended(data_1056, groupby_list_10, mgame_id=1056)
    result_10_mr_3 = get_recommended(data_1056_3, groupby_list_10, mgame_id=1056)

    # 坦克
    data_1043, data_1043_3 = get_plan_data(mgame_id=1043)

    result_1_tk = get_recommended(data_1043, groupby_list_1, mgame_id=1043)
    result_2_tk = get_recommended(data_1043, groupby_list_2, mgame_id=1043)
    result_3_tk = get_recommended(data_1043, groupby_list_3, mgame_id=1043)
    result_4_tk = get_recommended(data_1043, groupby_list_4, mgame_id=1043)
    result_5_tk = get_recommended(data_1043, groupby_list_5, mgame_id=1043)
    result_6_tk = get_recommended(data_1043, groupby_list_6, mgame_id=1043)
    result_7_tk = get_recommended(data_1043, groupby_list_7, mgame_id=1043)
    result_8_tk = get_recommended(data_1043, groupby_list_8, mgame_id=1043)
    result_9_tk = get_recommended(data_1043, groupby_list_9, mgame_id=1043)
    result_10_tk = get_recommended(data_1043, groupby_list_10, mgame_id=1043)
    result_10_tk_3 = get_recommended(data_1043_3, groupby_list_10, mgame_id=1043)

    # 帝国
    data_1112, data_1112_3 = get_plan_data(mgame_id=1112)

    result_1_dg = get_recommended(data_1112, groupby_list_1, mgame_id=1112)
    result_2_dg = get_recommended(data_1112, groupby_list_2, mgame_id=1112)
    result_3_dg = get_recommended(data_1112, groupby_list_3, mgame_id=1112)
    result_4_dg = get_recommended(data_1112, groupby_list_4, mgame_id=1112)
    result_5_dg = get_recommended(data_1112, groupby_list_5, mgame_id=1112)
    result_6_dg = get_recommended(data_1112, groupby_list_6, mgame_id=1112)
    result_7_dg = get_recommended(data_1112, groupby_list_7, mgame_id=1112)
    result_8_dg = get_recommended(data_1112, groupby_list_8, mgame_id=1112)
    result_9_dg = get_recommended(data_1112, groupby_list_9, mgame_id=1112)
    result_10_dg = get_recommended(data_1112, groupby_list_10, mgame_id=1112)
    result_10_dg_3 = get_recommended(data_1112_3, groupby_list_10, mgame_id=1112)

    # # 大东家
    # data_1136, data_1136_3 = get_plan_data(mgame_id=1136)
    #
    # result_1_ddj = get_recommended(data_1136, groupby_list_1, mgame_id=1136)
    # result_2_ddj = get_recommended(data_1136, groupby_list_2, mgame_id=1136)
    # result_3_ddj = get_recommended(data_1136, groupby_list_3, mgame_id=1136)
    # result_4_ddj = get_recommended(data_1136, groupby_list_4, mgame_id=1136)
    # result_5_ddj = get_recommended(data_1136, groupby_list_5, mgame_id=1136)
    # result_6_ddj = get_recommended(data_1136, groupby_list_6, mgame_id=1136)
    # result_7_ddj = get_recommended(data_1136, groupby_list_7, mgame_id=1136)
    # result_8_ddj = get_recommended(data_1136, groupby_list_8, mgame_id=1136)
    # result_9_ddj = get_recommended(data_1136, groupby_list_9, mgame_id=1136)
    # result_10_ddj = get_recommended(data_1136, groupby_list_10, mgame_id=1136)
    # result_10_ddj_3 = get_recommended(data_1136_3, groupby_list_10, mgame_id=1136)

    # 合并数据
    result_1 = pd.concat([result_1_mr, result_1_tk, result_1_dg], axis=0)
    result_2 = pd.concat([result_2_mr, result_2_tk, result_2_dg], axis=0)
    result_3 = pd.concat([result_3_mr, result_3_tk, result_3_dg], axis=0)
    result_4 = pd.concat([result_4_mr, result_4_tk, result_4_dg], axis=0)
    result_5 = pd.concat([result_5_mr, result_5_tk, result_5_dg], axis=0)
    result_6 = pd.concat([result_6_mr, result_6_tk, result_6_dg], axis=0)
    result_7 = pd.concat([result_7_mr, result_7_tk, result_7_dg], axis=0)
    result_8 = pd.concat([result_8_mr, result_8_tk, result_8_dg], axis=0)
    result_9 = pd.concat([result_9_mr, result_9_tk, result_9_dg], axis=0)

    result_10_1 = pd.concat([result_10_mr, result_10_tk, result_10_dg], axis=0)
    result_10_2 = pd.concat([result_10_mr_3, result_10_tk_3, result_10_dg_3], axis=0)

    # 合并3、7天计划评分数据
    result_10_1['data_win'] = 7
    result_10_2['data_win'] = 3
    result_10 = pd.concat([result_10_1, result_10_2], axis=0)

    # 获取媒体+平台数据
    result_1.to_csv('./media_platfrom.txt', index=0, encoding='utf_8_sig', header=None, sep='\t')
    # 获取媒体+版位数据
    result_2.to_csv('./media_site.txt', index=0, encoding='utf_8_sig', header=None, sep='\t')
    # 获取媒体+游戏
    result_3.to_csv('./media_game.txt', index=0, encoding='utf_8_sig', header=None, sep='\t')
    # 获取媒体+出价方式
    result_4.to_csv('./media_bid.txt', index=0, encoding='utf_8_sig', header=None, sep='\t')
    # 获取媒体+版位+出价方式
    result_5.to_csv('./media_site_bid.txt', index=0, encoding='utf_8_sig', header=None, sep='\t')
    # 获取媒体+平台+版位+出价方式
    result_6.to_csv('./media_platform_site_bid.txt', index=0, encoding='utf_8_sig', header=None, sep='\t')
    # 获取媒体+平台+游戏+版位+出价方式
    result_7.to_csv('./media_platform_game_site_bid.txt', index=0, encoding='utf_8_sig', header=None, sep='\t')
    # 获取媒体+账号
    result_8.to_csv('./media_account.txt', index=0, encoding='utf_8_sig', header=None, sep='\t')
    # 获取媒体+平台+素材
    result_9.to_csv('./media_platform_image.txt', index=0, encoding='utf_8_sig', header=None, sep='\t')
    # 获取媒体+计划
    result_10.to_csv('./media_source.txt', index=0, encoding='utf_8_sig', header=None, sep='\t')

