import pandas as pd
import numpy as np
import json
import time
import datetime
from numpy import *
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
import warnings

warnings.filterwarnings('ignore')
import lightgbm as lgb
from impala.dbapi import connect
from impala.util import as_pandas
import pymysql
import ast
import requests
from itertools import combinations
from tqdm import tqdm_notebook


def get_game_id():
    conn = pymysql.connect(host='db-slave-modelfenxi-001.ch', port=3306, user='model_read',
                           passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8')
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
        SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1056 AND dev_game_id IS NOT NULL 
    '''
    cur.execute(sql)
    result_df = cur.fetchall()
    cur.close()
    conn.close()
    return result_df


# 获取近期所有计划()
def get_plan_info():
    game_id = get_game_id()
    game_id = list(map(lambda x: x['game_id'], game_id))
    game_id = [str(i) for i in game_id]
    game_id = ','.join(game_id)
    conn = pymysql.connect(host='db-slave-modeltoufang-001.ch', port=3306, user='model_read',
                           passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8', db='db_ptom')
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
    /*手动查询*/
        SELECT
                p.*,
                b.image_id
            FROM
                db_ptom.ptom_third_plan p
            left join
                db_ptom.ptom_plan b
            on p.plan_id=b.plan_id
            WHERE
                p.game_id IN ({})
                AND p.media_id = 16
                AND p.platform = 1
                AND p.create_time>= date( NOW() - INTERVAL 1440 HOUR )
                AND p.create_time<= date(NOW())
                            AND p.plan_id >= (
                                select plan_id from db_ptom.ptom_plan
                                where create_time >= date( NOW() - INTERVAL 1440 HOUR )
                                and create_time <= date( NOW() - INTERVAL 1416 HOUR )
                                limit 1
                            )
    '''
    finalSql = sql.format(game_id)
    result_df = pd.read_sql(finalSql, conn)
    cur.close()
    conn.close()
    return result_df


# 获取image_id,label_ids
def get_image_info():
    conn = pymysql.connect(host='db-slave-modelfenxi-001.ch', port=3306, user='model_read',
                           passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8')
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
        SELECT
            a.chl_user_id AS channel_id,
            a.source_id,
            a.image_id,
            b.label_ids
        FROM
            db_data_ptom.ptom_plan a
            LEFT JOIN
            db_data_ptom.ptom_image_info b
        on a.image_id = b.image_id
        WHERE
            a.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1056 AND dev_game_id IS NOT NULL ) 
            AND a.media_id = 16 
            AND a.create_time >= date( NOW() - INTERVAL 1440 HOUR ) 
        GROUP BY
            a.chl_user_id,
            a.source_id,
            a.image_id
    '''
    cur.execute(sql)
    result_df = pd.read_sql(sql, conn)
    cur.close()
    conn.close()
    return result_df


def get_launch_report():
    game_id = get_game_id()
    game_id = list(map(lambda x: x['game_id'], game_id))
    game_id = [str(i) for i in game_id]
    game_id = ','.join(game_id)

    conn = pymysql.connect(host='db-slave-modelfenxi-001.ch', port=3306, user='model_read',
                           passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8')
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
        /*手动查询*/
        SELECT
            a.chl_user_id AS channel_id,
            a.source_id AS source_id,
            b.tdate,
            b.amount,
            b.new_role_money,
            b.new_role_money / b.amount as roi,
            b.pay_role_user_num / b.create_role_num as pay_rate
        FROM
            db_data_ptom.ptom_plan a
            LEFT JOIN db_stdata.st_lauch_report b ON a.chl_user_id = b.channel_id 
            AND a.source_id = b.source_id 
        WHERE
            a.create_time >= date( NOW() - INTERVAL 1440 HOUR ) 
            AND b.tdate >= date( NOW() - INTERVAL 1440 HOUR ) 
            AND b.tdate_type = 'day' 
            AND b.media_id = 16
            AND b.game_id IN ({})
            AND b.amount >= 500
    '''
    finalSql = sql.format(game_id)
    cur.execute(finalSql)
    result_df = pd.read_sql(finalSql, conn)
    cur.close()
    conn.close()
    result_df['tdate'] = pd.to_datetime(result_df['tdate'])
    result_df = result_df.sort_values('tdate')
    result_df = result_df.drop_duplicates(['channel_id', 'source_id'], keep='first')

    return result_df


# 解析json
def get_plan_json(plan_info):
    plan_info.drop(['inventory_type', 'budget', 'bid_mode'], axis=1, inplace=True)
    plan_info.dropna(how='all', inplace=True, axis=1)
    plan_info.dropna(subset=['ad_info'], inplace=True)

    plan_info['ad_info'] = plan_info['ad_info'].apply(json.loads)
    temp = plan_info['ad_info'].apply(pd.Series)
    plan_info = pd.concat([plan_info, temp], axis=1)
    plan_info.drop('ad_info', axis=1, inplace=True)

    temp = plan_info['targeting'].apply(pd.Series)
    plan_info = pd.concat([plan_info, temp], axis=1)
    plan_info.drop('targeting', axis=1, inplace=True)

    temp = plan_info['deep_conversion_spec'].apply(pd.Series)
    plan_info = pd.concat([plan_info, temp], axis=1)
    plan_info.drop('deep_conversion_spec', axis=1, inplace=True)

    temp = plan_info['behavior_or_interest'].apply(pd.Series)
    plan_info = pd.concat([plan_info, temp], axis=1)
    plan_info.drop('behavior_or_interest', axis=1, inplace=True)
    plan_info.drop(0, axis=1, inplace=True)

    temp = plan_info['intention'].apply(pd.Series)
    plan_info = pd.concat([plan_info, temp], axis=1)
    plan_info.drop('intention', axis=1, inplace=True)
    plan_info = plan_info.rename(columns={'targeting_tags': 'intention_targeting_tags'})
    plan_info.drop(0, axis=1, inplace=True)

    temp = plan_info['interest'].apply(pd.Series)
    plan_info = pd.concat([plan_info, temp], axis=1)
    plan_info.drop('interest', axis=1, inplace=True)
    plan_info = plan_info.rename(
        columns={'category_id_list': 'interest_category_id_list', 'keyword_list': 'interest_keyword_list',
                 'targeting_tags': 'interest_targeting_tags'})
    plan_info.drop(0, axis=1, inplace=True)

    temp = plan_info['behavior'].apply(pd.Series)
    plan_info = pd.concat([plan_info, temp], axis=1)
    plan_info.drop('behavior', axis=1, inplace=True)
    temp = plan_info[0].apply(pd.Series)
    plan_info = pd.concat([plan_info, temp], axis=1)
    plan_info.drop(0, axis=1, inplace=True)
    plan_info = plan_info.rename(columns={'category_id_list': 'behavior_category_id_list',
                                          'intensity': 'behavior_intensity',
                                          'keyword_list': 'behavior_keyword_list',
                                          'scene': 'behavior_scene',
                                          'targeting_tags': 'behavior_targeting_tags',
                                          'time_window': 'behavior_time_window'})

    temp = plan_info['excluded_converted_audience'].apply(pd.Series)
    plan_info = pd.concat([plan_info, temp], axis=1)
    plan_info.drop('excluded_converted_audience', axis=1, inplace=True)
    plan_info.drop(0, axis=1, inplace=True)

    temp = plan_info['geo_location'].apply(pd.Series)
    plan_info = pd.concat([plan_info, temp], axis=1)
    plan_info.drop('geo_location', axis=1, inplace=True)
    plan_info.drop(0, axis=1, inplace=True)

    # 过滤一对多计划
    plan_info['ad_id_count'] = plan_info.groupby('plan_id')['ad_id'].transform('count')
    plan_info = plan_info[plan_info['ad_id_count'] == 1]

    # 删除纯买激活的计划
    plan_info = plan_info[~((plan_info['deep_conversion_type'].isna()) & (
            plan_info['optimization_goal'] == 'OPTIMIZATIONGOAL_APP_ACTIVATE'))]
    # 删除auto_audience=True 的记录，并且删除auto_audience字段
    plan_info[plan_info['auto_audience'] == False]
    plan_info = plan_info[['ad_account_id', 'game_id', 'channel_id', 'source_id', 'budget_mode',
                           'create_time', 'image_id', 'optimization_goal', 'time_series',
                           'bid_strategy', 'bid_amount', 'daily_budget', 'expand_enabled',
                           'expand_targeting', 'device_price', 'app_install_status',
                           'gender', 'game_consumption_level', 'age', 'network_type',
                           'deep_conversion_type', 'deep_conversion_behavior_spec',
                           'deep_conversion_worth_spec', 'intention_targeting_tags',
                           'interest_category_id_list', 'interest_keyword_list',
                           'behavior_category_id_list',
                           'behavior_intensity', 'behavior_keyword_list', 'behavior_scene',
                           'behavior_time_window',
                           'conversion_behavior_list', 'excluded_dimension', 'location_types',
                           'regions']]
    return plan_info


# 获取近期60天优化计划的创意数据
def get_creative():
    game_id = get_game_id()
    game_id = list(map(lambda x: x['game_id'], game_id))
    game_id = [str(i) for i in game_id]
    game_id = ','.join(game_id)

    conn = pymysql.connect(host='db-slave-modeltoufang-001.ch', port=3306, user='model_read',
                           passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8', db='db_ptom')
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
        /*手动查询*/ 
       SELECT
            b.chl_user_id AS channel_id,
            b.source_id,
            b.image_id,
            a.creative_param
        FROM
            db_ptom.ptom_batch_ad_task a
            LEFT JOIN db_ptom.ptom_plan b ON a.plan_name = b.plan_name 
        WHERE
            a.media_id = 16 
            AND b.create_time >= date( NOW() - INTERVAL 1440 HOUR )    
            AND a.game_id IN ({})
            AND b.image_id is not null

    '''
    finalSql = sql.format(game_id)
    cur.execute(finalSql)
    result = pd.read_sql(finalSql, conn)
    cur.close()
    conn.close()
    return result


# 获取score_image (分数大于550的image_id)
def get_score_image():
    conn = connect(host='192.168.0.97', port=10000, auth_mechanism='PLAIN', user='hadoop',
                   password='Ycjh8FxiaoMtShZRd3-97%3hCEL0CK4ns1w', database='default')
    cursor = conn.cursor()
    sql_engine = 'set hive.execution.engine=tez'
    sql = 'select image_id,label_ids from dws.dws_image_score_d where media_id=16 and score>=520 and dt=CURRENT_DATE group by image_id,label_ids'
    cursor.execute(sql_engine)
    cursor.execute(sql)
    result = as_pandas(cursor)
    result['label_ids'] = result['label_ids'].astype(str)
    result['label_ids'] = result['label_ids'].apply(lambda x: x.strip('-1;') if '-1' in x else x)
    result['label_ids'] = pd.to_numeric(result['label_ids'], errors='coerce')

    # 关闭链接
    cursor.close()
    conn.close()

    return result['image_id'].values


# 获取近期计划的运营数据
def get_now_plan_roi():
    game_id = get_game_id()
    game_id = list(map(lambda x: x['game_id'], game_id))
    game_id = [str(i) for i in game_id]
    game_id = ','.join(game_id)

    conn = pymysql.connect(host='db-slave-modelfenxi-001.ch', port=3306, user='model_read',
                           passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8')
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
        SELECT
            a.ad_account_id,
            b.channel_id,
            b.source_id,
            b.tdate,
            b.amount,
            b.new_role_money,
            b.new_role_money / b.amount AS roi,
            b.pay_role_user_num / b.create_role_num AS pay_rate 
        FROM
            db_data_ptom.ptom_plan a 
        left join
            db_stdata.st_lauch_report b
        on a.chl_user_id=b.channel_id and a.source_id=b.source_id
        WHERE
            b.tdate >= date( NOW() - INTERVAL 168 HOUR ) 
            AND b.tdate_type = 'day' 
            AND b.media_id = 16 
            AND b.game_id IN ({}) 
            AND b.amount >= 200 
            AND b.pay_role_user_num >= 1 
            AND b.new_role_money >= 48
            AND (b.new_role_money / b.amount)>=0.015
    '''
    finalSql = sql.format(game_id)
    cur.execute(finalSql)
    result_df = pd.read_sql(finalSql, conn)
    cur.close()
    conn.close()
    result_df['tdate'] = pd.to_datetime(result_df['tdate'])
    result_df = result_df.sort_values('tdate')
    result_df = result_df.drop_duplicates(['channel_id', 'source_id'], keep='first')
    return result_df


def get_roi(x, y, z):
    conn = pymysql.connect(host='db-slave-modelfenxi-001.ch', port=3306, user='model_read',
                           passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8')
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
       SELECT
            aa.launch_op_id AS 'launch_op_id',
            aa.media_id AS 'media_id',
            sum( aa.amount ) AS 'amount',
            IFNULL( sum( aa.create_role_num ), 0 ) AS 'create_role_num',
            IFNULL( sum( bb.pay_role_user_num ), 0 ) AS 'pay_num',
            IFNULL( sum( bb.new_role_money ), 0 ) AS 'pay_sum',
            (
         CASE
                    WHEN ifnull( sum( aa.amount ), 0 )= 0 THEN
                            0 ELSE IFNULL( sum( bb.new_role_money ), 0 ) / ifnull( sum( aa.amount ), 0 )
                                                    END 
                                                    ) AS 'create_role_roi' 
        FROM
            (
            SELECT
                a.game_id,
                a.channel_id,
                a.source_id,
                b.launch_op_id,
                a.media_id,
                IFNULL( sum( a.amount ), 0 ) AS amount,
                IFNULL( sum( create_role_num ), 0 ) AS create_role_num
            FROM
                db_stdata.st_lauch_report a
                INNER JOIN db_data_ptom.ptom_plan b ON a.game_id = b.game_id 
                AND a.source_id = b.source_id 
                AND a.channel_id = b.chl_user_id
            WHERE
                a.tdate_type = 'day' 
                AND a.tdate <= DATE_SUB( date( NOW()), INTERVAL {} DAY )  AND a.tdate >= DATE_SUB( date( NOW()), INTERVAL {} DAY ) 
                AND a.media_id = 16
                AND a.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1056 AND dev_game_id IS NOT NULL ) 
                AND b.launch_op_id=13268
            GROUP BY
                a.game_id,
                a.channel_id,
                a.source_id  
            ) aa
            LEFT JOIN (
                SELECT
                    c.game_id,
                    c.channel_id,
                    c.source_id,
                    sum( c.create_role_money ) new_role_money,
                    IFNULL( sum( c.pay_role_user_num ), 0 ) AS pay_role_user_num 
                FROM
                    db_stdata.st_game_days c 
                WHERE
                    c.report_days = {} 
                    AND c.tdate = date( NOW() - INTERVAL 24 HOUR ) 
                    AND c.tdate_type = 'day' 
                    AND c.query_type = 13 
                GROUP BY
                    c.game_id,
                    c.channel_id,
                    c.source_id 
                HAVING
                ( new_role_money > 0 OR pay_role_user_num > 0 )
            ) bb ON aa.game_id = bb.game_id 
            AND aa.channel_id = bb.channel_id 
            AND aa.source_id = bb.source_id 
        GROUP BY
            aa.launch_op_id,
            aa.media_id
    '''
    finalSql = sql.format(x, y, z)
    cur.execute(finalSql)
    result_df = pd.read_sql(finalSql, conn)
    cur.close()
    conn.close()

    return result_df['create_role_roi'].values[0]


def get_roi_current():
    conn = pymysql.connect(host='db-slave-modelfenxi-001.ch', port=3306, user='model_read',
                           passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8')
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
       /*手动查询*/ 
        SELECT
            aa.launch_op_id AS 'launch_op_id',
            aa.media_id AS 'media_id',
            sum( aa.amount ) AS 'amount',
            IFNULL( sum( aa.create_role_num ), 0 ) AS 'create_role_num',
            IFNULL( sum( aa.pay_role_user_num ), 0 ) AS 'pay_role_user_num',
            IFNULL( sum( aa.new_role_money ), 0 ) AS 'pay_sum',
            (
         CASE
                    WHEN ifnull( sum( aa.pay_role_user_num ), 0 )= 0 THEN
                            IFNULL( sum( aa.amount ), 0 ) ELSE IFNULL( sum( aa.amount ), 0 ) / ifnull( sum( aa.pay_role_user_num ), 0 )
                                                    END 
                                                    ) AS 'create_role_pay_cost', 
            (
         CASE
                    WHEN ifnull( sum( aa.amount ), 0 )= 0 THEN
                            0 ELSE IFNULL( sum( aa.new_role_money ), 0 ) / ifnull( sum( aa.amount ), 0 )
                                                    END 
                                                    ) AS 'create_role_roi' 
        FROM
            (
            SELECT
                a.game_id,
                a.channel_id,
                a.source_id,
                b.launch_op_id,
                a.media_id,
                IFNULL( sum( a.amount ), 0 ) AS amount,
                IFNULL( sum( create_role_num ), 0 ) AS create_role_num,
                IFNULL( sum( a.new_role_money ), 0 ) AS new_role_money,
                IFNULL( sum( a.pay_role_user_num ), 0 ) AS pay_role_user_num
            FROM
                db_stdata.st_lauch_report a
                INNER JOIN db_data_ptom.ptom_plan b ON a.game_id = b.game_id 
                AND a.source_id = b.source_id 
                AND a.channel_id = b.chl_user_id
            WHERE
                a.tdate_type = 'day' 
                AND a.tdate = date( NOW())
                AND a.media_id = 16
                AND a.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1056 AND dev_game_id IS NOT NULL ) 
                AND b.launch_op_id=13268
            GROUP BY
                a.game_id,
                a.channel_id,
                a.source_id  
            ) aa
        GROUP BY
            aa.launch_op_id,
            aa.media_id
    '''
    cur.execute(sql)
    result_df = pd.read_sql(sql, conn)
    cur.close()
    conn.close()

    return result_df['create_role_roi'].values[0], result_df['create_role_pay_cost'].values[0]


def get_plan_num_1(n=40):
    roi_3 = get_roi(1, 3, 3)
    roi_1 = get_roi(1, 1, 1)
    if roi_1 < 0.01:
        a = -(n * 0.4)
    elif roi_1 >= 0.01 and roi_1 < 0.015:
        a = -(n * 0.2)
    elif roi_1 >= 0.015 and roi_1 < 0.02:
        a = 0
    elif roi_1 >= 0.02 and roi_1 < 0.03:
        a = n * 0.2
    else:
        a = n * 0.3

    if roi_3 < 0.03:
        b = -(n * 0.4)
    elif roi_3 >= 0.03 and roi_3 < 0.045:
        b = 0
    elif roi_3 >= 0.045 and roi_3 < 0.06:
        b = (n * 0.2)
    else:
        b = (n * 0.3)
    return int(n + a + b)


def get_plan_num_2(n=25):
    roi_current, create_role_pay_cost = get_roi_current()
    if create_role_pay_cost > 15000:
        a = 0
    elif create_role_pay_cost > 12000 and create_role_pay_cost <= 15000:
        if roi_current < 0.01:
            a = 0
        elif roi_current >= 0.01 and roi_current < 0.015:
            a = n * 0.2
        elif roi_current >= 0.015 and roi_current < 0.02:
            a = n * 0.3
        elif roi_current >= 0.02 and roi_current < 0.03:
            a = n * 0.4
        else:
            a = n * 0.5
    else:
        if roi_current < 0.01:
            a = 0
        elif roi_current >= 0.01 and roi_current < 0.015:
            a = n * 0.3
        elif roi_current >= 0.015 and roi_current < 0.02:
            a = n * 0.4
        elif roi_current >= 0.02 and roi_current < 0.03:
            a = n * 0.5
        else:
            a = n * 0.6
    return int(a)


def get_plan_num_3(n=15):
    roi_current, create_role_pay_cost = get_roi_current()
    if create_role_pay_cost > 15000:
        a = 0
    elif create_role_pay_cost > 12000 and create_role_pay_cost <= 15000:
        if roi_current < 0.015:
            a = 0
        elif roi_current >= 0.015 and roi_current < 0.02:
            a = n * 0.2
        elif roi_current >= 0.02and roi_current < 0.03:
            a = n * 0.3
        elif roi_current >= 0.03 and roi_current < 0.04:
            a = n * 0.4
        else:
            a = n * 0.5
    else:
        if roi_current < 0.015:
            a = 0
        elif roi_current >= 0.015 and roi_current < 0.02:
            a = n * 0.3
        elif roi_current >= 0.02 and roi_current < 0.03:
            a = n * 0.4
        elif roi_current >= 0.03 and roi_current < 0.04:
            a = n * 0.5
        else:
            a = n * 0.6
    return int(a)


def get_set_info():
    conn = pymysql.connect(host='db-slave-modelfenxi-001.ch', port=3306, user='model_read',
                           passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8')
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
       /*手动查询*/ 
            SELECT
            aa.advert_posi_id AS 'advert_posi_id',
            aa.media_id AS 'media_id',
            sum( aa.amount ) AS 'amount',
            IFNULL( sum( aa.create_role_num ), 0 ) AS 'create_role_num',
            IFNULL( sum( bb.pay_role_user_num ), 0 ) AS 'pay_num',
            IFNULL( sum( bb.new_role_money ), 0 ) AS 'pay_sum',
            (
            CASE

                    WHEN ifnull( sum( aa.amount ), 0 )= 0 THEN
                    0 ELSE IFNULL( sum( bb.new_role_money ), 0 ) / ifnull( sum( aa.amount ), 0 ) 
                END 
                ) AS 'create_role_roi' 
            FROM
                (
                SELECT
                    a.game_id,
                    a.channel_id,
                    a.source_id,
                    b.advert_posi_id,
                    a.media_id,
                    IFNULL( sum( a.amount ), 0 ) AS amount,
                    IFNULL( sum( create_role_num ), 0 ) AS create_role_num 
                FROM
                    db_stdata.st_lauch_report a
                    INNER JOIN db_data_ptom.ptom_plan b ON a.game_id = b.game_id 
                    AND a.source_id = b.source_id 
                    AND a.channel_id = b.chl_user_id 
                WHERE
                    a.tdate_type = 'day' 
                    AND a.tdate <= DATE_SUB( date( NOW()), INTERVAL 1 DAY ) AND a.tdate >= DATE_SUB( date( NOW()), INTERVAL 15 DAY ) 
                    AND a.media_id = 16 
                    AND a.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1056 AND dev_game_id IS NOT NULL ) 
                GROUP BY
                    a.game_id,
                    a.channel_id,
                    a.source_id 
                ) aa
                LEFT JOIN (
                SELECT
                    c.game_id,
                    c.channel_id,
                    c.source_id,
                    sum( c.create_role_money ) new_role_money,
                    IFNULL( sum( c.pay_role_user_num ), 0 ) AS pay_role_user_num 
                FROM
                    db_stdata.st_game_days c 
                WHERE
                    c.report_days = 15 
                    AND c.tdate = date( NOW() - INTERVAL 24 HOUR ) 
                    AND c.tdate_type = 'day' 
                    AND c.query_type = 13 
                GROUP BY
                    c.game_id,
                    c.channel_id,
                    c.source_id 
                HAVING
                    ( new_role_money > 0 OR pay_role_user_num > 0 ) 
                ) bb ON aa.game_id = bb.game_id 
                AND aa.channel_id = bb.channel_id 
                AND aa.source_id = bb.source_id 
            GROUP BY
            aa.advert_posi_id
            HAVING
            sum( aa.amount )>50000
            ORDER BY create_role_roi DESC
    '''
    cur.execute(sql)
    result_df = pd.read_sql(sql, conn)
    cur.close()
    conn.close()

    return result_df


def create_plan(df, score_image):
    # 选ad_account_id、image_id每个账号+素材8条
    game_id = 1001379
    # df = df[df['game_id'] == game_id]
    df = df[df['game_id'].isin([1001379, 1001757])]
    # ad_account_id_group = np.array([7981, 7984, 8035, 8036, 8038, 8039, 8079, 8077, 8074, 8073])
    # ad_account_id_group = np.array([7982, 8037, 8082, 8080, 8078, 8076, 8075, 7981, 7984, 8035, 8036,
    #                                 8038, 8039, 8077, 8073])
    # ad_account_id_group = np.array([
    #     7982, 8037, 8082, 8080, 8078, 8076, 8075, 7981, 7984, 8035, 8036, 8038, 8039, 8077, 8073,
    #     8814, 8815, 8816, 8817, 8818, 8819, 8820, 8821, 8822, 8823, 8824, 8825, 8826, 8827, 8828,
    #     8829, 8830, 8831, 8832, 8833, 8834, 8835, 8836, 8837, 8838, 8839, 8840, 8841, 8842, 8843,
    #     8844, 8845, 8846, 8847, 8848, 8854, 8855, 8856, 8857, 8858, 8859, 8860, 8743, 8742, 8741,
    #     8740, 8739, 8738, 8737, 8736, 8735, 8734, 8733, 8732, 8731, 8730, 8729])
    ad_account_id_group = np.array([
        7982, 8037, 8082, 8080, 8076, 8075, 7981, 7984, 8035, 8036, 8038, 8039, 8077, 8073,
        8815, 8816, 8817, 8819, 8820, 8821, 8822, 8823, 8827,
        8829, 8830, 8831, 8832, 8833, 8835, 8837, 8838, 8839, 8840, 8841, 8842, 8843,
        8844, 8845, 8847, 8854, 8855, 8856, 8857, 8858, 8743, 8742, 8741,
        8737, 8736, 8734, 8733, 8731, 8729])

    image_id_group = np.intersect1d(df['image_id'].unique(), score_image)
    image_id_group = list(filter(lambda x: x >= 32861, image_id_group))

    # print(image_id_group)
    df = df[df['image_id'].isin(image_id_group)]
    #     df = df[df['site_set'].notna()]

    plan = pd.DataFrame()
    for ad_account in ad_account_id_group:
        for image in image_id_group:
            #         print(image)
            temp = pd.DataFrame({'ad_account_id': [ad_account], 'image_id': [image]})
            plan = plan.append(temp)
    #         print(temp)
    plan = pd.DataFrame(np.repeat(plan.values, 10, axis=0), columns=plan.columns)

    plan['game_id'] = game_id

    # 选择预算，不限制预算
    plan['budget_mode'] = 0
    plan['daily_budget'] = 0

    # 选扩量方式expand_enabled、expand_targeting
    sample_df = df[['expand_enabled', 'expand_targeting']]
    sample_df = sample_df.sample(n=plan.shape[0], replace=True).reset_index(drop=True)
    plan = pd.concat([plan, sample_df], axis=1)

    # 选定向targeting
    sample_df = df[['device_price', 'app_install_status', 'gender', 'game_consumption_level', 'age', 'network_type',
                    'conversion_behavior_list', 'excluded_dimension', 'location_types', 'regions',
                    'intention_targeting_tags', 'interest_category_id_list', 'interest_keyword_list',
                    'behavior_category_id_list', 'behavior_intensity',
                    'behavior_keyword_list', 'behavior_scene', 'behavior_time_window']]
    sample_df = sample_df.sample(n=plan.shape[0], replace=True).reset_index(drop=True)
    plan = pd.concat([plan, sample_df], axis=1)

    # 选创意\出价方式、出价
    create_df = df[
        ['image_id', 'site_set', 'deep_link_url', 'adcreative_template_id', 'page_spec', 'page_type', 'link_page_spec',
         'link_name_type', 'link_page_type', 'promoted_object_id', 'profile_id', 'promoted_object_type',
         'automatic_site_enabled', 'label', 'adcreative_elements',
         'optimization_goal', 'bid_strategy', 'bid_amount', 'deep_conversion_type', 'deep_conversion_behavior_spec',
         'deep_conversion_worth_spec']]
    plan_ = pd.DataFrame()
    for image_id in image_id_group:
        plan_1 = plan[plan['image_id'] == image_id]
        create_df_1 = create_df[create_df['image_id'] == image_id]
        create_df_1['site_set'] = create_df_1['site_set'].map(str)
        create_df_1['weight'] = create_df_1.groupby(['site_set'])['image_id'].transform('count')

        sample_df_1 = create_df_1.sample(n=plan_1.shape[0], replace=True, weights=create_df_1['weight']).reset_index(
            drop=True)
        sample_df_1.drop(['image_id', 'weight'], axis=1, inplace=True)

        plan_1 = plan_1.reset_index(drop=True)
        sample_df_1 = sample_df_1.reset_index(drop=True)
        plan_1 = pd.concat([plan_1, sample_df_1], axis=1)
        plan_ = plan_.append(plan_1)

    plan = plan_
    plan['site_set'] = plan['site_set'].apply(ast.literal_eval)

    # 选time_series
    cols = ['time_series']
    for col in cols:
        count_df = pd.DataFrame(data=df[col].value_counts()).reset_index()
        count_df.columns = ['col', 'counts']
        count_df['pct'] = count_df['counts'] / count_df['counts'].sum()
        plan[col] = plan.apply(lambda x: np.random.choice(count_df['col'].values, 1, p=count_df['pct'].values)[0],
                               axis=1)

    plan['promoted_object_id'] = '1111059412'
    plan['create_time'] = pd.to_datetime(pd.datetime.now())
    plan['create_date'] = pd.to_datetime(pd.datetime.now().date())
    plan = plan.reset_index(drop=True)
    return plan


# 对列表内容进行编码
def get_mutil_feature(data):
    cols = ['behavior_category_id_list', 'behavior_intensity', 'behavior_keyword_list',
            'intention_targeting_tags', 'interest_category_id_list', 'interest_keyword_list']
    for col in cols:
        data[col] = data[col].apply(lambda x: x if x == x else [])
        data = data.join(data[col].str.join('|').str.get_dummies().add_prefix(col + '_').reset_index(drop=True))
        data.drop(col, axis=1, inplace=True)
    return data


def get_train_df():
    plan_info = get_plan_info()
    plan_info = get_plan_json(plan_info)
    creative_info = get_creative()
    image_info = get_image_info()
    launch_report = get_launch_report()
    creative_info['creative_param'] = creative_info['creative_param'].apply(json.loads)
    temp = creative_info['creative_param'].apply(pd.Series)
    if 'image_id' in temp.columns:
        temp = temp.drop('image_id', axis=1)
    creative_info = pd.concat([creative_info, temp], axis=1)
    creative_info.drop('creative_param', axis=1, inplace=True)
    creative_info.drop(['title', 'adcreative_template_parent', 'idea_type', 'adcreative_name', 'ideaName', '_creative'],
                       axis=1, inplace=True)
    creative_info['adcreative_elements_array'] = creative_info['adcreative_elements_array'].apply(lambda x: x[0])
    creative_info['adcreative_elements'] = creative_info['adcreative_elements_array'].apply(
        lambda x: x['adcreative_elements'])
    creative_info.drop('adcreative_elements_array', axis=1, inplace=True)
    creative_info = creative_info[['channel_id', 'source_id', 'image_id', 'deep_link_url',
                                   'adcreative_template_id', 'page_spec', 'page_type', 'site_set', 'label',
                                   'promoted_object_id', 'promoted_object_type', 'automatic_site_enabled',
                                   'link_name_type', 'link_page_type', 'profile_id', 'link_page_spec',
                                   'adcreative_elements']]
    plan_info = pd.merge(plan_info.drop(['image_id'], axis=1), creative_info, on=['channel_id', 'source_id'],
                         how='inner')
    plan_info.dropna(subset=['image_id'], inplace=True)
    plan_info['image_id'] = plan_info['image_id'].astype(int)
    plan_info['adcreative_template_id'] = plan_info['adcreative_template_id'].astype(int)
    plan_info['profile_id'] = plan_info['profile_id'].apply(lambda x: x if x != x else str(int(x)))
    plan_info['create_time'] = pd.to_datetime(plan_info['create_time'])
    plan_info_current = plan_info[plan_info['create_time'] >= pd.datetime.now() - pd.DateOffset(30)]
    now_plan_roi = get_now_plan_roi()
    score_image = get_score_image()
    image_info = image_info[image_info['image_id'].notna()]
    image_info['image_id'] = image_info['image_id'].astype(int)
    df_create = pd.merge(plan_info_current, image_info, on=['channel_id', 'source_id', 'image_id'], how='left')
    df_create = pd.merge(df_create, now_plan_roi.drop(['ad_account_id'], axis=1), on=['channel_id', 'source_id'],
                         how='inner')
    df_create = df_create[df_create['site_set'].notna()]

    plan_create = create_plan(df_create, score_image)
    image_info.dropna(subset=['image_id'], inplace=True)
    image_info['image_id'] = image_info['image_id'].astype(int)
    plan_create = pd.merge(plan_create, image_info[['image_id', 'label_ids']].drop_duplicates(), on='image_id',
                           how='left')

    df = pd.merge(plan_info, image_info, on=['channel_id', 'source_id', 'image_id'], how='left')
    df.dropna(subset=['image_id'], inplace=True)
    df = pd.merge(df, launch_report, on=['channel_id', 'source_id'], how='left')
    df.drop(df[df['tdate'].isna()].index, inplace=True)
    df = df[df['amount'] >= 500]

    df['plan_label'] = df.apply(lambda x: 1 if x.new_role_money / x.amount >= 0.015 else 0, axis=1)
    df['ad_account_id'] = df['ad_account_id'].astype('int')
    df['image_id'] = df['image_id'].astype('int')
    df.rename(columns={'tdate': 'create_date'}, inplace=True)
    df['create_date'] = pd.to_datetime(df['create_date'])
    df['create_time'] = pd.to_datetime(df['create_time'])

    df.drop(['channel_id', 'source_id', 'budget_mode', 'bid_amount', 'daily_budget', 'deep_conversion_behavior_spec',
             'deep_conversion_worth_spec',
             'deep_link_url', 'page_spec', 'promoted_object_id', 'promoted_object_type', 'automatic_site_enabled',
             'link_page_type',
             'profile_id', 'link_page_spec', 'adcreative_elements', 'amount', 'roi', 'pay_rate',
             'new_role_money'], axis=1, inplace=True)

    plan_create_train = plan_create.drop(['adcreative_elements', 'automatic_site_enabled', 'bid_amount',
                                          'budget_mode', 'daily_budget', 'deep_conversion_behavior_spec',
                                          'deep_conversion_worth_spec', 'deep_link_url', 'link_page_spec',
                                          'link_page_type', 'page_spec', 'profile_id', 'promoted_object_id',
                                          'promoted_object_type'], axis=1)
    df['train_label'] = 1
    plan_create_train['train_label'] = 0
    plan_create_train['plan_label'] = -1

    df = df.append(plan_create_train)

    df['create_date'] = pd.to_datetime(df['create_date'])
    df['ad_im_sort_id'] = df.groupby(['ad_account_id', 'image_id'])['create_time'].rank()
    df['ad_game_sort_id'] = df.groupby(['ad_account_id', 'game_id'])['create_time'].rank()
    df['im_ad_sort_id'] = df.groupby(['image_id', 'ad_account_id'])['create_time'].rank()

    df = get_mutil_feature(df)

    cat_cols = ['ad_account_id', 'game_id', 'optimization_goal', 'time_series', 'bid_strategy', 'expand_enabled',
                'expand_targeting',
                'device_price', 'app_install_status', 'gender', 'game_consumption_level', 'age', 'network_type',
                'deep_conversion_type',
                'behavior_scene', 'behavior_time_window', 'conversion_behavior_list', 'excluded_dimension',
                'location_types', 'regions',
                'image_id', 'adcreative_template_id', 'page_type', 'site_set', 'label', 'link_name_type', 'label_ids']

    cat_cross = []
    for col in tqdm_notebook(combinations(cat_cols, 2)):
        df[str(col[0]) + '_' + str(col[1])] = df[col[0]].map(str) + '_' + df[col[1]].map(str)
        cat_cross.append(str(col[0]) + '_' + str(col[1]))

    for col in cat_cols + cat_cross:
        df[col] = df[col].astype(str)
        le = LabelEncoder()
        df[col] = le.fit_transform(df[col])

    return df, plan_create


def get_ad_create(plan_result):
    ad_info = []
    for i in range(plan_result.shape[0]):
        ad_info.append(json.loads(plan_result.iloc[i].to_json()))
    open_api_url_prefix = "https://ptom.caohua.com/"
    # open_api_url_prefix = "http://192.168.0.60:8085/"
    uri = "model/generationPlanBatchTask"
    url = open_api_url_prefix + uri
    params = {
        "secretkey": "abc2018!@**@888",
        "mediaId": 16
    }
    rsp = requests.post(url, json=ad_info, params=params)
    rsp_data = rsp.json()
    print('结束....')
    return rsp_data


def main_model():
    df, plan_create = get_train_df()
    train_data = df[df['train_label'] == 1]
    test_data = df[df['train_label'] == 0]
    train_data.drop(['train_label', 'create_time', 'create_date'], axis=1, inplace=True)
    test_data.drop(['train_label', 'create_time', 'create_date'], axis=1, inplace=True)

    target = train_data['plan_label']
    features = train_data.drop(['plan_label'], axis=1)
    features.columns = ['c' + str(i) for i in range(features.shape[1])]
    X_val, x_test, Y_val, y_test = train_test_split(features, target, test_size=0.3)

    params = {
        "objective": "binary",
        "boosting_type": "gbdt",
        "learning_rate": 0.01,
        "max_depth": 8,
        "num_leaves": 55,
        "max_bin": 255,
        "min_data_in_leaf": 101,
        "min_child_samples": 15,
        "feature_fraction": 0.5,
        "bagging_fraction": 0.6,
        "bagging_freq": 20,
        "lambda_l1": 1e-05,
        "lambda_l2": 0,
        "min_split_gain": 0.0,
        "metric": "auc",
        'is_unbalance': True
    }

    train_data = lgb.Dataset(X_val, label=Y_val)
    val_data = lgb.Dataset(x_test, label=y_test, reference=train_data)
    model = lgb.train(params, train_data, num_boost_round=8000, early_stopping_rounds=100,
                      valid_sets=[train_data, val_data])

    features_test = test_data.drop(['label'], axis=1)
    y_predict = model.predict(features_test)

    plan_create['prob'] = y_predict
    threshold = pd.Series(y_predict).sort_values(ascending=False).reset_index(drop=True)[int(y_predict.shape[0] * 0.3)]

    plan_result = plan_create[plan_create['prob'] >= threshold]
    plan_result['rank_ad_im'] = plan_result.groupby(['ad_account_id', 'image_id'])['prob'].rank(ascending=False,
                                                                                                method='first')
    plan_result = plan_result[plan_result['rank_ad_im'] <= 1]

    plan_create['rank_ad_im'] = plan_create.groupby(['ad_account_id', 'image_id'])['prob'].rank(ascending=False,
                                                                                                method='first')
    plan_result_pr = plan_create[plan_create['rank_ad_im'] <= 1]

    ad_num = plan_result['ad_account_id'].value_counts()
    for ad in np.setdiff1d(plan_create['ad_account_id'].values, ad_num[ad_num > 2].index):
        add_plan = plan_result_pr[plan_result_pr['ad_account_id'] == ad].sort_values('prob', ascending=False)[0:2]
        plan_result = plan_result.append(add_plan)

    ad_num = plan_result['image_id'].value_counts()
    for ad in np.setdiff1d(plan_create['image_id'].values, ad_num[ad_num > 2].index):
        add_plan = plan_result_pr[plan_result_pr['image_id'] == ad].sort_values('prob', ascending=False)[0:3]
        plan_result = plan_result.append(add_plan)

    plan_result['weight'] = plan_result.groupby(['ad_account_id'])['game_id'].transform('count')
    plan_result['site_set'] = plan_result['site_set'].map(str)
    # # 版位过滤
    # set_info = get_set_info()
    # set_dict = {
    #     '1263,1264,1265,1266': "['SITE_SET_QQ_MUSIC_GAME', 'SITE_SET_KANDIAN', 'SITE_SET_TENCENT_NEWS', 'SITE_SET_TENCENT_VIDEO']",
    #     '1268': "['SITE_SET_WECHAT']",
    #     '1269': "['SITE_SET_MOMENTS']",
    #     '1263,1265,1266,1267': "['SITE_SET_QQ_MUSIC_GAME', 'SITE_SET_KANDIAN', 'SITE_SET_TENCENT_VIDEO', 'SITE_SET_MOBILE_UNION']",
    #     '1263,1264,1265,1266,1267': "['SITE_SET_QQ_MUSIC_GAME', 'SITE_SET_KANDIAN', 'SITE_SET_TENCENT_NEWS', 'SITE_SET_TENCENT_VIDEO', 'SITE_SET_MOBILE_UNION']",
    #     '1263,1265,1266': "['SITE_SET_QQ_MUSIC_GAME', 'SITE_SET_KANDIAN', 'SITE_SET_TENCENT_VIDEO']",
    #     '1262': "['AUTOMATIC_SITE_ENABLED']",
    #     '1265,1266,1267': "['SITE_SET_QQ_MUSIC_GAME', 'SITE_SET_KANDIAN', 'SITE_SET_MOBILE_UNION']"
    # }
    # set_info['advert_posi_id'] = set_info['advert_posi_id'].replace(set_dict)
    # roi_avg = set_info['pay_sum'].sum() / set_info['amount'].sum()
    # # 选择回款率高于平均数0.9倍的版位
    # set_info = set_info[set_info['create_role_roi'] >= roi_avg * 0.9][['advert_posi_id', 'create_role_roi']]
    # set_info = set_info.rename(columns={'advert_posi_id': 'site_set'})
    #
    # plan_result['site_set'] = plan_result['site_set'].map(str)
    # plan_result = pd.merge(plan_result, set_info, on='site_set', how='left')
    # plan_result = plan_result[plan_result['create_role_roi'].notna()]
    # print(plan_result['site_set'].value_counts())
    # 建计划数量plan_num
    d_time_1 = datetime.datetime.strptime(str(datetime.datetime.now().date()) + '07:00', '%Y-%m-%d%H:%M')
    d_time_2 = datetime.datetime.strptime(str(datetime.datetime.now().date()) + '13:00', '%Y-%m-%d%H:%M')
    d_time_3 = datetime.datetime.strptime(str(datetime.datetime.now().date()) + '17:00', '%Y-%m-%d%H:%M')
    n_time = datetime.datetime.now()
    if n_time > d_time_1 and n_time < d_time_2:
        plan_num = get_plan_num_1()
    elif n_time > d_time_2 and n_time < d_time_3:
        plan_num = get_plan_num_2()
    elif n_time > d_time_3:
        plan_num = get_plan_num_3()
    # print(plan_num)
    plan_num = 0
    # 朋友圈200,非朋友圈20
    plan_result_1 = plan_result[plan_result['site_set'] == "['SITE_SET_MOMENTS']"]
    if plan_result_1.shape[0] > 150:
        plan_result_1 = plan_result_1.sample(150, weights=plan_result_1['weight'])
    plan_result_2 = plan_result[plan_result['site_set'] != "['SITE_SET_MOMENTS']"]
    if plan_result_1.shape[0] != 0:
        if plan_result_2.shape[0] > plan_num:
            plan_result_2 = plan_result_2.sample(plan_num, weights=plan_result_2['weight'])
    else:
        if plan_result_2.shape[0] > plan_num * 3:
            plan_result_2 = plan_result_2.sample(plan_num * 3, weights=plan_result_2['weight'])

    plan_result = plan_result_1.append(plan_result_2)

    # if plan_result.shape[0] > plan_num:
    #     plan_result = plan_result.sample(plan_num, weights=plan_result['weight'])

    if n_time < d_time_2:
        ad_num = plan_result['image_id'].value_counts()
        for ad in np.setdiff1d(plan_create['image_id'].values, ad_num[ad_num >= 2].index):
            add_plan = plan_result_pr[plan_result_pr['image_id'] == ad].sort_values('prob', ascending=False)[0:2]
            add_plan['site_set'] = add_plan['site_set'].map(str)
            plan_result = plan_result.append(add_plan)

    plan_result['rank_ad_im'] = plan_result.groupby(['ad_account_id', 'image_id'])['prob'].rank(ascending=False,
                                                                                                method='first')
    plan_result = plan_result[plan_result['rank_ad_im'] <= 1]

    plan_result = plan_result.drop(['create_time', 'create_date', 'prob', 'rank_ad_im', 'label_ids', 'weight'],
                                   axis=1)
    plan_result = plan_result[plan_result['site_set'] == "['SITE_SET_MOMENTS']"]  ## TODO 只跑朋友圈
    # [SITE_SET_WECHAT] 公众号和小程序adcreative_template_id只跑1480、560、720、721、1064五种
    plan_result = plan_result[~((plan_result['site_set'] == "['SITE_SET_WECHAT']") & (
        ~plan_result['adcreative_template_id'].isin([1480, 560, 720, 721, 1064])))]

    plan_result['location_types'] = plan_result['location_types'].apply(lambda x: ['LIVE_IN'] if x == x else x)

    # 修改落地页ID
    plan_result['page_spec'] = plan_result.apply(
        lambda x: {'override_canvas_head_option': 'OPTION_CREATIVE_OVERRIDE_CANVAS',
                   'page_id': '2230311524'} if x.site_set == "['SITE_SET_MOMENTS']"
        else ({'override_canvas_head_option': 'OPTION_CREATIVE_OVERRIDE_CANVAS',
               'page_id': '2179536949'} if x.site_set == "['SITE_SET_WECHAT']" else np.nan), axis=1)
    plan_result['link_page_spec'] = plan_result.apply(
        lambda x: {'page_id': '2230311524'} if x.site_set == "['SITE_SET_MOMENTS']"
        else ({'page_id': '2179536949'} if x.site_set == "['SITE_SET_WECHAT']" else np.nan), axis=1)

    # 朋友圈头像ID
    plan_result['ad_account_id'] = plan_result['ad_account_id'].map(str)
    plan_result_1 = plan_result[plan_result['site_set'] == "['SITE_SET_MOMENTS']"]
    plan_result_2 = plan_result[plan_result['site_set'] != "['SITE_SET_MOMENTS']"]
    profile_id_dict = {'7981': '372606', '7982': '372597', '7983': '372591', '7984': '372585', '7985': '372485',
                       '8035': '383952', '8036': '383967', '8037': '383976', '8038': '383987', '8039': '383994',
                       '8082': '408038', '8081': '408049', '8080': '408052', '8079': '408056', '8078': '408059',
                       '8077': '408062', '8076': '408066', '8075': '408069', '8074': '408073', '8073': '408082',
                       '8814': '508418', '8815': '508430', '8816': '508441', '8817': '508450', '8818': '508455',
                       '8819': '508460', '8820': '508465', '8821': '508478', '8822': '508484', '8823': '508490',
                       '8824': '508498', '8825': '508500', '8826': '508506', '8827': '508513', '8828': '508525',
                       '8829': '508528', '8830': '508531', '8831': '508535', '8832': '508540', '8833': '508550',
                       '8834': '508557', '8835': '508565', '8836': '508572', '8837': '508577', '8838': '508580',
                       '8839': '508584', '8840': '508591', '8841': '508596', '8842': '508602', '8843': '508603',
                       '8844': '508608', '8845': '508611', '8846': '508620', '8847': '508634', '8848': '508646',
                       '8854': '519213', '8855': '519237', '8856': '519243', '8857': '519248', '8858': '519259',
                       '8859': '519267', '8860': '519273', '8743': '519293', '8742': '519300', '8741': '519305',
                       '8740': '519312', '8739': '519323', '8738': '519335', '8737': '519346', '8736': '519356',
                       '8735': '519363', '8734': '519369', '8733': '519377', '8732': '519385', '8731': '519393',
                       '8730': '519399', '8729': '519407', '8728': '519409', '8727': '519414', '8726': '519423',
                       '8725': '519429', '8724': '519438'}
    plan_result_1['profile_id'] = plan_result_1['ad_account_id'].map(profile_id_dict)

    plan_result = plan_result_1.append(plan_result_2)
    plan_result = plan_result.reset_index(drop=True)

    # 年龄定向
    plan_result['age'] = plan_result['age'].apply(lambda x: [{'min': 20, 'max': 50}])

    # 定义组合json
    plan_result['intention'] = plan_result['intention_targeting_tags'].apply(lambda x: {'targeting_tags': x})
    plan_result.drop(['intention_targeting_tags'], axis=1, inplace=True)

    ad_info = []
    for i in range(plan_result.shape[0]):
        ad_info.append(
            json.loads(plan_result.loc[i, ['interest_category_id_list', 'interest_keyword_list']].rename(index={
                'interest_category_id_list': 'category_id_list',
                'interest_keyword_list': 'keyword_list'}).to_json()))
    plan_result['interest'] = ad_info
    plan_result.drop(['interest_category_id_list', 'interest_keyword_list'], axis=1, inplace=True)

    ad_info = []
    for i in range(plan_result.shape[0]):
        ad_info.append(json.loads(plan_result.loc[i, 'behavior_category_id_list':'behavior_time_window'].rename(index={
            'behavior_category_id_list': 'category_id_list',
            'behavior_intensity': 'intensity',
            'behavior_keyword_list': 'keyword_list',
            'behavior_scene': 'scene',
            'behavior_time_window': 'time_window'}).to_json()))
    plan_result['behavior'] = ad_info
    plan_result.drop(['behavior_category_id_list', 'behavior_intensity', 'behavior_keyword_list', 'behavior_scene',
                      'behavior_time_window'], axis=1, inplace=True)

    plan_result['behavior'] = plan_result['behavior'].apply(lambda x: [x])

    ad_info = []
    for i in range(plan_result.shape[0]):
        ad_info.append(json.loads(plan_result.loc[i, ['conversion_behavior_list', 'excluded_dimension']].to_json()))
    plan_result['excluded_converted_audience'] = ad_info
    plan_result.drop(['conversion_behavior_list', 'excluded_dimension'], axis=1, inplace=True)

    ad_info = []
    for i in range(plan_result.shape[0]):
        ad_info.append(json.loads(plan_result.loc[i, ['regions', 'location_types']].to_json()))
    plan_result['geo_location'] = ad_info
    plan_result.drop(['regions', 'location_types'], axis=1, inplace=True)

    ad_info = []
    for i in range(plan_result.shape[0]):
        ad_info.append(json.loads(plan_result.loc[
                                      i, ["device_price", "app_install_status", "gender", "game_consumption_level",
                                          "age", "network_type",
                                          "excluded_converted_audience", "geo_location",
                                          "intention", "interest", "behavior"]].to_json()))
    plan_result['targeting'] = ad_info

    plan_result.drop(["device_price", "app_install_status", "gender", "game_consumption_level", "age", "network_type",
                      "excluded_converted_audience", "geo_location",
                      "intention", "interest", "behavior"], axis=1, inplace=True)

    # plan_result['operation'] = 'disable'
    plan_result['op_id'] = 13268
    plan_result['flag'] = 'GDT'
    plan_result['game_name'] = '幸存者挑战'
    plan_result['platform'] = 1
    plan_result['ad_account_id'] = plan_result['ad_account_id'].astype(int)
    plan_result['site_set'] = plan_result['site_set'].apply(ast.literal_eval)
    # 周三周四更新，凌晨不跑计划
    plan_result['time_series'] = plan_result['time_series'].apply(
        lambda x: x[0:96] + '1111111111000000000011' + x[118:144] + '1111111111000000000011' + x[166:])
    plan_result.drop('page_type', axis=1, inplace=True)
    plan_result.to_csv('./plan_result.csv', index=0)  # 保存创建日志

    plan_result_seg = pd.DataFrame()
    for i in range(plan_result.shape[0]):
        plan_result_seg = plan_result_seg.append(plan_result.iloc[i:i + 1])
        if (i > 0 and i % 40 == 0) or i == (plan_result.shape[0] - 1):
            plan_result_seg = plan_result_seg.reset_index(drop=True)
            print(plan_result_seg.shape[0])
            rsp_data = get_ad_create(plan_result_seg)
            print(rsp_data)
            time.sleep(200)
            plan_result_seg = pd.DataFrame()


if __name__ == '__main__':
    main_model()
