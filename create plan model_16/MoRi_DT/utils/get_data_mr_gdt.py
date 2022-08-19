# -*- coding:utf-8 -*-
"""
   File Name：     get_data.py
   Description :   数据准备：动态计划的数据提取（广点通末日）
   Author :        royce.mao
   date：          2021/10/11 17:00
"""

import pandas as pd
import numpy as np
import json
import requests
import traceback
import datetime
# os.chdir(os.path.dirname(__file__))
import pymysql
from impala.dbapi import connect
from impala.util import as_pandas
from itertools import product

import warnings
warnings.filterwarnings('ignore')


def get_baseimage_score(base_image):
    """ 数据：提取指定素材的最近1次评分 """
    conn = connect(host='192.168.0.97', port=10000, auth_mechanism='PLAIN', user='hadoop',
                   password='Ycjh8FxiaoMtShZRd3-97%3hCEL0CK4ns1w', database='default')
    cursor = conn.cursor()
    sql_engine = 'set hive.execution.engine=tez'
    sql = 'select image_id,score,dt from dws.dws_image_score_d where media_id=16 and dt>=date_sub(current_date, 60)'
    cursor.execute(sql_engine)
    cursor.execute(sql)
    result = as_pandas(cursor)
    result = result[result['image_id'].isin(base_image)]
    result = result.sort_values('dt')
    result = result.drop_duplicates(['image_id'], keep='last')  ## TODO:保留最近的1次
    # 关闭链接
    cursor.close()
    conn.close()

    return result[['image_id','score']]

def get_baseimage_amount(base_image):
    """ 数据：提取指定素材的素材累计消耗 """
    conn = connect(host='192.168.0.97', port=10000, auth_mechanism='PLAIN', user='hadoop',
                   password='Ycjh8FxiaoMtShZRd3-97%3hCEL0CK4ns1w', database='default')
    cursor = conn.cursor()
    sql_engine = 'set hive.execution.engine=tez'
    sql = 'select image_id,image_run_date_amount,dt from dws.dws_image_score_d where media_id=16 and dt>=date_sub(current_date, 30)'
    cursor.execute(sql_engine)
    cursor.execute(sql)
    result = as_pandas(cursor)
    result = result[result['image_id'].isin(base_image)]
    result = result.sort_values('dt')
    result = result.drop_duplicates(['image_id'], keep='last')  ## TODO:保留最近的1次
    # 关闭链接
    cursor.close()
    conn.close()

    return result[['image_id','image_run_date_amount']]

def get_plan_num_peraccount(ad_account_ids):
    """ 数据：每个账号已建的计划数量 """
    
    conn = pymysql.connect(host='db-slave-modeltoufang-001.ch', port=3306, user='model_read',
                           passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8', db='db_ptom')
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
    /*手动查询*/
        SELECT
            a.chl_user_id AS channel_id,
            a.source_id AS source_id,
            a.ad_account_id,
            b.tdate
        FROM
            db_data_ptom.ptom_plan a
            LEFT JOIN db_stdata.st_lauch_report b ON a.chl_user_id = b.channel_id 
            AND a.source_id = b.source_id  
        WHERE
            a.ad_account_id IN ({})
            AND a.media_id = 16
            AND a.create_time>=date( NOW() - INTERVAL 720 HOUR )
'''
    ad_account_ids = [str(i) for i in ad_account_ids]
    finalSql = sql.format(','.join(ad_account_ids))
    result = pd.read_sql(finalSql, conn)
    result = result.sort_values('tdate')
    result = result.drop_duplicates(['channel_id', 'source_id'], keep='last')  ## TODO:保留最近的1次
    result['plan_num'] = result.groupby('ad_account_id').transform('count')
    result = result.drop_duplicates(['ad_account_id', 'plan_num'], keep='last')
    # 关闭链接
    cur.close()
    conn.close()
    return result[['ad_account_id','plan_num']]

def get_score_image():
    """ 数据：近15天的历史有过600评分且当前评分550以上 """

    conn = connect(host='192.168.0.97', port=10000, auth_mechanism='PLAIN', user='hadoop',
                   password='Ycjh8FxiaoMtShZRd3-97%3hCEL0CK4ns1w', database='default')
    cursor = conn.cursor()
    sql_engine = 'set hive.execution.engine=tez'
    sql = 'select image_id,dt from dws.dws_image_score_d where media_id=16 and score>=600 and dt>=date_sub(current_date, 30)'
    cursor.execute(sql_engine)
    cursor.execute(sql)
    result_1 = as_pandas(cursor)
    sql = 'select image_id,dt from dws.dws_image_score_d where media_id=16 and score>=550 and dt=current_date'
    cursor.execute(sql_engine)
    cursor.execute(sql)
    result_2 = as_pandas(cursor)
    result = np.intersect1d(result_1['image_id'], result_2['image_id'])
    # 关闭链接
    cursor.close()
    conn.close()

    return result

def get_good_image():
    """ 数据：近15天的付费计划素材 """

    conn = pymysql.connect(host='db-slave-modelfenxi-001.ch', port=3306, user='model_read',
                           passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8')
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
        SELECT
            b.channel_id,
            b.source_id,
            b.tdate,
            b.amount 
        FROM
            db_stdata.st_lauch_report b
        WHERE
            b.tdate >= date( NOW() - INTERVAL 720 HOUR ) 
            AND b.tdate_type = 'day' 
            AND b.media_id = 16 
            AND b.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1056 AND dev_game_id IS NOT NULL ) 
            AND b.amount >= 500
            AND b.pay_role_user_num >= 1
            AND b.new_role_money >= 6
    '''
    cur.execute(sql)
    result_df = pd.read_sql(sql, conn)

    sql = '''
        SELECT
            a.chl_user_id AS channel_id,
            a.source_id,
            a.image_id
        FROM
            db_data_ptom.ptom_plan a
        WHERE
            a.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1056 AND dev_game_id IS NOT NULL ) 
            AND a.media_id = 16 
            AND a.create_time >= date( NOW() - INTERVAL 720 HOUR )
        GROUP BY
            a.chl_user_id,
            a.source_id,
            a.image_id
    '''
    cur.execute(sql)
    result = pd.read_sql(sql, conn)
    result = pd.merge(result, result_df, on=['channel_id', 'source_id'], how='right')
    result['tdate'] = pd.to_datetime(result['tdate'])
    result = result.sort_values('tdate')
    result = result.drop_duplicates(['channel_id', 'source_id'], keep='last')
    # 关闭链接
    cur.close()
    conn.close()

    return np.unique(result['image_id'].values)

def get_R_image():
    """ 数据：近30天的已关停大R计划素材 """

    conn = pymysql.connect(host='db-slave-modelfenxi-001.ch', port=3306, user='model_read',
                           passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8')
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
        /*手动查询*/
        SELECT
            a.chl_user_id AS channel_id,
            a.source_id AS source_id,
            a.image_id,
            b.tdate
        FROM
            db_data_ptom.ptom_plan a
            LEFT JOIN db_stdata.st_lauch_report b ON a.chl_user_id = b.channel_id 
            AND a.source_id = b.source_id 
        WHERE
            a.create_time >= date( NOW() - INTERVAL 720 HOUR )
            AND b.tdate >= date( NOW() - INTERVAL 720 HOUR )
            AND b.tdate_type = 'day' 
            AND b.media_id = 16
            AND b.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1056 AND dev_game_id IS NOT NULL )
            AND b.amount >= 4000
            AND ((b.new_role_money / b.pay_role_user_num >= 2000) or (b.new_role_money >= 1000 and b.pay_role_user_num = 1))
    '''
    cur.execute(sql)
    result = pd.read_sql(sql, conn)
    result['tdate'] = pd.to_datetime(result['tdate'])
    result = result.sort_values('tdate')
    result = result.drop_duplicates(['channel_id', 'source_id'], keep='last')  ## TODO:保留最近的1次
    result = result[result['tdate'].dt.date < datetime.date.today()]
    
    # 关闭链接
    cur.close()
    conn.close()

    return np.unique(result['image_id'].values)

def get_lz_image():
    """ 数据：近30天的有量低充值计划素材 """
    
    conn = pymysql.connect(host='db-slave-modelfenxi-001.ch', port=3306, user='model_read',
                           passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8')
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
        SELECT
            a.chl_user_id AS channel_id,
            a.source_id,
            a.image_id
        FROM
            db_data_ptom.ptom_plan a
            LEFT JOIN
            db_data_ptom.ptom_image_info b
        on a.image_id = b.image_id
        WHERE
            a.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1056 AND dev_game_id IS NOT NULL ) 
            AND a.media_id = 16
            AND b.create_time >= date( NOW() - INTERVAL 720 HOUR )  ## 素材：创建时间在30天以内
    '''
    cur.execute(sql)
    result_df = pd.read_sql(sql, conn)
    # 关闭链接
    cur.close()
    conn.close()

    conn = connect(host='192.168.0.97', port=10000, auth_mechanism='PLAIN', user='hadoop',
                   password='Ycjh8FxiaoMtShZRd3-97%3hCEL0CK4ns1w', database='default')
    cursor = conn.cursor()
    sql_engine = 'set hive.execution.engine=tez'
    sql = 'select image_id,image_run_date_amount,image_create_role_pay_sum,image_create_role_roi,dt from dws.dws_image_score_d where media_id=16 and dt>=date_sub(current_date, 30)'
    cursor.execute(sql_engine)
    cursor.execute(sql)
    result = as_pandas(cursor)
    result = result.sort_values('dt')
    result = result.drop_duplicates(['image_id'], keep='last')  ## TODO:保留最近的1次
    result = result[(result['image_run_date_amount']>10000) & (result['image_create_role_pay_sum']>18) & (result['image_create_role_roi']<0.01)]
    result = np.intersect1d(result['image_id'], result_df['image_id'])
    # 关闭链接
    cur.close()
    conn.close()

    return result

def get_lg_image():
    """ 数据：近30天的纯高量计划素材 """
    
    conn = pymysql.connect(host='db-slave-modelfenxi-001.ch', port=3306, user='model_read',
                           passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8')
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
        SELECT
            a.chl_user_id AS channel_id,
            a.source_id,
            a.image_id
        FROM
            db_data_ptom.ptom_plan a
            LEFT JOIN
            db_data_ptom.ptom_image_info b
        on a.image_id = b.image_id
        WHERE
            a.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1056 AND dev_game_id IS NOT NULL ) 
            AND a.media_id = 16
            AND b.create_time >= date( NOW() - INTERVAL 720 HOUR )  ## 素材：创建时间在30天以内
    '''
    cur.execute(sql)
    result_df = pd.read_sql(sql, conn)
    # 关闭链接
    cur.close()
    conn.close()

    conn = connect(host='192.168.0.97', port=10000, auth_mechanism='PLAIN', user='hadoop',
                   password='Ycjh8FxiaoMtShZRd3-97%3hCEL0CK4ns1w', database='default')
    cursor = conn.cursor()
    sql_engine = 'set hive.execution.engine=tez'
    sql = 'select image_id,image_run_date_amount,dt from dws.dws_image_score_d where media_id=16 and dt>=date_sub(current_date, 30)'
    cursor.execute(sql_engine)
    cursor.execute(sql)
    result = as_pandas(cursor)
    result = result.sort_values('dt')
    result = result.drop_duplicates(['image_id'], keep='last')  ## TODO:保留最近的1次
    result = result[result['image_run_date_amount']>80000]
    result = np.intersect1d(result['image_id'], result_df['image_id'])
    # 关闭链接
    cur.close()
    conn.close()

    return result

def get_roi(x, y, z):
    """ 数据：大盘roi与大盘pay_cost两个指标 """

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
                AND a.tdate <= DATE_SUB( date( NOW()), INTERVAL {} DAY )  AND a.tdate >= DATE_SUB( date( NOW()), INTERVAL {} DAY ) 
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

    return result_df['amount'].values[0], result_df['create_role_roi'].values[0], result_df['create_role_pay_cost'].values[0]

def get_roi_my(x, y, z):
    """ 数据：个人盘roi指标 """

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
                AND a.tdate <= DATE_SUB( date( NOW()), INTERVAL {} DAY )  AND a.tdate >= DATE_SUB( date( NOW()), INTERVAL {} DAY ) 
                AND a.media_id = 16
                AND a.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1056 AND dev_game_id IS NOT NULL ) 
                AND b.launch_op_id=13268  ## 只看个人盘数据
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

    return result_df['amount'].values[0], result_df['create_role_roi'].values[0], result_df['create_role_pay_cost'].values[0]

def get_amount_perimage_my(image_id):
    """ 数据：个人盘指定素材的消耗量 """

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
                AND a.tdate <= DATE_SUB( date( NOW()), INTERVAL 0 DAY )  AND a.tdate >= DATE_SUB( date( NOW()), INTERVAL 3 DAY ) 
                AND a.media_id = 16
                AND a.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1056 AND dev_game_id IS NOT NULL )
                AND b.image_id = {}
                # AND b.launch_op_id=13268  ## 只看个人盘数据
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
                    c.report_days = 3 
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
    finalSql = sql.format(int(image_id))
    cur.execute(finalSql)
    result_df = pd.read_sql(finalSql, conn)
    cur.close()
    conn.close()

    return result_df['amount'].values[0]


def get_roi_peraccount_my(ad_account_id):
    """ 数据：个人盘指定账号的roi """

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
                AND a.tdate <= DATE_SUB( date( NOW()), INTERVAL 0 DAY )  AND a.tdate >= DATE_SUB( date( NOW()), INTERVAL 7 DAY ) 
                AND a.media_id = 16
                AND a.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1056 AND dev_game_id IS NOT NULL ) 
                AND b.ad_account_id = {}
                # AND b.launch_op_id=13268  ## 可选：是否只看个人数据
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
                    c.report_days = 7 
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
    finalSql = sql.format(ad_account_id)
    cur.execute(finalSql)
    result_df = pd.read_sql(finalSql, conn)
    cur.close()
    conn.close()

    return result_df['create_role_roi'].values[0]


# TODO:分割线 ==========================================


def get_game_id(midware_id):
    """ 中间件关联游戏ID """
    conn = pymysql.connect(host='db-slave-modelfenxi-001.ch', port=3306, user='model_read',
                           passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8')
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
        SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = {} AND dev_game_id IS NOT NULL 
    '''
    cur.execute(sql.format(midware_id))
    result_df = cur.fetchall()
    cur.close()
    conn.close()
    return result_df

def get_ad_create(plan_result):
    """ 计划上传接口请求 """
    
    ad_info = []
    for i in range(plan_result.shape[0]):
        ad_info.append(json.loads(plan_result.iloc[i].to_json()))
    open_api_url_prefix = "http://ptom-pre.caohua.com/"
    uri = "model/generationPlanBatchTask"
    url = open_api_url_prefix + uri
    params = {
        "secretkey": "abc2018!@**@888",
        "mediaId": 16
    }
    rsp = requests.post(url, json=ad_info, params=params)
    rsp_data = rsp.json()
    print('[INFO]：上传结束...')

    return rsp_data

def location_type(dic):
    """ 修正：地域定向 """
    try:
        dic = eval(dic)
    except:
        dic = dic
        
    dic['geo_location']['location_types'] = ['LIVE_IN'] if dic['geo_location']['location_types']==['RECENTLY_IN'] else dic['geo_location']['location_types']
    
    return dic

def image_size(image_id, old_id, temp_id, conn):
    """ 匹配：素材-版位 """
    mapping = {560:[750,1334], 720:[1280,720], 721:[720,1280], 1480:[750,1334]}  ## 创意形式与尺寸对照

    sql = '''
            /*手动查询*/ 
            SELECT
                id,
                image_id,
                image_name,
                width,
                height,
                size
            FROM
                db_ptom.ptom_image_detail_info
            WHERE
                image_id = {}
        '''
    new_ids = pd.read_sql(sql.format(image_id), conn)  ## 新素材-主ID
    
    sql = '''
            /*手动查询*/ 
            SELECT
                id,
                image_id,
                image_name,
                width,
                height,
                size
            FROM
                db_ptom.ptom_image_detail_info
            WHERE
                id = {}
        '''
    old_result = pd.read_sql(sql.format(old_id), conn)  ## 原-子素材ID
    
    if len(old_result) != 0:
        new_id = new_ids[(new_ids['width']==old_result['width'].item()) & (new_ids['height']==old_result['height'].item())]['id']
    else:
        new_id = new_ids[(new_ids['width']==mapping[temp_id][0]) & (new_ids['height']==mapping[temp_id][1])]['id']

    return new_id.values[0]

def image_elements(df):
    """ 替换：尺寸匹配下的子素材ID """
    conn = pymysql.connect(host='db-slave-modeltoufang-001.ch', port=3306, user='model_read',
                       passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8')

    dic = df['adcreative_elements'].copy()
    try:
        a = list(dic.keys())
        b = ['short_video_struct','image','video']  ## 需要替换id的三个key
        ks = list(set(a).intersection(set(b)))
        for k in ks:
            if isinstance(dic[k], dict):
                old_id = dic[k]['short_video1']
                new_id = image_size(df['image_id'], old_id, df['adcreative_template_id'], conn)
                tmp = dic[k].copy()
                tmp['short_video1'] = new_id
                dic[k] = tmp
            else:
                old_id = dic[k]
                new_id = image_size(df['image_id'], old_id, df['adcreative_template_id'], conn)
                dic[k] = new_id
        conn.close()
        return dic
    except Exception as e:
        traceback.print_exc()
        return np.nan

# def matching(ad_account_ids, image_ids):
#     """ 映射：账号与素材的配对 """
#     assert len(ad_account_ids) == len(image_ids), "长度不一致！"
#     repeat = np.inf
#     confs = {'ad_account_ids':ad_account_ids, 'image_ids':image_ids}

#     def yield_genarate(confs):
#         """ 生成器：穷举 """
#         for conf in product(*confs.values()):
#             yield {k:v for k,v in zip(confs.keys(), conf)}

#     for conf in yield_genarate(confs):