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
        SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1143 AND dev_game_id IS NOT NULL 
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
            a.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1143 AND dev_game_id IS NOT NULL ) 
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

    if 'behavior_or_interest' in plan_info.columns:
        temp = plan_info['behavior_or_interest'].apply(pd.Series)
        plan_info = pd.concat([plan_info, temp], axis=1)
        plan_info.drop('behavior_or_interest', axis=1, inplace=True)
        plan_info.drop(0, axis=1, inplace=True)

        if 'intention' in plan_info.columns:
            temp = plan_info['intention'].apply(pd.Series)
            plan_info = pd.concat([plan_info, temp], axis=1)
            plan_info.drop('intention', axis=1, inplace=True)
            plan_info = plan_info.rename(columns={'targeting_tags': 'intention_targeting_tags'})
            plan_info.drop(0, axis=1, inplace=True)
        else:
            plan_info['intention_targeting_tags'] = np.nan

        if 'interest' in plan_info.columns:
            temp = plan_info['interest'].apply(pd.Series)
            plan_info = pd.concat([plan_info, temp], axis=1)
            plan_info.drop('interest', axis=1, inplace=True)
            plan_info = plan_info.rename(
                columns={'category_id_list': 'interest_category_id_list', 'keyword_list': 'interest_keyword_list',
                         'targeting_tags': 'interest_targeting_tags'})
            plan_info.drop(0, axis=1, inplace=True)
        else:
            plan_info['interest_category_id_list'] = np.nan
            plan_info['interest_keyword_list'] = np.nan
            plan_info['interest_targeting_tags'] = np.nan

        if 'behavior' in plan_info.columns:
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
        else:
            plan_info['behavior_category_id_list'] = np.nan
            plan_info['behavior_intensity'] = np.nan
            plan_info['behavior_keyword_list'] = np.nan
            plan_info['behavior_scene'] = np.nan
            plan_info['behavior_targeting_tags'] = np.nan
            plan_info['behavior_time_window'] = np.nan

    else:
        plan_info['intention_targeting_tags'] = np.nan
        plan_info['interest_category_id_list'] = np.nan
        plan_info['interest_keyword_list'] = np.nan
        plan_info['interest_targeting_tags'] = np.nan
        plan_info['behavior_category_id_list'] = np.nan
        plan_info['behavior_intensity'] = np.nan
        plan_info['behavior_keyword_list'] = np.nan
        plan_info['behavior_scene'] = np.nan
        plan_info['behavior_targeting_tags'] = np.nan
        plan_info['behavior_time_window'] = np.nan

    if 'excluded_converted_audience' in plan_info.columns:
        temp = plan_info['excluded_converted_audience'].apply(pd.Series)
        plan_info = pd.concat([plan_info, temp], axis=1)
        plan_info.drop('excluded_converted_audience', axis=1, inplace=True)
        plan_info.drop(0, axis=1, inplace=True)
    else:
        plan_info['excluded_dimension'] = np.nan

    if 'geo_location' in plan_info.columns:
        temp = plan_info['geo_location'].apply(pd.Series)
        plan_info = pd.concat([plan_info, temp], axis=1)
        plan_info.drop('geo_location', axis=1, inplace=True)
        plan_info.drop(0, axis=1, inplace=True)
    else:
        plan_info['location_types'] = np.nan
        plan_info['regions'] = np.nan

    # 过滤一对多计划
    plan_info['ad_id_count'] = plan_info.groupby('plan_id')['ad_id'].transform('count')
    plan_info = plan_info[plan_info['ad_id_count'] == 1]

    # 删除纯买激活的计划
    plan_info = plan_info[~((plan_info['deep_conversion_type'].isna()) & (
            plan_info['optimization_goal'] == 'OPTIMIZATIONGOAL_APP_ACTIVATE'))]
    # 删除auto_audience=True 的记录，并且删除auto_audience字段
    plan_info[plan_info['auto_audience'] == False]
    for col in ['ad_account_id', 'game_id', 'channel_id', 'source_id', 'budget_mode',
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
                'regions']:
        if col in plan_info.columns:
            pass
        else:
            plan_info[col] = np.nan
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


# 获取近期30天优化计划的创意数据
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
# def get_score_image():
#     conn = connect(host='192.168.0.97', port=10000, auth_mechanism='PLAIN', user='hadoop',
#                    password='Ycjh8FxiaoMtShZRd3-97%3hCEL0CK4ns1w', database='default')
#     cursor = conn.cursor()
#     sql_engine = 'set hive.execution.engine=tez'
#     sql = 'select image_id,label_ids from dws.dws_image_score_d where media_id=16 and score>=520 and dt=CURRENT_DATE group by image_id,label_ids'
#     cursor.execute(sql_engine)
#     cursor.execute(sql)
#     result = as_pandas(cursor)
#     result['label_ids'] = result['label_ids'].astype(str)
#     result['label_ids'] = result['label_ids'].apply(lambda x: x.strip('-1;') if '-1' in x else x)
#     result['label_ids'] = pd.to_numeric(result['label_ids'], errors='coerce')
#
#     # 关闭链接
#     cursor.close()
#     conn.close()
#
#     return result['image_id'].values

# 获取近3天回款率高的素材
def get_good_image():
    conn = pymysql.connect(host='db-slave-modelfenxi-001.ch', port=3306, user='model_read',
                           passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8')
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
        /*手动查询*/ 
        SELECT
            a.image_id AS 'image_id',
            a.image_name AS 'image_name',
            a.media_id,
            a.label_ids,
            ifnull( a.image_run_date_amount, 0 ) AS 'image_run_date_amount',
            ifnull( a.image_create_role_pay_num, 0 ) AS 'image_create_role_pay_num',
            ifnull( a.image_create_role_num, 0 ) AS 'image_create_role_num',
            ifnull( a.image_create_role_pay_sum, 0 ) AS 'image_create_role_pay_sum',
            ifnull( a.image_source_num, 0 ) AS 'image_source_num',
            (
            CASE

                    WHEN ifnull( a.image_create_role_num, 0 )= 0 THEN
                    0 ELSE IFNULL( a.image_create_role_pay_num, 0 ) / ifnull( a.image_create_role_num, 0 ) 
                END 
                ) AS 'image_create_role_pay_rate',
                (
                CASE

                        WHEN ifnull( a.image_create_role_num, 0 )= 0 THEN
                        0 ELSE IFNULL( a.image_run_date_amount, 0 ) / ifnull( a.image_create_role_num, 0 ) 
                    END 
                    ) AS 'image_create_role_cost',
                    (
                    CASE

                            WHEN ifnull( a.image_create_role_pay_num, 0 )= 0 THEN
                            0 ELSE IFNULL( a.image_run_date_amount, 0 ) / ifnull( a.image_create_role_pay_num, 0 ) 
                        END 
                        ) AS 'image_create_role_pay_cost',
                        (
                        CASE

                                WHEN ifnull( a.image_run_date_amount, 0 )= 0 THEN
                                0 ELSE IFNULL( a.image_create_role_pay_sum, 0 ) / ifnull( a.image_run_date_amount, 0 ) 
                            END 
                            ) AS 'image_create_role_roi' 
                        FROM
                            (
                            SELECT
                                aa.image_id AS 'image_id',
                                aa.image_name AS 'image_name',
                                aa.media_id AS 'media_id',
                                aa.label_ids AS 'label_ids',
                                sum( aa.amount ) AS 'image_run_date_amount',
                                IFNULL( sum( aa.create_role_num ), 0 ) AS 'image_create_role_num',
                                IFNULL( sum( bb.pay_role_user_num ), 0 ) AS 'image_create_role_pay_num',
                                IFNULL( sum( bb.new_role_money ), 0 ) AS 'image_create_role_pay_sum',
                                IFNULL( sum( aa.image_source_num ), 0 ) AS 'image_source_num' 
                            FROM
                                (
                                SELECT
                                    a.game_id,
                                    a.channel_id,
                                    a.source_id,
                                    a.media_id,
                                    c.image_id,
                                    c.image_name,
                                    c.label_ids,
                                    IFNULL( sum( a.amount ), 0 ) AS amount,
                                    IFNULL( sum( create_role_num ), 0 ) AS create_role_num,
                                    count( DISTINCT b.plan_id ) AS 'image_source_num' 
                                FROM
                                    db_stdata.st_lauch_report a
                                    INNER JOIN db_data_ptom.ptom_plan b ON a.game_id = b.game_id 
                                    AND a.source_id = b.source_id 
                                    AND a.channel_id = b.chl_user_id
                                    LEFT JOIN db_data_ptom.ptom_image_info c ON b.image_id = c.image_id 
                                WHERE
                                    a.tdate_type = 'day' 
                                    AND a.tdate <= date( NOW()) AND a.tdate >= DATE_SUB( date( NOW()), INTERVAL 3 DAY ) 
                                    AND a.amount > 0 
                                    AND a.media_id IN ( 10, 16 ) 
                                    AND b.image_id IS NOT NULL 
                                    AND b.image_id <> '' 
                                    AND a.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1143 AND dev_game_id IS NOT NULL ) 
                                GROUP BY
                                    a.game_id,
                                    a.channel_id,
                                    a.source_id 
                                HAVING
                                    c.image_id IS NOT NULL 
                                    AND c.image_name <> '' 
                                ) aa
                                LEFT JOIN (
                                SELECT
                                    game_id,
                                    channel_id,
                                    source_id,
                                    IFNULL( sum( m.new_role_money ), 0 ) AS new_role_money,
                                    IFNULL( sum( m.pay_role_user_num ), 0 ) AS pay_role_user_num 
                                FROM
                                    (
                                    SELECT
                                        a.game_id,
                                        a.channel_id,
                                        a.source_id,
                                        IFNULL( sum( a.new_role_money ), 0 ) AS new_role_money,
                                        IFNULL( sum( a.pay_role_user_num ), 0 ) AS pay_role_user_num 
                                    FROM
                                        db_stdata.st_lauch_report a
                                        INNER JOIN db_data_ptom.ptom_plan pp ON ( a.game_id = pp.game_id AND a.channel_id = pp.chl_user_id AND a.source_id = pp.source_id ) 
                                    WHERE
                                        a.tdate = date(
                                        NOW()) 
                                        AND a.tdate_type = 'day' 
                                    GROUP BY
                                        a.game_id,
                                        a.channel_id,
                                        a.source_id 
                                    HAVING
                                        ( new_role_money > 0 OR pay_role_user_num > 0 ) UNION ALL
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
                                    ( new_role_money > 0 OR pay_role_user_num > 0 )) m 
                                GROUP BY
                                    game_id,
                                    channel_id,
                                    source_id 
                                ) bb ON aa.game_id = bb.game_id 
                                AND aa.channel_id = bb.channel_id 
                                AND aa.source_id = bb.source_id 
                            GROUP BY
                                aa.image_id,
                                aa.image_name,
                                aa.media_id 
                            HAVING
                            image_run_date_amount >= 100 
            ) a
            WHERE a.image_create_role_pay_sum >0 and 
                a.media_id = 16
                ORDER BY image_create_role_roi Desc
    '''
#     finalSql = sql.format(game_id)
    cur.execute(sql)
    result_df = pd.read_sql(sql, conn)
    cur.close()
    conn.close()
    result_df = result_df[['image_id','image_run_date_amount','image_create_role_pay_num','image_create_role_pay_sum',
                        'image_create_role_pay_cost','image_create_role_roi']]
    result_df['roi_rank'] = result_df['image_create_role_roi'].rank(ascending=False)
    result_df = result_df[result_df['roi_rank'] <= np.ceil((result_df.shape[0] / 2))]
    return result_df['image_id'].values


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
            b.tdate >= date( NOW() - INTERVAL 240 HOUR ) 
            AND b.tdate_type = 'day' 
            AND b.media_id = 16 
            AND b.game_id IN ({}) 
            AND b.amount >= 200  
            AND b.pay_role_user_num >= 1 
            AND b.new_role_money >= 20
            AND (b.new_role_money / b.amount)>=0.02
    '''
    finalSql = sql.format(game_id)
    cur.execute(finalSql)
    result_df = pd.read_sql(finalSql, conn)
    cur.close()
    conn.close()
    result_df['tdate'] = pd.to_datetime(result_df['tdate'])
    result_df = result_df.sort_values('tdate')
    result_df = result_df.drop_duplicates(['channel_id', 'source_id'], keep='first')
#     result_df = result_df[result_df['roi'] >= 0.03]
    return result_df


def create_plan(df, score_image):
    # 选ad_account_id、image_id每个账号+素材8条
    game_id = 1001854
    # df = df[df['game_id'] == game_id]
    # df = df[df['game_id'].isin([1001379, 1001757, 1001841])]

    ad_account_id_group = np.array([9561, 9562, 9563, 9564, 9565])

    image_id_group = np.intersect1d(df['image_id'].unique(), score_image)

    print(image_id_group)
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

    plan['promoted_object_id'] = '2000002794'
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
    for col in ['channel_id', 'source_id', 'image_id', 'deep_link_url',
                'adcreative_template_id', 'page_spec', 'page_type', 'site_set', 'label',
                'promoted_object_id', 'promoted_object_type', 'automatic_site_enabled',
                'link_name_type', 'link_page_type', 'profile_id', 'link_page_spec',
                'adcreative_elements']:
        if col in creative_info.columns:
            pass
        else:
            creative_info[col] = np.nan
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
    # score_image = get_score_image()
    score_image = get_good_image()   ## TODO 没有素材评分，暂时用这个
    image_info = image_info[image_info['image_id'].notna()]
    image_info['image_id'] = image_info['image_id'].astype(int)
    df_create = pd.merge(plan_info_current, image_info, on=['channel_id', 'source_id', 'image_id'], how='left')
    df_create = pd.merge(df_create, now_plan_roi.drop(['ad_account_id'], axis=1), on=['channel_id', 'source_id'],
                         how='inner')
    # df_create = df_create[df_create['site_set'].notna()]   # TODO支持自动版位
    df_create['site_set'] = df_create['site_set'].map(str)
    df_create['site_set'] = df_create['site_set'].replace('nan', "[]")
    # 只跑ROI和付费次数 TODO
    # # df_create = df_create[(df_create['optimization_goal'] == 'OPTIMIZATIONGOAL_APP_ACTIVATE') | (df_create['optimization_goal'] == 'OPTIMIZATIONGOAL_APP_PURCHASE')]
    # df_create = df_create[df_create['optimization_goal'] == 'OPTIMIZATIONGOAL_APP_ACTIVATE']
    df_create.to_csv('./df_create.csv', index=0)
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

    df['plan_label'] = df.apply(lambda x: 1 if x.new_role_money / x.amount >= 0.025 else 0, axis=1)
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
#     open_api_url_prefix = "https://ptom-pre.caohua.com/"   ## 预发布环境
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
    threshold = pd.Series(y_predict).sort_values(ascending=False).reset_index(drop=True)[int(y_predict.shape[0] * 0.5)]

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

    # plan_result['weight'] = plan_result.groupby(['ad_account_id'])['game_id'].transform('count')
    plan_result['site_set'] = plan_result['site_set'].map(str)

    # if plan_result.shape[0] > plan_num:
    #     plan_result = plan_result.sample(plan_num, weights=plan_result['weight'])

    ad_account_id_group = np.array([9561, 9562, 9563, 9564, 9565])
    # ad_account_id_group = np.array([8854, 8817, 8839])
    # 不要711
    plan_result = plan_result[plan_result['adcreative_template_id'] != 711]
    plan_num = 5
    plan_result_n = pd.DataFrame()
    for account_id in ad_account_id_group:
        plan_result_ = plan_result[plan_result['ad_account_id'] == account_id]
        plan_result_ = plan_result_.sample(plan_num)
        plan_result_n = plan_result_n.append(plan_result_)
    plan_result = plan_result_n

    plan_result['rank_ad_im'] = plan_result.groupby(['ad_account_id', 'image_id'])['prob'].rank(ascending=False,
                                                                                                method='first')
    plan_result = plan_result[plan_result['rank_ad_im'] <= 1]

    plan_result = plan_result.drop(['create_time', 'create_date', 'prob', 'rank_ad_im', 'label_ids'],
                                   axis=1)
    # plan_result = plan_result[plan_result['site_set'] == "['SITE_SET_MOMENTS']"]  ## TODO 只跑朋友圈
    # [SITE_SET_WECHAT] 公众号和小程序adcreative_template_id只跑1480、560、720、721、1064五种
    plan_result = plan_result[~((plan_result['site_set'] == "['SITE_SET_WECHAT']") & (
        ~plan_result['adcreative_template_id'].isin([1480, 560, 720, 721, 1064])))]

    plan_result['location_types'] = plan_result['location_types'].apply(lambda x: ['LIVE_IN'] if x == x else x)

    # 修改落地页ID
    plan_result['page_spec'] = plan_result.apply(
        lambda x: {'override_canvas_head_option': 'OPTION_CREATIVE_OVERRIDE_CANVAS',
                   'page_id': '2314299445'} if x.site_set == "['SITE_SET_MOMENTS']"
        else ({'override_canvas_head_option': 'OPTION_CREATIVE_OVERRIDE_CANVAS',
               'page_id': '2314272060'} if x.site_set == "['SITE_SET_WECHAT']" else np.nan), axis=1)

    # plan_result['page_spec'] = np.where(plan_result.site_set.str.contains('SITE_SET_WECHAT') & plan_result.site_set != "['SITE_SET_WECHAT']", {'page_id': '2314272060'}, plan_result.page_spec)

    # 朋友圈头像ID
    plan_result['ad_account_id'] = plan_result['ad_account_id'].map(str)
    plan_result_1 = plan_result[plan_result['site_set'] == "['SITE_SET_MOMENTS']"]
    plan_result_2 = plan_result[plan_result['site_set'] != "['SITE_SET_MOMENTS']"]

    # 七雄
    profile_id_dict = {'9561': '663136', '9562': '663143', '9563': '663157', '9564': '663169', '9565': '663178'}

    plan_result_1['profile_id'] = plan_result_1['ad_account_id'].map(profile_id_dict)

    plan_result = plan_result_1.append(plan_result_2)
    plan_result = plan_result.reset_index(drop=True)

    # 年龄定向
    plan_result['age'] = plan_result['age'].apply(lambda x: [{'min': 21, 'max': 48}])

    # 出价方式  ##TODO  固定ROI出价
    plan_result['optimization_goal'] = plan_result['optimization_goal'].apply(lambda x: 'OPTIMIZATIONGOAL_APP_ACTIVATE')
    plan_result['deep_conversion_type'] = plan_result['optimization_goal'].apply(
        lambda x: 'DEEP_CONVERSION_WORTH' if x == 'OPTIMIZATIONGOAL_APP_ACTIVATE' else np.nan)
    plan_result['deep_conversion_worth_spec'] = plan_result['optimization_goal'].apply(
        lambda x: {'goal': 'GOAL_1DAY_PURCHASE_ROAS',
                   'expected_roi': 0.036} if x == 'OPTIMIZATIONGOAL_APP_ACTIVATE' else np.nan)
    plan_result['deep_conversion_behavior_spec'] = np.nan
    # 固定出价
    plan_result['bid_amount'] = plan_result['optimization_goal'].apply(
        lambda x: random.randint(250000, 280000) if x == 'OPTIMIZATIONGOAL_APP_ACTIVATE'
        else (random.randint(210000, 220000) if x == 'OPTIMIZATIONGOAL_APP_PURCHASE'
              else (random.randint(430000, 440000))))

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
    plan_result['plan_auto_task_id'] = "11427,12114"
    plan_result['op_id'] = 13268
    plan_result['flag'] = 'GDT'
    plan_result['game_name'] = '七雄'
    plan_result['platform'] = 1
    plan_result['ad_account_id'] = plan_result['ad_account_id'].astype(int)
    # 固定品牌名
    plan_result = plan_result.reset_index(drop=True)
    # plan_result['adcreative_elements'] = plan_result['adcreative_elements'].apply(ast.literal_eval)
    # for i in range(plan_result.shape[0]):
    #     a = plan_result.loc[i, 'adcreative_elements'].copy()
    #     a['brand'] = {'brand_name': '七雄争霸', 'brand_img': 123609}
    #     plan_result.loc[i, 'adcreative_elements'] = str(a)
    #
    # plan_result['adcreative_elements'] = plan_result['adcreative_elements'].apply(ast.literal_eval)
    plan_result['site_set'] = plan_result['site_set'].apply(ast.literal_eval)
    # # 开启一键起量
    # plan_result['auto_acquisition_enabled'] = True
    # plan_result['auto_acquisition_budget'] = 6666
    # # 开启自动扩量
    # plan_result['expand_enabled'] = True
    # plan_result['expand_targeting'] = plan_result['expand_targeting'].apply(lambda x: [])
    # # # 开启加速投放
    # plan_result['speed_mode'] = 'SPEED_MODE_FAST'

    # plan_result['time_series'] = plan_result['time_series'].apply(
    #     lambda x: x[0:96] + '1111111111000000000011' + x[118:144] + '1111111111000000000011' + x[166:])
    # plan_result['wechat_page '] = 1
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
