# -*- coding:utf-8 -*-

import pandas as pd
import numpy as np
import json
import logging
import numpy as np
import gc
from impala.dbapi import connect
from impala.util import as_pandas
import pymysql
import ast
import requests
import random
import warnings
warnings.filterwarnings('ignore', category=FutureWarning)

import modelservice.serv_conf as serv_conf
nacosServ = serv_conf.get_var()  # !!! 增加线上服务参数nacos配置


def get_game_id(game_id):
    conn = pymysql.connect(host=nacosServ['DB_SLAVE_FENXI_HOST'], port=int(nacosServ['DB_SLAVE_FENXI_PORT']), user=nacosServ['DB_SLAVE_FENXI_USERNAME'],
                           passwd=nacosServ['DB_SLAVE_FENXI_PASSWORD'])
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql1 = '''
        SELECT game_id FROM db_data.t_game_config WHERE dev_game_id = {}
    '''
    sql2 = '''
        SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = {} AND dev_game_id IS NOT NULL 
    '''
    mgame_id = pd.read_sql(sql1.format(game_id), conn)['game_id'].item()
    cur.execute(sql2.format(mgame_id))
    result_df = cur.fetchall()
    cur.close()
    conn.close()

    return result_df


# TODO =========分界线：素材部分===========

def get_score_image(media_id, score_limit):
    conn = connect(host=nacosServ['HIVE_HOST'], port=int(nacosServ['HIVE_PORT']), auth_mechanism=nacosServ['HIVE_AUTH_MECHANISM'], 
                   user=nacosServ['HIVE_USERNAME'],password=nacosServ['HIVE_PASSWORD'], database=nacosServ['HIVE_DATABASE'])
    cursor = conn.cursor()
    sql_engine = 'set hive.execution.engine=tez'
    sql_queue = 'set tez.queue.name=offline'
    sql = '''
        SELECT
            image_id,
            label_ids 
        FROM
            dws.dws_image_score_d 
        WHERE
            dt = CURRENT_DATE 
            AND media_id = {}
            AND image_create_role_roi >= 0.005 
            AND ( image_create_role_retain_1d >= 0.01 OR image_create_role_retain_1d IS NULL ) 
            AND score >= {}
        GROUP BY
            image_id,
            label_ids
    '''
    cursor.execute(sql_engine)
    cursor.execute(sql_queue)
    cursor.execute(sql.format(media_id, score_limit))
    result = as_pandas(cursor)
    cursor.close()
    conn.close()
    
    return result['image_id'].values


def get_score_image_nodt(media_id, score_limit):
    conn = connect(host=nacosServ['HIVE_HOST'], port=int(nacosServ['HIVE_PORT']), auth_mechanism=nacosServ['HIVE_AUTH_MECHANISM'], 
                   user=nacosServ['HIVE_USERNAME'],password=nacosServ['HIVE_PASSWORD'], database=nacosServ['HIVE_DATABASE'])
    cursor = conn.cursor()
    sql_engine = 'set hive.execution.engine=tez'
    sql_queue = 'set tez.queue.name=offline'
    sql = '''
        SELECT
            image_id,
            label_ids 
        FROM
            dws.dws_image_score_d 
        WHERE
            dt >= date_sub(current_date, 2)
            dt <= CURRENT_DATE
            AND media_id = {}
            AND image_create_role_roi >= 0.005 
            AND ( image_create_role_retain_1d >= 0.05 OR image_create_role_retain_1d IS NULL ) 
            AND score >= {}
        GROUP BY
            image_id,
            label_ids
    '''
    cursor.execute(sql_engine)
    cursor.execute(sql_queue)
    cursor.execute(sql.format(media_id, score_limit))
    result = as_pandas(cursor)
    cursor.close()
    conn.close()
    
    return result['image_id'].values


def filter_game_image(image_ids, game_id):
    image_ids = [str(i) for i in image_ids]
    image_ids = ','.join(image_ids)

    conn = pymysql.connect(host=nacosServ['DB_SLAVE_FENXI_HOST'], port=int(nacosServ['DB_SLAVE_FENXI_PORT']), user=nacosServ['DB_SLAVE_FENXI_USERNAME'],
                           passwd=nacosServ['DB_SLAVE_FENXI_PASSWORD'])
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
        SELECT
            image_id,
            image_name
        FROM
            db_data_ptom.ptom_image_info
        WHERE
            image_id IN ({})
            AND FIND_IN_SET({}, game_ids)
    '''
    cur.execute(sql.format(image_ids, game_id))
    result = pd.read_sql(sql.format(image_ids, game_id), conn)
    cur.close()
    conn.close()

    return result['image_id'].values


def filter_exclude_image(image_ids, media_id):
    image_ids = [str(i) for i in image_ids]
    image_ids = ','.join(image_ids)

    conn = pymysql.connect(host=nacosServ['DB_SLAVE_FENXI_HOST'], port=int(nacosServ['DB_SLAVE_FENXI_PORT']), user=nacosServ['DB_SLAVE_FENXI_USERNAME'],
                           passwd=nacosServ['DB_SLAVE_FENXI_PASSWORD'])
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
        SELECT
            image_id,
            image_name
        FROM
            db_data_ptom.ptom_image_info
        WHERE
            image_id IN ({})
            AND NOT FIND_IN_SET({}, exclude_media_ids)
    '''
    result = pd.read_sql(sql.format(image_ids, media_id), conn)
    cur.close()
    conn.close()

    return result['image_id'].values


def get_sort_image(image_ids, media_id):
    image_ids = [str(i) for i in image_ids]
    image_ids = ','.join(image_ids)

    conn = connect(host=nacosServ['HIVE_HOST'], port=int(nacosServ['HIVE_PORT']), auth_mechanism=nacosServ['HIVE_AUTH_MECHANISM'], 
                   user=nacosServ['HIVE_USERNAME'],password=nacosServ['HIVE_PASSWORD'], database=nacosServ['HIVE_DATABASE'])
    cursor = conn.cursor()
    sql_engine = 'set hive.execution.engine=tez'
    sql_queue = 'set tez.queue.name=offline'
    sql = '''
        SELECT
            image_id,
            score 
        FROM
            dws.dws_image_score_d
        WHERE
            dt = CURRENT_DATE
            AND image_id IN ({})
            AND media_id = {}
        GROUP BY
            image_id,
            score
    '''
    cursor.execute(sql_engine)
    cursor.execute(sql_queue)
    cursor.execute(sql.format(image_ids, media_id))
    result = as_pandas(cursor)
    result.sort_values(by='score', ascending=False, inplace=True)
    cursor.close()
    conn.close()
    
    return result['image_id'].values.tolist()


# TODO =========分界线：要素部分===========

def get_plan_info(game_ids, media_id):
    game_ids = list(map(lambda x: x['game_id'], game_ids))
    game_ids = [str(i) for i in game_ids]
    game_ids = ','.join(game_ids)
    conn = pymysql.connect(host=nacosServ['DB_SLAVE_TOUFANG_HOST'], port=int(nacosServ['DB_SLAVE_TOUFANG_PORT']), user=nacosServ['DB_SLAVE_TOUFANG_USERNAME'],
                           passwd=nacosServ['DB_SLAVE_TOUFANG_PASSWORD'], db=nacosServ['DB_SLAVE_TOUFANG_DATABASE'])
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
    /*手动查询*/
        SELECT
                * 
            FROM
                db_ptom.ptom_third_plan p
            WHERE
                game_id IN ({})
                AND media_id = {}
                AND create_time>=date( NOW() - INTERVAL 1440 HOUR )
                AND create_time<= date(NOW())
                            AND plan_id >= (
                                select plan_id from db_ptom.ptom_plan
                                where create_time >= date( NOW() - INTERVAL 1440 HOUR )
                                and create_time <= date( NOW() - INTERVAL 1416 HOUR )
                                limit 1
                            )
    '''
    finalSql = sql.format(game_ids, media_id)
    result_df = pd.read_sql(finalSql, conn)
    cur.close()
    conn.close()
    return result_df


def get_plan_json_tt(plan_info):
    plan_info.drop(['inventory_type', 'budget'], axis=1, inplace=True)
    plan_info.dropna(how='all', inplace=True, axis=1)
    plan_info.dropna(subset=['ad_info'], inplace=True)
    # 解析json
    plan_info['ad_info'] = plan_info['ad_info'].apply(json.loads)
    temp = plan_info['ad_info'].apply(pd.Series)
    plan_info = pd.concat([plan_info, temp], axis=1)
    plan_info.drop('ad_info', axis=1, inplace=True)
    temp = plan_info['audience'].apply(pd.Series)
    plan_info = pd.concat([plan_info, temp], axis=1)
    plan_info.drop('audience', axis=1, inplace=True)
    temp = plan_info['action'].apply(pd.Series)
    plan_info = pd.concat([plan_info, temp], axis=1)
    plan_info.drop('action', axis=1, inplace=True)
    plan_info.dropna(how='all', inplace=True, axis=1)

    for col in ['ad_account_id', 'game_id', 'channel_id', 'source_id',
                           'create_time', 'smart_bid_type', 'hide_if_exists', 'budget',
                           'delivery_range', 'adjust_cpa', 'inventory_type', 'hide_if_converted',
                           'flow_control_mode', 'schedule_time', 'cpa_bid', 'auto_extend_enabled',
                           'gender', 'city', 'platform', 'launch_price',
                           'retargeting_tags_exclude', 'interest_categories',
                           'ac', 'android_osv', 'location_type', 'retargeting_tags_include',
                           'ios_osv', 'interest_action_mode', 'age',
                           'action_categories', 'action_days', 'action_scene', 'deep_bid_type', 'roi_goal']:
        if col in plan_info.columns:
            pass
        else:
            plan_info[col] = np.nan
    plan_info = plan_info[['ad_account_id', 'game_id', 'channel_id', 'source_id',
                           'create_time', 'smart_bid_type', 'hide_if_exists', 'budget',
                           'delivery_range', 'adjust_cpa', 'inventory_type', 'hide_if_converted',
                           'flow_control_mode', 'schedule_time', 'cpa_bid', 'auto_extend_enabled',
                           'gender', 'city', 'platform', 'launch_price',
                           'retargeting_tags_exclude', 'interest_categories',
                           'ac', 'android_osv', 'location_type', 'retargeting_tags_include',
                           'ios_osv', 'interest_action_mode', 'age',
                           'action_categories', 'action_days', 'action_scene', 'deep_bid_type', 'roi_goal']]
    return plan_info


def get_plan_json_gdt(plan_info):
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
                'gender', 'game_consumption_level', 'age', 'custom_audience', 'excluded_custom_audience',
                'network_type',
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
                           'gender', 'game_consumption_level', 'age', 'custom_audience', 'excluded_custom_audience',
                           'network_type',
                           'deep_conversion_type', 'deep_conversion_behavior_spec',
                           'deep_conversion_worth_spec', 'intention_targeting_tags',
                           'interest_category_id_list', 'interest_keyword_list',
                           'behavior_category_id_list',
                           'behavior_intensity', 'behavior_keyword_list', 'behavior_scene',
                           'behavior_time_window',
                           'conversion_behavior_list', 'excluded_dimension', 'location_types',
                           'regions']]
    return plan_info


def get_plan_json_bd(plan_info):
    plan_info['ad_info'] = plan_info['ad_info'].apply(json.loads)
    temp = plan_info['ad_info'].apply(pd.Series)
    plan_info = pd.concat([plan_info,temp], axis=1)
    plan_info.drop('ad_info', axis=1, inplace=True)

    temp = plan_info['audience'].apply(pd.Series)
    plan_info = pd.concat([plan_info,temp], axis=1)
    plan_info.drop('audience', axis=1, inplace=True)

    temp = plan_info['ocpc'].apply(pd.Series)
    plan_info = pd.concat([plan_info,temp], axis=1)
    plan_info.drop('ocpc', axis=1, inplace=True)

    plan_info = plan_info[['ad_account_id', 'game_id', 'channel_id', 'source_id', 'create_time',
           'bidtype', 'producttypes', 'ftypes', 'pause', 'autoExpansion',
           'education', 'keywords', 'keywordsExtend', 'newInterests',
           'sex', 'iosVersion', 'androidVersion', 'excludeTrans', 'region',
           'device', 'age', 'transType', 'ocpcLevel', 'isSkipStageOne', 'payMode',
           'optimizeDeepTrans', 'transFrom', 'ocpcBid', 'deepOcpcBid',
           'deepTransType', 'roiRatio', 'useRoi']]
    
    plan_info = plan_info.sort_values(by='create_time')
    plan_info = plan_info.drop_duplicates(subset=['channel_id','source_id'], keep='last')
    return plan_info


def get_plan_json_ks(plan_info):
    plan_info.drop(['inventory_type', 'budget'], axis=1, inplace=True)
    plan_info.dropna(how='all', inplace=True, axis=1)
    plan_info.dropna(subset=['ad_info'], inplace=True)
    # 解析json
    plan_info['ad_info'] = plan_info['ad_info'].apply(json.loads)

    temp = plan_info['ad_info'].apply(pd.Series)
    plan_info = pd.concat([plan_info, temp], axis=1)
    plan_info.drop('ad_info', axis=1, inplace=True)
    temp = plan_info['diverse_data'].apply(pd.Series)
    plan_info = pd.concat([plan_info, temp], axis=1)
    plan_info.drop('diverse_data', axis=1, inplace=True)
    temp = plan_info['backflow_forecast'].apply(pd.Series)
    plan_info = pd.concat([plan_info, temp], axis=1)
    plan_info.drop('backflow_forecast', axis=1, inplace=True)
    temp = plan_info['target'].apply(pd.Series)
    plan_info = pd.concat([plan_info, temp], axis=1)
    plan_info.drop('target', axis=1, inplace=True)
    plan_info.dropna(how='all', inplace=True, axis=1)

    try:
        plan_info = plan_info[['channel_id', 'source_id', 
                               'ad_account_id', 'game_id', 'create_time',
                               'bid_type', 'cpa_bid', 'day_budget',
                               'scene_id', 'ocpx_action_type', 'roi_ratio',
                               'deep_conversion_type', 'deep_conversion_bid', 
                               'unit_type', 'show_mode', 'speed',
                               'auto_target', 'filter_converted_level', 'schedule_time',
                               'gender', 'region', 'platform_os',
                               'android_osv', 'network', 'device_brand_ids',
                               'population', 'business_interest_type',
                               'age', 'behavior_interest', 'intelli_extend']] if 'android_osv' in plan_info.columns else \
                    plan_info[['channel_id', 'source_id', 
                               'ad_account_id', 'game_id', 'create_time',
                               'bid_type', 'cpa_bid', 'day_budget',
                               'scene_id', 'ocpx_action_type', 'roi_ratio',
                               'deep_conversion_type', 'deep_conversion_bid', 
                               'unit_type', 'show_mode', 'speed',
                               'auto_target', 'filter_converted_level', 'schedule_time',
                               'gender', 'region', 'platform_os',
                               'ios_osv', 'network', 'device_brand_ids',
                               'population', 'business_interest_type',
                               'age', 'behavior_interest', 'intelli_extend']]
    except:
        plan_info = plan_info[['channel_id', 'source_id', 
                               'ad_account_id', 'game_id', 'create_time',
                               'bid_type', 'cpa_bid', 'day_budget',
                               'scene_id', 'ocpx_action_type', 'roi_ratio',
                               'deep_conversion_type', 'deep_conversion_bid', 
                               'unit_type', 'show_mode', 'speed',
                               'auto_target', 'filter_converted_level', 'schedule_time',
                               'gender', 'region', 'platform_os',
                               'android_osv', 'network', 'device_brand_ids', 
                               'population', 'business_interest_type',
                               'age', 'intelli_extend']] if 'android_osv' in plan_info.columns else \
                    plan_info[['channel_id', 'source_id', 
                               'ad_account_id', 'game_id', 'create_time',
                               'bid_type', 'cpa_bid', 'day_budget',
                               'scene_id', 'ocpx_action_type', 'roi_ratio',
                               'deep_conversion_type', 'deep_conversion_bid', 
                               'unit_type', 'show_mode', 'speed',
                               'auto_target', 'filter_converted_level', 'schedule_time',
                               'gender', 'region', 'platform_os',
                               'ios_osv', 'network', 'device_brand_ids',
                               'population', 'business_interest_type',
                               'age', 'intelli_extend']]
    return plan_info.loc[:, ~plan_info.columns.duplicated()]


def get_creative_tt(game_ids, media_id):
    game_ids = list(map(lambda x: x['game_id'], game_ids))
    game_ids = [str(i) for i in game_ids]
    game_ids = ','.join(game_ids)

    conn = pymysql.connect(host=nacosServ['DB_SLAVE_TOUFANG_HOST'], port=int(nacosServ['DB_SLAVE_TOUFANG_PORT']), user=nacosServ['DB_SLAVE_TOUFANG_USERNAME'],
                           passwd=nacosServ['DB_SLAVE_TOUFANG_PASSWORD'], db=nacosServ['DB_SLAVE_TOUFANG_DATABASE'])
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
        /*手动查询*/ 
        SELECT
            b.chl_user_id AS channel_id,
            b.source_id,
            JSON_EXTRACT( a.creative_param, '$.ad_keywords' ) AS ad_keywords,
            JSON_EXTRACT( a.creative_param, '$.title_list' ) AS title_list,
            JSON_EXTRACT( a.creative_param, '$.third_industry_id' ) AS third_industry_id 
        FROM
            db_ptom.ptom_batch_ad_task a
            LEFT JOIN db_ptom.ptom_plan b ON a.plan_name = b.plan_name
        WHERE
            a.game_id IN ({}) 
            AND a.media_id = {}
            AND b.create_time >= date( NOW() - INTERVAL 720 HOUR )
    '''
    finalSql = sql.format(game_ids, media_id)
    cur.execute(finalSql)
    result = pd.read_sql(finalSql, conn)
    cur.close()
    conn.close()

    return result


def get_creative_gdt(game_ids, media_id):
    game_ids = list(map(lambda x: x['game_id'], game_ids))
    game_ids = [str(i) for i in game_ids]
    game_ids = ','.join(game_ids)

    conn = pymysql.connect(host=nacosServ['DB_SLAVE_TOUFANG_HOST'], port=int(nacosServ['DB_SLAVE_TOUFANG_PORT']), user=nacosServ['DB_SLAVE_TOUFANG_USERNAME'],
                           passwd=nacosServ['DB_SLAVE_TOUFANG_PASSWORD'], db=nacosServ['DB_SLAVE_TOUFANG_DATABASE'])
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
            a.game_id IN ({})
            AND a.media_id = {}
            AND b.create_time >= date( NOW() - INTERVAL 1440 HOUR )    
            AND b.image_id is not null

    '''
    finalSql = sql.format(game_ids, media_id)
    cur.execute(finalSql)
    result = pd.read_sql(finalSql, conn)
    cur.close()
    conn.close()
    return result


def get_creative_bd(game_id):
    conn = pymysql.connect(host=nacosServ['DB_SLAVE_TOUFANG_HOST'], port=int(nacosServ['DB_SLAVE_TOUFANG_PORT']), user=nacosServ['DB_SLAVE_TOUFANG_USERNAME'],
                           passwd=nacosServ['DB_SLAVE_TOUFANG_PASSWORD'], db=nacosServ['DB_SLAVE_TOUFANG_DATABASE'])
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
        /*手动查询*/ 
        SELECT
            pd.doc_content
        FROM
            db_ptom.ptom_document_creative_relation pdcr
            INNER JOIN db_ptom.ptom_plan_document pd ON pd.doc_id = pdcr.doc_id 
        WHERE
            FIND_IN_SET({}, pd.game_ids)
            AND pdcr.create_time >= '2020-01-01' and pdcr.media_id in (10,16,45,32)
            AND pd.game_ids IS NOT NULL 
            AND pd.game_ids != ''
            GROUP BY
                pdcr.doc_id
            ORDER BY
                count(*) DESC
                LIMIT 20
    '''
    cur.execute(sql.format(game_id))
    result = pd.read_sql(sql.format(game_id), conn)
    cur.close()
    conn.close()
    return result['doc_content'].values


def get_creative_ks(game_ids, media_id):
    game_ids = list(map(lambda x: x['game_id'], game_ids))
    game_ids = [str(i) for i in game_ids]
    game_ids = ','.join(game_ids)

    conn = pymysql.connect(host=nacosServ['DB_SLAVE_TOUFANG_HOST'], port=int(nacosServ['DB_SLAVE_TOUFANG_PORT']), user=nacosServ['DB_SLAVE_TOUFANG_USERNAME'],
                           passwd=nacosServ['DB_SLAVE_TOUFANG_PASSWORD'], db=nacosServ['DB_SLAVE_TOUFANG_DATABASE'])
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
        /*手动查询*/ 
        SELECT
            b.chl_user_id AS channel_id,
            b.source_id,
            a.creative_param 
        FROM
            db_ptom.ptom_batch_ad_task a
            LEFT JOIN db_ptom.ptom_plan b ON a.plan_name = b.plan_name 
        WHERE
            a.game_id IN ({}) 
            AND a.media_id = {}
            AND b.create_time >= date( NOW() - INTERVAL 1440 HOUR )
    '''
    finalSql = sql.format(game_ids, media_id)
    cur.execute(finalSql)
    result = pd.read_sql(finalSql, conn)
    cur.close()
    conn.close()
    return result


def get_creative_json_ks(creative_info):
    creative_info['creative_param'] = creative_info['creative_param'].apply(json.loads)
    temp = creative_info['creative_param'].apply(pd.Series)
    creative_info = pd.concat([creative_info, temp], axis=1)
    creative_info.drop('creative_param', axis=1, inplace=True)
    
    creative_info.drop(['gift', 'code', 'title', 'package_name', 'ideaName', 'creative_name', '_creative'],
                       axis=1, inplace=True)
    creative_info['adcreative_elements_array'] = creative_info['adcreative_elements_array'].apply(lambda x: x[0])
    creative_info['adcreative_elements'] = creative_info['adcreative_elements_array'].apply(
        lambda x: x['adcreative_elements'])
    creative_info['creative_material_type'] = creative_info['adcreative_elements_array'].apply(
        lambda x: x['creative_material_type'] if 'creative_material_type' in x.keys() else np.nan)
    creative_info.drop('adcreative_elements_array', axis=1, inplace=True)
    
    temp = creative_info['creative_details'].apply(pd.Series)
    creative_info = pd.concat([creative_info, temp], axis=1)
    creative_info.drop('creative_details', axis=1, inplace=True)
    temp = creative_info['adcreative_elements'].apply(pd.Series)
    creative_info = pd.concat([creative_info, temp], axis=1)
    creative_info.drop('adcreative_elements', axis=1, inplace=True)
    
    creative_info = creative_info[creative_info['photo_id'].notna()]
    creative_info['photo_id'] = creative_info['photo_id'].astype(int)
    creative_info['creative_material_type'] = creative_info['creative_material_type'].astype(int)
    
    creative_info = creative_info[['channel_id', 'source_id', 'smart_cover', 'asset_mining', 'creative_category', 'creative_tag',
                                   'creative_material_type', 'scene',  
                                   'photo_id', 'action_bar_text', 'description']]
    return creative_info


def get_now_plan_roi(game_ids, media_id):
    game_ids = list(map(lambda x: x['game_id'], game_ids))
    game_ids = [str(i) for i in game_ids]
    game_ids = ','.join(game_ids)
    conn = pymysql.connect(host=nacosServ['DB_SLAVE_FENXI_HOST'], port=int(nacosServ['DB_SLAVE_FENXI_PORT']), user=nacosServ['DB_SLAVE_FENXI_USERNAME'],
                           passwd=nacosServ['DB_SLAVE_FENXI_PASSWORD'])
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
            AND b.game_id IN ({})
            AND b.media_id = {}
            AND b.amount >= 200
            AND b.pay_role_user_num >= 1
            AND b.new_role_money >= 12
            AND (b.new_role_money / b.amount)>=0.005
    '''
    finalSql = sql.format(game_ids, media_id)
    cur.execute(finalSql)
    result_df = pd.read_sql(finalSql, conn)
    cur.close()
    conn.close()
    result_df['tdate'] = pd.to_datetime(result_df['tdate'])
    result_df = result_df.sort_values('tdate')
    result_df = result_df.drop_duplicates(['channel_id', 'source_id'], keep='first')
    return result_df


def get_field_media(game_ids, media_id):
    """ 分媒体要素 """
    if media_id == 10:
        fields = get_field_tt(game_ids, media_id)
    elif media_id == 16:
        fields = get_field_gdt(game_ids, media_id)
    elif media_id == 45:
        fields = get_field_bd(game_ids, media_id)
    elif media_id == 32:
        fields = get_field_ks(game_ids, media_id)
    # TODO：可拓展其他媒体
    return fields


def get_field_tt(game_ids, media_id):
    """ 头条要素 """
    plan_info = get_plan_info(game_ids, media_id)
    plan_info = get_plan_json_tt(plan_info)
    creative_info = get_creative_tt(game_ids, media_id)
    creative_info['title_list'] = creative_info['title_list'].fillna('[]')
    creative_info['ad_keywords'] = creative_info['ad_keywords'].fillna('[]')
    creative_info['title_list'] = creative_info['title_list'].apply(json.loads)
    creative_info['ad_keywords'] = creative_info['ad_keywords'].apply(json.loads)
    
    now_plan_roi = get_now_plan_roi(game_ids, media_id)
    now_plan_roi = pd.merge(now_plan_roi, creative_info, on=['channel_id', 'source_id'], how='left')
    
    df_create = pd.merge(plan_info, now_plan_roi, on=['channel_id', 'source_id'], how='inner')
    
    df_create['platform'] = df_create['platform'].astype(str)
    df_create['platform'] = df_create['platform'].map({"['ANDROID']": 1, "['IOS']": 2})
    df_create['platform'] = df_create['platform'].astype(int)
    
    df_create = df_create[df_create['platform'] == 1]
    df_create = df_create.dropna(subset=['ad_keywords','title_list','third_industry_id'],how='any')
    
    df_create['inventory_type'] = df_create['inventory_type'].astype(str)
    df_create['retargeting_tags_include'] = df_create['retargeting_tags_include'].astype(str)
    df_create['retargeting_tags_exclude'] = df_create['retargeting_tags_exclude'].astype(str)
    df_create['action_scene'] = df_create['action_scene'].astype(str)
    df_create['ad_keywords'] = df_create['ad_keywords'].astype(str)
    df_create['title_list'] = df_create['title_list'].astype(str)
    inventory_type_info = df_create['inventory_type'].value_counts().to_dict()
    deep_bid_type_info = df_create['deep_bid_type'].value_counts().to_dict()
    retargeting_tags_include_info = df_create['retargeting_tags_include'].value_counts().to_dict()
    retargeting_tags_exclude_info = df_create['retargeting_tags_exclude'].value_counts().to_dict()
    interest_action_mode_info = df_create['interest_action_mode'].value_counts().to_dict()
    action_scene_info = df_create['action_scene'].value_counts().to_dict()
    ad_keywords_info = df_create['ad_keywords'].value_counts().to_dict()
    title_list_info = df_create['title_list'].value_counts().to_dict()
    third_industry_id_info = df_create['third_industry_id'].value_counts().to_dict()
    hide_if_converted_info = df_create['hide_if_converted'].value_counts().to_dict()
    schedule_time_info = df_create['schedule_time'].value_counts().to_dict()
    return dict(zip(['inventory_type','deep_bid_type','retargeting_tags_include','retargeting_tags_exclude','interest_action_mode','action_scene','ad_keywords','title_list','third_industry_id','hide_if_converted','schedule_time'], 
                    [inventory_type_info,deep_bid_type_info,retargeting_tags_include_info,retargeting_tags_exclude_info,interest_action_mode_info,action_scene_info,ad_keywords_info,title_list_info,third_industry_id_info,hide_if_converted_info,schedule_time_info]))



def get_field_gdt(game_ids, media_id):
    """ 广点通要素 """
    plan_info = get_plan_info(game_ids, media_id)
    plan_info = get_plan_json_gdt(plan_info)
    creative_info = get_creative_gdt(game_ids, media_id)
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
    
    temp = creative_info['adcreative_elements'].apply(pd.Series)
    creative_info = pd.concat([creative_info, temp], axis=1)
    creative_info.drop('adcreative_elements', axis=1, inplace=True)

    if 'label' in creative_info.columns:
        pass
    else:
        creative_info['label'] = np.nan

    creative_info = creative_info[['channel_id', 'source_id', 'image_id', 'deep_link_url',
                                   'adcreative_template_id', 'page_spec', 'page_type', 'site_set', 'label',
                                   'promoted_object_id', 'promoted_object_type', 'automatic_site_enabled',
                                   'link_name_type', 'link_page_type', 'profile_id', 'link_page_spec',
                                   'description']]
    
    plan_info = pd.merge(plan_info.drop(['image_id'], axis=1), creative_info, on=['channel_id', 'source_id'],
                         how='inner')
    plan_info.dropna(subset=['image_id'], inplace=True)
    plan_info['image_id'] = plan_info['image_id'].astype(int)
    plan_info['adcreative_template_id'] = plan_info['adcreative_template_id'].astype(int)
    plan_info['profile_id'] = plan_info['profile_id'].apply(lambda x: x if x != x else str(int(x)))
    plan_info['create_time'] = pd.to_datetime(plan_info['create_time'])
    plan_info_current = plan_info[plan_info['create_time'] >= pd.datetime.now() - pd.DateOffset(30)]
    now_plan_roi = get_now_plan_roi(game_ids, media_id)
    df_create = pd.merge(plan_info_current, now_plan_roi, on=['channel_id', 'source_id'], how='inner')
    df_create['site_set'] = df_create['site_set'].fillna("[]")
    df_create['custom_audience'] = df_create['custom_audience'].fillna("[]")
    df_create['excluded_custom_audience'] = df_create['excluded_custom_audience'].fillna("[]")

    
    df_create['site_set'] = df_create['site_set'].astype(str)
    df_create['custom_audience'] = df_create['custom_audience'].astype(str)
    df_create['excluded_custom_audience'] = df_create['excluded_custom_audience'].astype(str)
    df_create['optimization_goal'] = df_create['optimization_goal'].astype(str)
    df_create['description'] = df_create['description'].astype(str)

    site_set_info = df_create['site_set'].value_counts().to_dict()
    site_set_info = {key:val for key, val in site_set_info.items() if len(key.split(',')) > 1}
    
    custom_audience_info = df_create['custom_audience'].value_counts().to_dict()
    excluded_custom_audience_info = df_create['excluded_custom_audience'].value_counts().to_dict()
    optimization_goal_info = df_create['optimization_goal'].value_counts().to_dict()
    description_info = df_create['description'].value_counts().to_dict()
    time_series_info = df_create['time_series'].value_counts().to_dict()
    # ad_keywords_info = df_create['ad_keywords'].value_counts().to_dict()
    # title_list_info = df_create['title_list'].value_counts().to_dict()
    # third_industry_id_info = df_create['third_industry_id'].value_counts().to_dict()
    return dict(zip(['site_set','custom_audience','excluded_custom_audience','optimization_goal','description','time_series'], 
                    [site_set_info,custom_audience_info,excluded_custom_audience_info,optimization_goal_info,description_info,time_series_info]))


def get_field_bd(game_ids, media_id):
    """ 百度要素 """
    plan_info = get_plan_info(game_ids, media_id)
    plan_info = get_plan_json_bd(plan_info)
    plan_info['create_time'] = pd.to_datetime(plan_info['create_time'])
    plan_info_current = plan_info[plan_info['create_time']>=pd.datetime.now() - pd.DateOffset(30)]
    
    now_plan_roi = get_now_plan_roi(game_ids, media_id)
    df_create = pd.merge(plan_info_current, now_plan_roi.drop(['ad_account_id'], axis=1), on=['channel_id','source_id'], how='inner')

    # !!! 需要统计分布的字段
    info_dic = []
    info_col = ['producttypes', 'ftypes','education', 'keywords', 'keywordsExtend', 'newInterests',
                'sex', 'iosVersion', 'androidVersion', 'excludeTrans', 'region',
                'device', 'age','transType', 'ocpcLevel', 'isSkipStageOne', 'payMode',
                'optimizeDeepTrans', 'transFrom', 'ocpcBid', 'deepOcpcBid',
                'deepTransType', 'roiRatio', 'useRoi']
    for col in info_col:
        df_create[col] = df_create[col].astype(str)
        info_dic += [df_create[col].value_counts().to_dict()]

    return dict(zip(info_col, info_dic))


def get_field_ks(game_ids, media_id):
    """ 快手要素 """
    plan_info = get_plan_info(game_ids, media_id)
    plan_info = get_plan_json_ks(plan_info)
    creative_info = get_creative_ks(game_ids, media_id)
    creative_info = get_creative_json_ks(creative_info)
    now_plan_roi = get_now_plan_roi(game_ids, media_id)

    now_plan_roi = pd.merge(now_plan_roi, creative_info, on=['channel_id', 'source_id'], how='left')
    df_create = pd.merge(plan_info, now_plan_roi, on=['channel_id', 'source_id'], how='inner')
    
    df_create = df_create[df_create['scene'].notna()]
    df_create['scene'] = df_create['scene'].astype(int)
    df_create['creative_category'] = df_create['creative_category'].astype(int)
    df_create['create_time'] = pd.to_datetime(df_create['create_time'])
    
    return df_create


# TODO =========分界线：计划部分===========

def get_plans_media(conf, **kwargs):
    """ 分媒体计划 """
    if kwargs['media_id'] == 10:
        plans = get_plans_tt(conf, **kwargs)
    elif kwargs['media_id'] == 16:
        plans = get_plans_gdt(conf, **kwargs)
    elif kwargs['media_id'] == 45:
        plans = get_plans_bd(conf, **kwargs)
    elif kwargs['media_id'] == 32:
        plans = get_plans_ks(conf, **kwargs)
    # TODO：可拓展其他媒体
    return plans

def get_plans_tt(conf, **kwargs):
    k = '{},{},{}'.format(kwargs['game_id'], kwargs['media_id'], kwargs['platform'])
    plan_result = pd.DataFrame()
    accou_lib = kwargs['account_ids']
    image_lib = kwargs['image_lib']
    accou_counts = np.array([0] * len(accou_lib), dtype=int)  # 账号计数
    image_counts = np.array([0] * len(image_lib), dtype=int)  # 素材计数

    # !!! 搭配选择
    for _ in range(kwargs['nums']):
        # 1、账号量是素材量的倍数
        # 2、素材刚好分配过1轮次
        if len(set(image_counts)) == 1 and np.min(image_counts) >= 1:
            accou_lib = sorted(accou_lib, key=lambda x: random.random())

        accou_id = accou_lib[np.argmin(accou_counts)]
        image_id = image_lib[np.argmin(image_counts)]
        
        temp = pd.DataFrame({'ad_account_id': [accou_id], 'image_id': [image_id]})
        plan_result = plan_result.append(temp)
        accou_counts[accou_counts.argsort()[:1]] += 1
        image_counts[image_counts.argsort()[:1]] += 1
    
    # !!! 基础参数
    plan_result['game_id'] = kwargs['game_id']
    plan_result['platform'] = kwargs['platform']
    plan_result['budget'] = conf[k]['budget']
    plan_result['android_osv'] = "None"
    plan_result['ios_osv'] = "None"
    plan_result['inventory_type'] = plan_result.apply(lambda x:np.random.choice(list(kwargs['field_sprea']['inventory_type'].keys()), p=np.divide(list(kwargs['field_sprea']['inventory_type'].values()), sum(list(kwargs['field_sprea']['inventory_type'].values()))).tolist()), axis=1)
    plan_result['delivery_range'] = plan_result.apply(lambda x:'UNION' if x.inventory_type == "['INVENTORY_UNION_SLOT']" else 'DEFAULT', axis=1)
    plan_result['flow_control_mode'] = "FLOW_CONTROL_MODE_FAST"
    plan_result['district'] = 'CITY'
    plan_result['plan_auto_task_id'] = conf[k]['plan_auto_task_id']
    plan_result['op_id'] = conf[k]['op_id']
    plan_result['web_url'] = conf[k]['web_url']
    plan_result['schedule_time'] = plan_result.apply(lambda x:np.random.choice(list(kwargs['field_sprea']['schedule_time'].keys()), p=np.divide(list(kwargs['field_sprea']['schedule_time'].values()), sum(list(kwargs['field_sprea']['schedule_time'].values()))).tolist()), axis=1)

    # !!! 定向池
    plan_result['retargeting_tags_include'] = plan_result.apply(lambda x:np.random.choice(list(kwargs['field_sprea']['retargeting_tags_include'].keys()), p=np.divide(list(kwargs['field_sprea']['retargeting_tags_include'].values()), sum(list(kwargs['field_sprea']['retargeting_tags_include'].values()))).tolist()), axis=1)
    plan_result['retargeting_tags_exclude'] = plan_result.apply(lambda x:np.random.choice(list(kwargs['field_sprea']['retargeting_tags_exclude'].keys()), p=np.divide(list(kwargs['field_sprea']['retargeting_tags_exclude'].values()), sum(list(kwargs['field_sprea']['retargeting_tags_exclude'].values()))).tolist()), axis=1)
    plan_result['retargeting_tags_include'] = "[]"  # 人群包
    plan_result['retargeting_tags_exclude'] = "[]"  # 人群包

    plan_result['city'] = plan_result.apply(lambda x:"[]", axis=1)
    plan_result['location_type'] = "CURRENT"
    plan_result['gender'] = plan_result.apply(lambda x:np.random.choice(["GENDER_MALE","None"],p=[0.9,0.1]), axis=1)
    plan_result['age'] = plan_result.apply(lambda x:"['AGE_BETWEEN_24_30', 'AGE_BETWEEN_31_40', 'AGE_BETWEEN_41_49']", axis=1)
    plan_result['ac'] = "[]"
    plan_result['launch_price'] = plan_result.apply(lambda x:np.random.choice(["[]", "[2000, 11000]"],p=[0.1,0.9]), axis=1)
    plan_result['auto_extend_enabled'] = 0
    plan_result['hide_if_exists'] = 0
    plan_result['hide_if_converted'] =  plan_result.apply(lambda x:np.random.choice(list(kwargs['field_sprea']['hide_if_converted'].keys()), p=np.divide(list(kwargs['field_sprea']['hide_if_converted'].values()), sum(list(kwargs['field_sprea']['hide_if_converted'].values()))).tolist()), axis=1)

    # !!! 兴趣
    plan_result['interest_action_mode'] = plan_result.apply(lambda x:np.random.choice(list(kwargs['field_sprea']['interest_action_mode'].keys()), p=np.divide(list(kwargs['field_sprea']['interest_action_mode'].values()), sum(list(kwargs['field_sprea']['interest_action_mode'].values()))).tolist()), axis=1)
    plan_result['action_scene'] = plan_result.apply(lambda x:np.random.choice(list(kwargs['field_sprea']['action_scene'].keys()), p=np.divide(list(kwargs['field_sprea']['action_scene'].values()), sum(list(kwargs['field_sprea']['action_scene'].values()))).tolist()), axis=1)
    plan_result['action_days'] = np.nan
    plan_result['action_categories'] = np.nan   # 行为定向
    plan_result['interest_categories'] = np.nan # 兴趣定向

    # !!! 创意
    plan_result['ad_keywords'] = plan_result.apply(lambda x:np.random.choice(list(kwargs['field_sprea']['ad_keywords'].keys()), p=np.divide(list(kwargs['field_sprea']['ad_keywords'].values()), sum(list(kwargs['field_sprea']['ad_keywords'].values()))).tolist()), axis=1)
    plan_result['title_list'] = plan_result.apply(lambda x:np.random.choice(list(kwargs['field_sprea']['title_list'].keys()), p=np.divide(list(kwargs['field_sprea']['title_list'].values()), sum(list(kwargs['field_sprea']['title_list'].values()))).tolist()), axis=1)
    plan_result['third_industry_id'] = plan_result.apply(lambda x:np.random.choice(list(kwargs['field_sprea']['third_industry_id'].keys()), p=np.divide(list(kwargs['field_sprea']['third_industry_id'].values()), sum(list(kwargs['field_sprea']['third_industry_id'].values()))).tolist()), axis=1)
    
    # !!! 出价
    plan_result['deep_bid_type'] = plan_result.apply(lambda x:np.random.choice(list(kwargs['field_sprea']['deep_bid_type'].keys()), p=np.divide(list(kwargs['field_sprea']['deep_bid_type'].values()), sum(list(kwargs['field_sprea']['deep_bid_type'].values()))).tolist()), axis=1)
    plan_result['smart_bid_type'] = plan_result['deep_bid_type'].apply(lambda x:'SMART_BID_CUSTOM' if x == 'ROI_COEFFICIENT' else 'SMART_BID_NO_BID')
    plan_result['roi_goal'] = plan_result.apply(lambda x:np.nan if x.deep_bid_type !='ROI_COEFFICIENT' else conf[k]['roi_goal'], axis=1)
    plan_result['adjust_cpa'] = 0
    plan_result['cpa_bid'] = plan_result.apply(lambda x:random.randint(conf[k]['cpa_bid'][0], conf[k]['cpa_bid'][1]), axis=1)
    plan_result['convertIndex'] = plan_result['deep_bid_type'].apply(lambda x:23 if x == 'BID_PER_ACTION' else 24 if x == 'ROI_COEFFICIENT' else np.nan)

    # !!! 其他
    plan_result['inventory_type'] = plan_result['inventory_type'].apply(lambda x: "[]" if x ==
                    "['INVENTORY_UNION_SLOT', 'INVENTORY_AWEME_FEED', 'INVENTORY_FEED', 'INVENTORY_UNION_SPLASH_SLOT', "
                    "'INVENTORY_VIDEO_FEED', 'INVENTORY_HOTSOON_FEED', 'INVENTORY_TOMATO_NOVEL']" else x)
    plan_result['smart_bid_type'] = plan_result.apply(lambda x: 'SMART_BID_CUSTOM' if 'INVENTORY_UNION_SLOT' in x.inventory_type else x.smart_bid_type, axis=1)
    plan_result['flag'] = plan_result['smart_bid_type'].apply(lambda x:'CESHI_NB' if x == 'SMART_BID_NO_BID' else 'CESHI')

    # !!! 最后调整
    plan_result['inventory_type'] = plan_result['inventory_type'].apply(ast.literal_eval)
    plan_result['retargeting_tags_include'] = plan_result['retargeting_tags_include'].apply(ast.literal_eval)
    plan_result['retargeting_tags_exclude'] = plan_result['retargeting_tags_exclude'].apply(ast.literal_eval)
    plan_result['city'] = plan_result['city'].apply(ast.literal_eval)
    plan_result['age'] = plan_result['age'].apply(ast.literal_eval)
    plan_result['ac'] = plan_result['ac'].apply(ast.literal_eval)
    plan_result['launch_price'] = plan_result['launch_price'].apply(ast.literal_eval)
    # plan_result['action_scene'] = plan_result['action_scene'].apply(ast.literal_eval)
    plan_result['ad_keywords'] = plan_result['ad_keywords'].apply(ast.literal_eval)
    plan_result['title_list'] = plan_result['title_list'].apply(ast.literal_eval)
    # plan_result['operation'] = 'disable'

    return plan_result


def get_plans_gdt(conf, **kwargs):
    k = '{},{},{}'.format(kwargs['game_id'], kwargs['media_id'], kwargs['platform'])
    plan_result = pd.DataFrame()
    accou_lib = kwargs['account_ids']
    image_lib = kwargs['image_lib']
    accou_counts = np.array([0] * len(accou_lib), dtype=int)  # 账号计数
    image_counts = np.array([0] * len(image_lib), dtype=int)  # 素材计数

    # !!! 搭配选择
    for _ in range(kwargs['nums']):
        # 1、账号量是素材量的倍数
        # 2、素材刚好分配过1轮次
        if len(set(image_counts)) == 1 and np.min(image_counts) >= 1: 
            accou_lib = sorted(accou_lib, key=lambda x: random.random())
        
        accou_id = accou_lib[np.argmin(accou_counts)]
        image_id = image_lib[np.argmin(image_counts)]
        temp = pd.DataFrame({'ad_account_id': [accou_id], 'image_id': [image_id]})
        plan_result = plan_result.append(temp)
        accou_counts[accou_counts.argsort()[:1]] += 1
        image_counts[image_counts.argsort()[:1]] += 1

    # !!! 基础参数
    plan_result['site_set'] = plan_result.apply(lambda x:np.random.choice(list(kwargs['field_sprea']['site_set'].keys()), p=np.divide(list(kwargs['field_sprea']['site_set'].values()), sum(list(kwargs['field_sprea']['site_set'].values()))).tolist()), axis=1)
    plan_result['flag'] = plan_result['site_set'].apply(lambda x: 'CESHI_PYQ' if x == "['SITE_SET_MOMENTS']"
                                            else 'CESHI_YLH' if x == "['SITE_SET_MOBILE_UNION']"
                                            else 'CESHI_自动' if x == "[]"
                                            else 'CESHI_XQXS')
    plan_result['automatic_site_enabled'] = plan_result['site_set'].apply(lambda x: True if x=="[]" else False)
    plan_result['game_id'] = kwargs['game_id']
    plan_result['platform'] = kwargs['platform']
    plan_result['daily_budget'] = conf[k]['daily_budget']
    plan_result['budget'] = 0
    plan_result['expand_enabled'] = plan_result.apply(lambda x:np.random.choice([True,False],p=[0.2,0.8]), axis=1)
    plan_result['expand_targeting'] = plan_result.apply(lambda x: "[]" if x.expand_enabled==True else "['age']", axis=1)
    plan_result['expand_targeting'] = plan_result['expand_targeting'].apply(ast.literal_eval)
    plan_result['promoted_object_type'] = conf[k]['promoted_object_type']
    plan_result['promoted_object_id'] = conf[k]['promoted_object_id']
    plan_result['plan_auto_task_id'] = conf[k]['plan_auto_task_id']
    plan_result['op_id'] = conf[k]['op_id']
    plan_result['game_name'] = conf[k]['game_name']
    plan_result['link_name_type'] = plan_result['site_set'].apply(lambda x: 'DOWNLOAD_APP' if x == "['SITE_SET_MOMENTS']" else np.nan)
    plan_result['time_series'] = plan_result.apply(lambda x:np.random.choice(list(kwargs['field_sprea']['time_series'].keys()), p=np.divide(list(kwargs['field_sprea']['time_series'].values()), sum(list(kwargs['field_sprea']['time_series'].values()))).tolist()), axis=1)
    plan_result['ad_account_id'] = plan_result['ad_account_id'].map(int)

    # !!! 定向
    plan_result['device_price'] = plan_result.apply(lambda x:np.random.choice(["['PRICE_1500_2500', 'PRICE_2500_3500', 'PRICE_3500_4500', 'PRICE_4500_MORE']",
                                                                               "[]"],p=[0.5,0.5]), axis=1)
    plan_result['device_price'] = plan_result['device_price'].apply(ast.literal_eval)
    plan_result['app_install_status'] = plan_result.apply(lambda x:np.random.choice([np.nan, ['NOT_INSTALLED']],p=[0.01,0.99]), axis=1)

    plan_result['gender'] = plan_result.apply(lambda x:np.random.choice([np.nan, ['MALE']],p=[0.1,0.9]), axis=1)
    plan_result['game_consumption_level'] = plan_result.apply(lambda x:np.random.choice([np.nan, ['HIGH'],['HIGH', 'NORMAL']],p=[0.01,0.9,0.09]), axis=1)
    plan_result['age'] = plan_result.apply(lambda x: [{'min': 23, 'max': 58}], axis=1)
    plan_result['custom_audience'] = plan_result.apply(lambda x:np.random.choice(list(kwargs['field_sprea']['custom_audience'].keys()), p=np.divide(list(kwargs['field_sprea']['custom_audience'].values()), sum(list(kwargs['field_sprea']['custom_audience'].values()))).tolist()), axis=1)
    plan_result['custom_audience'] = plan_result['custom_audience'].apply(ast.literal_eval)
    plan_result['custom_audience'] = plan_result['custom_audience'].apply(lambda x: np.nan if x==[] else x)
    plan_result['excluded_custom_audience'] = plan_result.apply(lambda x:np.random.choice(list(kwargs['field_sprea']['excluded_custom_audience'].keys()), p=np.divide(list(kwargs['field_sprea']['excluded_custom_audience'].values()), sum(list(kwargs['field_sprea']['excluded_custom_audience'].values()))).tolist()), axis=1)
    plan_result['excluded_custom_audience'] = plan_result['excluded_custom_audience'].apply(ast.literal_eval)
    plan_result['excluded_custom_audience'] = plan_result['excluded_custom_audience'].apply(lambda x: np.nan)
    plan_result['network_type'] = np.nan
    plan_result['conversion_behavior_list'] = plan_result.apply(lambda x:np.random.choice([np.nan, ['OPTIMIZATIONGOAL_APP_ACTIVATE']],p=[0.7,0.3]), axis=1)
    plan_result['excluded_dimension'] = plan_result['conversion_behavior_list'].apply(lambda x: "EXCLUDED_DIMENSION_APP" if x == x else np.nan)
    plan_result['regions'] = np.nan
    plan_result['location_types'] = np.nan
    plan_result['intention_targeting_tags'] = np.nan
    plan_result['interest_category_id_list'] = np.nan
    plan_result['interest_keyword_list'] = np.nan
    plan_result['behavior_category_id_list'] = np.nan
    plan_result['behavior_intensity'] = np.nan
    plan_result['behavior_keyword_list'] = np.nan
    plan_result['behavior_scene'] = np.nan
    plan_result['behavior_time_window'] = np.nan

    # !!! 创意
    plan_result['deep_link_url'] = np.nan
    plan_result['adcreative_template_id'] = plan_result['site_set'].apply(lambda x:721 if (x=="['SITE_SET_MOMENTS']")|(('SITE_SET_WECHAT' in x)&('SITE_SET_TENCENT_NEWS' in x)) else 720)
    plan_result['adcreative_elements'] = plan_result['image_id'].apply(lambda x:{'image': 145702, 'description': '小时候玩的帝王终于出手游了！开局5个农民，建立你的王国！', 'video': 145702, 'button_text': '立即下载', 'brand': {'brand_name': conf[k]['brand_name'], 'brand_img': conf[k]['brand_img']}})

    plan_result = plan_result.reset_index(drop=True)
    for i in range(plan_result.shape[0]):
        a = plan_result.loc[i,'adcreative_elements'].copy()
        b = plan_result.loc[i,'adcreative_template_id']
        if b == 721:
            image_detail_id = image_size(plan_result.loc[i,'image_id'], (720, 1280))
        elif b == 720:
            image_detail_id = image_size(plan_result.loc[i,'image_id'], (1280, 720))
        a['video'] = image_detail_id
        a['image'] = image_detail_id
        a['description'] = np.random.choice(list(kwargs['field_sprea']['description'].keys()), p=np.divide(list(kwargs['field_sprea']['description'].values()), sum(list(kwargs['field_sprea']['description'].values()))).tolist())
        plan_result.loc[i,'adcreative_elements'] = str(a)

    plan_result['page_spec'] = plan_result['ad_account_id'].map(conf[k]['page_id_dict'])

    plan_result['ad_account_id'] = plan_result['ad_account_id'].map(str)
    plan_result_1 = plan_result[plan_result['site_set'] == "['SITE_SET_MOMENTS']"]
    plan_result_2 = plan_result[plan_result['site_set'] != "['SITE_SET_MOMENTS']"]

    # profile_id_dict = {'11917': '', '11918': '', '11538': ''}
    # plan_result_1['profile_id'] = plan_result_1['ad_account_id'].map(profile_id_dict)

    plan_result = plan_result_1.append(plan_result_2)
    plan_result = plan_result.reset_index(drop=True)

    plan_result['page_type'] = plan_result.apply(lambda x: conf[k]['page_type'], axis=1)
    plan_result['link_name_type'] = np.nan
    plan_result['link_page_type'] = np.nan

    # !!! 出价
    plan_result['optimization_goal'] = plan_result.apply(lambda x:np.random.choice(list(kwargs['field_sprea']['optimization_goal'].keys()), p=np.divide(list(kwargs['field_sprea']['optimization_goal'].values()), sum(list(kwargs['field_sprea']['optimization_goal'].values()))).tolist()), axis=1)
    plan_result['deep_conversion_type'] = plan_result['optimization_goal'].apply(lambda x: 'DEEP_CONVERSION_WORTH' if x == 'OPTIMIZATIONGOAL_APP_ACTIVATE' else np.nan)
    plan_result['deep_conversion_behavior_spec'] = np.nan
    plan_result['deep_conversion_worth_spec'] = plan_result['optimization_goal'].apply(lambda x: {'goal': 'GOAL_1DAY_PURCHASE_ROAS',
                                                            'expected_roi': conf[k]['expected_roi']} if x == 'OPTIMIZATIONGOAL_APP_ACTIVATE' else np.nan)
    plan_result['bid_amount'] = plan_result['optimization_goal'].apply(
                                        lambda x: random.choice(conf[k]['bid_amount']) if x == 'OPTIMIZATIONGOAL_APP_ACTIVATE'
                                        else (random.choice(conf[k]['bid_amount']) if x == 'OPTIMIZATIONGOAL_APP_PURCHASE'
                                            else (random.randint(310000, 320000))))
    plan_result['bid_strategy'] = plan_result.apply(lambda x:np.random.choice(["BID_STRATEGY_AVERAGE_COST","BID_STRATEGY_TARGET_COST"],p=[0.95,0.05]), axis=1)

    # !!! 其他
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
                                        "age", 'custom_audience', 'excluded_custom_audience', "network_type",
                                        "excluded_converted_audience", "geo_location",
                                        "intention", "interest", "behavior"]].to_json()))
    plan_result['targeting'] = ad_info

    # !!! 最后调整
    plan_result.drop(["device_price", "app_install_status", "gender", "game_consumption_level", "age",
                    'custom_audience', 'excluded_custom_audience', "network_type",
                    "excluded_converted_audience", "geo_location",
                    "intention", "interest", "behavior"], axis=1, inplace=True)
    plan_result['site_set'] = plan_result['site_set'].apply(ast.literal_eval)
    # plan_result['operation'] = 'disable'
    
    return plan_result


def get_plans_bd(conf, **kwargs):
    k = '{},{},{}'.format(kwargs['game_id'], kwargs['media_id'], kwargs['platform'])
    plan_result = pd.DataFrame()
    accou_lib = kwargs['account_ids']
    image_lib = kwargs['image_lib']
    accou_counts = np.array([0] * len(accou_lib), dtype=int)  # 账号计数
    image_counts = np.array([0] * len(image_lib), dtype=int)  # 素材计数

    # !!! 搭配选择
    for _ in range(kwargs['nums']):
        # 1、账号量是素材量的倍数
        # 2、素材刚好分配过1轮次
        if len(set(image_counts)) == 1 and np.min(image_counts) >= 1:
            accou_lib = sorted(accou_lib, key=lambda x: random.random())
        
        accou_id = accou_lib[np.argmin(accou_counts)]
        image_id = image_lib[np.argmin(image_counts)]
        temp = pd.DataFrame({'ad_account_id': [accou_id], 'image_id': [image_id]})
        plan_result = plan_result.append(temp)
        accou_counts[accou_counts.argsort()[:1]] += 1
        image_counts[image_counts.argsort()[:1]] += 1
    
    # !!! 基础参数
    plan_result['game_id'] = kwargs['game_id']
    plan_result['autoExpansion'] = 0
    plan_result['pause'] = plan_result.apply(lambda x:False, axis=1)
    plan_result['brand'] = plan_result.apply(lambda x:conf[k]['brand'] ,axis=1)
    plan_result['deepTransTypes'] = plan_result.apply(lambda x:conf[k]['deepTransTypes'], axis=1)

    plan_result = plan_result.reset_index(drop=True)
    plan_result['subject'] = conf[k]['subject']
    plan_result['plan_auto_task_id'] = conf[k]['plan_auto_task_id']
    plan_result['game_name'] = conf[k]['game_name']
    plan_result['op_id'] = conf[k]['op_id']
    plan_result['flag'] = "BD"
    
    # !!! 定向 $
    for col in ['producttypes', 'ftypes', 'education', 'keywordsExtend', 'newInterests', 'iosVersion', 'androidVersion', 'excludeTrans', 'region']:
        try:
            plan_result[col] = plan_result.apply(lambda x:np.random.choice(list(kwargs['field_sprea'][col].keys()), p=np.divide(list(kwargs['field_sprea'][col].values()), sum(list(kwargs['field_sprea'][col].values()))).tolist()), axis=1)
        except:
            plan_result[col] = plan_result.apply(lambda x:conf[k][col] ,axis=1)
    
    plan_result['device'] = conf[k]['device']
    plan_result['age'] = conf[k]['age']
    plan_result['sex'] = conf[k]['sex']
    plan_result['keywords'] = plan_result.apply(lambda x:np.random.choice(conf[k]['keywords']), axis=1)

    # !!! 出价
    plan_result['transFrom'] = conf[k]['transFrom']
    plan_result['payMode'] = conf[k]['payMode']
    plan_result['ocpcLevel'] = conf[k]['ocpcLevel']
    plan_result['isSkipStageOne'] = conf[k]['isSkipStageOne']

    plan_result['optimizeDeepTrans'] = plan_result.apply(lambda x:np.random.choice([False, True], p=conf[k]['optimizeDeepTrans']), axis=1)
    plan_result['transType'] = plan_result['optimizeDeepTrans'].apply(lambda x:26 if x==False else 4)
    plan_result['ocpcBid'] = plan_result['optimizeDeepTrans'].apply(lambda x:random.randint(conf[k]['ocpcBid_F'][0], conf[k]['ocpcBid_F'][1]) if x==False else random.randint(conf[k]['ocpcBid_T'][0], conf[k]['ocpcBid_T'][1]))
    plan_result['deepOcpcBid'] = plan_result['optimizeDeepTrans'].apply(lambda x:random.randint(conf[k]['deepOcpcBid'][0], conf[k]['deepOcpcBid'][1]) if x==True else np.nan)
    plan_result['useRoi'] = plan_result['optimizeDeepTrans'].apply(lambda x:True if x==False else np.nan)
    plan_result['roiRatio'] = plan_result['optimizeDeepTrans'].apply(lambda x:conf[k]['roiRatio'] if x==False else np.nan)
    plan_result['deepTransType'] = plan_result['optimizeDeepTrans'].apply(lambda x:np.nan if x==False else 26)

    plan_result['budget'] = plan_result.apply(lambda x:conf[k]['budget'] ,axis=1) # !!!

    # !!! 其他
    ad_info = []
    for i in range(plan_result.shape[0]):
        ad_info.append(json.loads(plan_result.loc[i, ['autoExpansion','education', 'keywords',
           'keywordsExtend', 'newInterests', 'sex', 'iosVersion', 'androidVersion',
           'excludeTrans', 'region', 'device', 'age']].to_json()))
    plan_result['audience'] = ad_info
    plan_result.drop(['autoExpansion','education', 'keywords',
           'keywordsExtend', 'newInterests', 'sex', 'iosVersion', 'androidVersion',
           'excludeTrans', 'region', 'device', 'age'], axis=1, inplace=True)
    ad_info = []
    for i in range(plan_result.shape[0]):
        ad_info.append(json.loads(plan_result.loc[i, [ 'transType', 'ocpcLevel',
           'isSkipStageOne', 'payMode', 'optimizeDeepTrans', 'transFrom',
           'ocpcBid', 'deepOcpcBid', 'deepTransType', 'roiRatio', 'useRoi']].to_json()))
    plan_result['ocpc'] = ad_info
    plan_result.drop([ 'transType', 'ocpcLevel',
           'isSkipStageOne', 'payMode', 'optimizeDeepTrans', 'transFrom',
           'ocpcBid', 'deepOcpcBid', 'deepTransType', 'roiRatio', 'useRoi'], axis=1, inplace=True)

    plan_result['schedule'] = plan_result['ocpc'].apply(lambda x: conf[k]['schedule'])

    document = get_creative_bd(kwargs['game_id'])
    plan_result['bgtctltype'] = conf[k]['bgtctltype']
    plan_result['materialstyle'] = conf[k]['materialstyle']
    plan_result['subtitle'] = conf[k]['subtitle']
    plan_result['userPortrait'] = conf[k]['userPortrait']
    plan_result['titles'] = plan_result['subtitle'].apply(lambda x: np.random.choice(document, 3))
    plan_result['image_ids'] = plan_result['image_id'].apply(lambda x: [x])
    plan_result.drop('image_id', axis=1, inplace=True)
    # plan_result['operation'] = 'disable'

    return plan_result


def get_plans_ks(conf, **kwargs):
    k = '{},{},{}'.format(kwargs['game_id'], kwargs['media_id'], kwargs['platform'])
    plan_result = pd.DataFrame()
    accou_lib = kwargs['account_ids']
    image_lib = kwargs['image_lib']
    accou_counts = np.array([0] * len(accou_lib), dtype=int)  # 账号计数
    image_counts = np.array([0] * len(image_lib), dtype=int)  # 素材计数

    # !!! 搭配选择
    for _ in range(kwargs['nums']):
        # 1、账号量是素材量的倍数
        # 2、素材刚好分配过1轮次
        if len(set(image_counts)) == 1 and np.min(image_counts) >= 1:
            accou_lib = sorted(accou_lib, key=lambda x: random.random())
        
        accou_id = accou_lib[np.argmin(accou_counts)]
        image_id = image_lib[np.argmin(image_counts)]
        temp = pd.DataFrame({'ad_account_id': [accou_id], 'image_id': [image_id]})
        plan_result = plan_result.append(temp)
        accou_counts[accou_counts.argsort()[:1]] += 1
        image_counts[image_counts.argsort()[:1]] += 1
    
    # !!! 基础参数
    # TODO:
    

def image_size(image_id,size=(720, 1280)):
    """ 匹配：素材-版位 """
    conn = pymysql.connect(host=nacosServ['DB_SLAVE_TOUFANG_HOST'], port=int(nacosServ['DB_SLAVE_TOUFANG_PORT']), user=nacosServ['DB_SLAVE_TOUFANG_USERNAME'],
                           passwd=nacosServ['DB_SLAVE_TOUFANG_PASSWORD'], db=nacosServ['DB_SLAVE_TOUFANG_DATABASE'])
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

    # 匹配
    new_id = new_ids[(new_ids['width']==size[0]) & (new_ids['height']==size[1])]['id']
    assert len(new_id) > 0, "主素材{}找不到对应的{}尺寸".format(image_id, (size[0], size[1]))
    return new_id.values[0]


# TODO =========分界线：请求===========

def get_ad_create(plan_result, media_id):
    ad_info = []
    for i in range(plan_result.shape[0]):
        ad_info.append(json.loads(plan_result.iloc[i].to_json()))
    open_api_url_prefix = "https://ptom.caohua.com/"
    # open_api_url_prefix = "https://ptom-pre.caohua.com/"
    uri = "model/generationPlanBatchTask"
    url = open_api_url_prefix + uri
    params = {
        "secretkey": "abc2018!@**@888",
        "mediaId": media_id
    }
    rsp = requests.post(url, json=ad_info, params=params)
    rsp_data = rsp.json()
    return rsp_data
