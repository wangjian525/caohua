# -*- coding:utf-8 -*-
"""
   File Name：     ptom_utils.py
   Description :   计划监控相关：采集数据的工具脚本
   Author :        royce.mao
   date：          2021/7/28 11:10
"""

from impala.dbapi import connect
from impala.util import as_pandas
import pandas as pd
import numpy as np
import requests
import pymysql
import json
import datetime
import time

from config import cur_config as cfg


def getRealData(conn1, conn2, action, media_id):
    """ 初始采集：模型传参数据提取 """
    sql = '''
        select A.*,
                IFNULL(B.two_day_create_retain,0) two_day_create_retain,
                IFNULL(F.new_role_money,0) new_role_money,
                (case when IFNULL(A.amount,0)=0 then 0 else IFNULL(IFNULL(F.new_role_money,0),0)*1.0/IFNULL(A.amount,0) end) as new_role_rate,
                IFNULL(F.pay_role_user_num,0) pay_role_user_num,
                IFNULL(IFNULL(A.amount,0)/IFNULL(F.pay_role_user_num,0),0) as new_pay_cost
                ,(case when IFNULL(A.create_role_num,0)=0 then 0 else IFNULL(IFNULL(F.pay_role_user_num,0),0)*1.0/IFNULL(A.create_role_num,0) end) as pay_role_user_rate
            from (
                SELECT
                pp.plan_id, pp.platform,
                pp.plan_name as plan_name, pp.launch_time,
                pp.media_id,
                pp.image_id,
                pp.learning_type,
                pp.learning_time_dt,
                pp.learning_time_hr,
                c.*
                from (
                SELECT a.game_id as product_id,a.channel_id chl_user_id,a.source_id,
                    IFNULL(sum(a.amount),0) as amount,
                    IFNULL(sum(create_role_num),0) as create_role_num,
                    (case when IFNULL(sum(a.create_role_num),0)=0 then IFNULL(sum(a.amount),0) else IFNULL(sum(a.amount),0)*1.0/IFNULL(sum(a.create_role_num),0) end) as create_role_cost
                from db_stdata.st_lauch_report a where a.tdate>=date( NOW() - INTERVAL 72 HOUR ) and a.tdate_type='day' and a.amount > 0 and a.media_id={}
                group by a.game_id,a.channel_id,a.source_id) c
                    
                        inner join db_data_ptom.ptom_plan pp on (c.product_id=pp.game_id and c.chl_user_id=pp.chl_user_id and c.source_id=pp.source_id)
                    where pp.plan_name LIKE '%CESHI%'
                group by pp.plan_id
            ) A LEFT JOIN
            (
            select game_id,channel_id,source_id,IFNULL(sum(m.new_role_money),0) as new_role_money,
            IFNULL(sum(m.pay_role_user_num),0) as pay_role_user_num
            from (
            select a.game_id,a.channel_id,a.source_id,IFNULL(sum(a.new_role_money),0) as new_role_money,
            IFNULL(sum(a.pay_role_user_num),0) as pay_role_user_num
            from db_stdata.st_lauch_report a
            inner join db_data_ptom.ptom_plan pp on (a.game_id=pp.game_id and a.channel_id=pp.chl_user_id and a.source_id=pp.source_id)
        
            where a.tdate_type='day' AND a.media_id={} AND pp.plan_name LIKE '%CESHI%'
            # and a.game_id=-1
            group by a.game_id,a.channel_id,a.source_id
            having (new_role_money>0 or pay_role_user_num>0)
            union ALL
            SELECT c.game_id,c.channel_id,c.source_id,sum(c.create_role_money) new_role_money,
            IFNULL(sum(c.pay_role_user_num),0) as pay_role_user_num
            from db_stdata.st_game_days c where c.report_days = 3 and c.tdate = date( NOW() - INTERVAL 24 HOUR ) and c.tdate_type='day' and c.query_type=13 and c.media_id={}
            group by c.game_id,c.channel_id,c.source_id
            having (new_role_money>0 or pay_role_user_num>0)
            ) m
            group by game_id,channel_id,source_id
            ) F ON  A.product_id=F.game_id AND A.chl_user_id=F.channel_id AND A.source_id=F.source_id
            LEFT JOIN
            (
            select game_id,channel_id,source_id,
            sum(bb.two_day_create_retain) as two_day_create_retain
                from (
                select game_id,channel_id,source_id,
                (case when retain_date=2 then create_retain_rate end) as two_day_create_retain
                from (
                    select m.game_id,m.channel_id,m.source_id,m.retain_date,
                    (case when IFNULL(sum(m.create_role_num),0)=0 then 0 else IFNULL(sum(m.create_role_retain_num),0)/sum(m.create_role_num) end) as create_retain_rate
                    from db_stdata.st_game_retain m
                    INNER JOIN db_data_ptom.ptom_plan pp on m.game_id=pp.game_id and m.channel_id=pp.chl_user_id and m.source_id=pp.source_id
                    where m.tdate>=date( NOW() - INTERVAL 72 HOUR )  and m.tdate_type='day' and m.query_type=19 and m.media_id={} and m.server_id=-1 and m.retain_date in (2)
                    and pp.plan_name LIKE '%CESHI%'
                    group by m.game_id,m.channel_id,m.source_id,m.retain_date
                ) cc
                )bb
                group by bb.game_id,bb.channel_id,bb.source_id
            ) B ON A.product_id=B.game_id AND A.chl_user_id=B.channel_id AND A.source_id=B.source_id
    '''
    result = pd.read_sql(sql.format(media_id, media_id, media_id, media_id), conn1)
    result['create_role_roi'] = result['new_role_money'] / result['amount']
    result.dropna(subset=['image_id'], inplace=True)
    result['image_id'] = result['image_id'].astype(int)
    result.drop(["learning_type","learning_time_dt","learning_time_hr"], axis=1, inplace=True)
    result.columns = ["plan_id","platform","plan_name","create_time","media_id","image_id","game_id","channel_id","source_id",
                      "source_run_date_amount","create_role_num","create_role_cost","two_day_create_retain","new_role_money","new_role_rate",
                      "create_role_pay_num","create_role_pay_cost","pay_role_rate","create_role_roi"]

    if action not in ['plan_control_model1', 'plan_control_model2']:
        return result
    else:
        sql = '''
            select plan_id, IFNULL(JSON_EXTRACT(ad_info, '$.deep_bid_type'),'DEEP_BID_DEFAULT') as deep_bid_type 
            from db_ptom.ptom_third_plan t_p 
            where media_id={} and create_time >= date( NOW() - INTERVAL 72 HOUR )
        '''
        dbt = pd.read_sql(sql.format(media_id), conn2)
        result = pd.merge(result, dbt, how='inner', on=['plan_id'])
        result['data_win'] = 3
        return result


def amount_cum(data, media_id, limit):
    """ 初始过滤：素材累计消耗未超限的素材 """
    # running = list(set(data['image_id'].values.tolist()))
    result = data_image_amount(media_id)
    result = result.groupby(['image_id']).sum().reset_index()
    result = result[result['amount'] < limit]
    
    data = pd.merge(result['image_id'], data, how='inner', on=['image_id'])

    return data


def plan_detail_toutiao(mgame_id):
    """ 中间采集：头条最近3天的计划详情 """
    plan_info = pd.read_csv('./data/ptom_third_plan.csv')
    ## 游戏ID确认
    game_ids = list(map(lambda x: x['game_id'], get_game_id(mgame_id)))
    game_ids = [str(i) for i in game_ids]
    game_ids = ','.join(game_ids)

    conn = pymysql.connect(host='db-slave-modeltoufang-001.ch', port=3306, user='model_read',
                            passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8')
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)

    ## 1、计划参数
    sql = '''
            /*手动查询*/ 
            SELECT
                *
            FROM
                db_ptom.ptom_third_plan p
            WHERE
                game_id IN ({})
                AND media_id = 10
                AND p.create_time >= date( NOW() - INTERVAL 120 HOUR )  ## 最近3天
        '''
    cur.execute(sql.format(game_ids))
    plan_param = pd.read_sql(sql.format(game_ids), conn)
    
    ## 2、计划创意
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
            b.create_time >= date( NOW() - INTERVAL 120 HOUR )    # 最近3天
            AND a.media_id = 10
            AND a.game_id IN ({}) 
    '''
    cur.execute(sql.format(game_ids))
    plan_creative = pd.read_sql(sql.format(game_ids), conn)
    plan_creative['title_list'] = plan_creative['title_list'].fillna('[]')
    plan_creative['ad_keywords'] = plan_creative['ad_keywords'].fillna('[]')
    plan_creative['title_list'] = plan_creative['title_list'].apply(json.loads)
    plan_creative['ad_keywords'] = plan_creative['ad_keywords'].apply(json.loads)
    plan_creative = plan_creative.loc[plan_creative.apply(lambda x:False if pd.isnull(x['third_industry_id']) else True, axis=1)]

    cur.close()
    conn.close()

    ## 3、计划素材
    conn = pymysql.connect(host='db-slave-modelfenxi-001.ch', port=3306, user='model_read',
                               passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8')
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
        /*手动查询*/
        SELECT
            a.chl_user_id as channel_id,
            a.source_id,
            a.image_id
        FROM
            db_data_ptom.ptom_plan a
        WHERE
            a.create_time >= date(NOW() - INTERVAL 120 HOUR)
            AND a.media_id = 10
            AND a.game_id IN ({})
    '''
    cur.execute(sql.format(game_ids))
    plan_image = pd.read_sql(sql.format(game_ids), conn)

    cur.close()
    conn.close()

    ## 4、合并
    plan_param = plan_param.append(plan_info)
    plan_param.drop(['inventory_type', 'budget'], axis=1, inplace=True)
    plan_param.dropna(how='all', inplace=True, axis=1)
    plan_param.dropna(subset=['ad_info'], inplace=True)
    plan_param['ad_info'] = plan_param['ad_info'].apply(json.loads)
    temp = plan_param['ad_info'].apply(pd.Series)
    plan_param = pd.concat([plan_param, temp], axis=1)
    plan_param.drop('ad_info', axis=1, inplace=True)
    temp = plan_param['audience'].apply(pd.Series)
    plan_param = pd.concat([plan_param, temp], axis=1)
    plan_param.drop('audience', axis=1, inplace=True)
    temp = plan_param['action'].apply(pd.Series)
    plan_param = pd.concat([plan_param, temp], axis=1)
    plan_param.drop('action', axis=1, inplace=True)
    plan_param.dropna(how='all', inplace=True, axis=1)
    plan_param = plan_param[['ad_account_id', 'game_id', 'channel_id', 'source_id',
                            'create_time', 'smart_bid_type', 'hide_if_exists', 'budget',
                            'delivery_range', 'adjust_cpa', 'inventory_type', 'hide_if_converted',
                            'flow_control_mode', 'schedule_time', 'cpa_bid', 'auto_extend_enabled',
                            'gender', 'city', 'platform', 'launch_price',
                            'retargeting_tags_exclude', 'interest_categories',
                            'ac', 'android_osv', 'location_type', 'retargeting_tags_include',
                            'ios_osv', 'interest_action_mode', 'age',
                            'action_categories', 'action_days', 'action_scene', 'deep_bid_type', 'roi_goal']]

    plan_param = plan_param.mask(plan_param.applymap(str).eq('NONE'))  ## NaN值
    plan_param['action_categories'] = plan_param['action_categories'].fillna('[]')
    plan_param['action_scene'] = plan_param['action_scene'].fillna('[]')
    plan_param['action_categories'] = plan_param['action_categories'].apply(lambda x:json.loads(x) if x == '[]' else x)
    plan_param['action_scene'] = plan_param['action_scene'].apply(lambda x:json.loads(x) if x == '[]' else x)
    plan_param['retargeting_type'] = np.nan
    plan_param['op_id'] = 13678
    plan_param['flag'] = 'CESHI'
    # plan_param['operation'] = 'disable'  ## 测试阶段尽量手动开
    plan_param['convertIndex'] = plan_param['deep_bid_type'].apply(lambda x: 13 if x == 'BID_PER_ACTION' else 14)

    if mgame_id == 1056:  ## 末日中间件
        plan_param['web_url'] = 'https://www.chengzijianzhan.com/tetris/page/6977185861965266981/'
    elif mgame_id == 1112:  ## 帝国中间件
        plan_param['city'] = plan_param['city'].apply(
        lambda x: x if x != [] else [11, 12, 13, 14, 15, 21, 22, 23, 31, 32, 33, 34, 35, 36, 37, 41, 420200, 420300,
                                    420500, 420600, 420700, 420800, 420900, 421000, 421100, 421200, 421300, 422800,
                                    429004, 429005, 429006, 429021, 43, 44, 45, 46, 50, 51, 52, 53, 54, 61, 62, 63, 64,
                                    65, 71, 81, 82])
        plan_param['district'] = 'CITY'
        plan_param['web_url'] = 'https://www.chengzijianzhan.com/tetris/page/6977187066200522789/'

    plan = pd.merge(plan_param, plan_creative, on=['channel_id', 'source_id'], how='left')
    plan = pd.merge(plan, plan_image, on=['channel_id', 'source_id'], how='left')  ## 完整计划的字段参数详情

    plan.dropna(subset=['image_id'], inplace=True)
    plan['image_id'] = plan['image_id'].astype(int)
    plan['platform'] = plan['platform'].astype(str)
    plan['platform'] = plan['platform'].map({"['ANDROID']": ['ANDROID'], "['IOS']": ['IOS']})
    plan['budget'] = plan['budget'].apply(np.ceil)

    return plan


def plan_detail_gdt(mgame_id):
    """ 中间采集：广点通最近3天的计划详情 """
    ## 游戏ID确认
    game_ids = list(map(lambda x: x['game_id'], get_game_id(mgame_id)))
    game_ids = [str(i) for i in game_ids]
    game_ids = ','.join(game_ids)

    conn = pymysql.connect(host='db-slave-modeltoufang-001.ch', port=3306, user='model_read',
                            passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8')
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)

    ## 1、计划参数（带素材ID）
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
                    AND p.create_time >= date( NOW() - INTERVAL 120 HOUR )
    '''
    finalSql = sql.format(game_ids)
    plan_param = pd.read_sql(finalSql, conn)
    
    ## 2、计划创意（带素材ID）
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
            b.create_time >= date( NOW() - INTERVAL 120 HOUR )    # 最近3天
            AND a.media_id = 16
            AND a.game_id IN ({})
    '''
    cur.execute(sql.format(game_ids))
    plan_creative = pd.read_sql(sql.format(game_ids), conn)
    plan_creative['creative_param'] = plan_creative['creative_param'].apply(json.loads)
    temp = plan_creative['creative_param'].apply(pd.Series)
    temp = temp.drop('image_id', axis=1)
    plan_creative = pd.concat([plan_creative, temp], axis=1)
    plan_creative.drop('creative_param', axis=1, inplace=True)
    plan_creative.drop(['title', 'adcreative_template_parent', 'idea_type', 'adcreative_name', 'ideaName', '_creative'],
                    axis=1, inplace=True)
    plan_creative['adcreative_elements_array'] = plan_creative['adcreative_elements_array'].apply(lambda x: x[0])
    plan_creative['adcreative_elements'] = plan_creative['adcreative_elements_array'].apply(
        lambda x: x['adcreative_elements'])
    plan_creative.drop('adcreative_elements_array', axis=1, inplace=True)
    plan_creative = plan_creative[['channel_id', 'source_id', 'image_id', 'deep_link_url',
                                   'adcreative_template_id', 'page_spec', 'page_type', 'site_set', 'label',
                                   'promoted_object_id', 'promoted_object_type', 'automatic_site_enabled',
                                   'link_name_type', 'link_page_type', 'profile_id', 'link_page_spec',
                                   'adcreative_elements']]

    cur.close()
    conn.close()

    ## 3、合并
    plan_param.drop(['inventory_type', 'budget', 'bid_mode'], axis=1, inplace=True)
    plan_param.dropna(how='all', inplace=True, axis=1)
    plan_param.dropna(subset=['ad_info'], inplace=True)
    ## 解析json
    plan_param['ad_info'] = plan_param['ad_info'].apply(json.loads)
    
    for col in ['ad_info', 'targeting', 'deep_conversion_spec', 'behavior_or_interest']:
        temp = plan_param[col].apply(pd.Series)
        plan_param = pd.concat([plan_param, temp], axis=1)
        plan_param.drop(col, axis=1, inplace=True)
    plan_param.drop(0, axis=1, inplace=True)

    temp = plan_param['intention'].apply(pd.Series)
    plan_param = pd.concat([plan_param, temp], axis=1)
    plan_param.drop('intention', axis=1, inplace=True)
    plan_param = plan_param.rename(columns={'targeting_tags': 'intention_targeting_tags'})
    plan_param.drop(0, axis=1, inplace=True)

    temp = plan_param['interest'].apply(pd.Series)
    plan_param = pd.concat([plan_param, temp], axis=1)
    plan_param.drop('interest', axis=1, inplace=True)
    plan_param = plan_param.rename(
        columns={'category_id_list': 'interest_category_id_list', 'keyword_list': 'interest_keyword_list',
                 'targeting_tags': 'interest_targeting_tags'})
    plan_param.drop(0, axis=1, inplace=True)

    temp = plan_param['behavior'].apply(pd.Series)
    plan_param = pd.concat([plan_param, temp], axis=1)
    plan_param.drop('behavior', axis=1, inplace=True)
    temp = plan_param[0].apply(pd.Series)
    plan_param = pd.concat([plan_param, temp], axis=1)
    plan_param.drop(0, axis=1, inplace=True)
    plan_param = plan_param.rename(columns={'category_id_list': 'behavior_category_id_list',
                                            'intensity': 'behavior_intensity',
                                            'keyword_list': 'behavior_keyword_list',
                                            'scene': 'behavior_scene',
                                            'targeting_tags': 'behavior_targeting_tags',
                                            'time_window': 'behavior_time_window'})

    temp = plan_param['excluded_converted_audience'].apply(pd.Series)
    plan_param = pd.concat([plan_param, temp], axis=1)
    plan_param.drop('excluded_converted_audience', axis=1, inplace=True)
    plan_param.drop(0, axis=1, inplace=True)

    try:
        temp = plan_param['geo_location'].apply(pd.Series)
        plan_param = pd.concat([plan_param, temp], axis=1)
        plan_param.drop('geo_location', axis=1, inplace=True)
        plan_param.drop(0, axis=1, inplace=True)
    except:
        pass

    ## 过滤一对多计划
    plan_param['ad_id_count'] = plan_param.groupby('plan_id')['ad_id'].transform('count')
    plan_param = plan_param[plan_param['ad_id_count'] == 1]

    ## 删除纯买激活的计划
    plan_param = plan_param[~((plan_param['deep_conversion_type'].isna()) & (
                 plan_param['optimization_goal'] == 'OPTIMIZATIONGOAL_APP_ACTIVATE'))]
    ## 删除auto_audience=True 的记录，并且删除auto_audience字段
    plan_param[plan_param['auto_audience'] == False]
    plan_param['deep_conversion_behavior_spec'] = np.nan if 'deep_conversion_behavior_spec' not in plan_param.columns else plan_param['deep_conversion_behavior_spec']
    plan_param['interest_keyword_list'] = None if 'interest_keyword_list' not in plan_param.columns else plan_param['interest_keyword_list']
    plan_param['network_type'] = np.nan if 'network_type' not in plan_param.columns else plan_param['network_type']

    try:
        plan_param = plan_param[['ad_account_id', 'game_id', 'channel_id', 'source_id', 'budget_mode',
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
    except:
        plan_param = plan_param[['ad_account_id', 'game_id', 'channel_id', 'source_id', 'budget_mode',
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
                                'conversion_behavior_list', 'excluded_dimension']]
    
    plan_param = pd.merge(plan_param.drop(['image_id'], axis=1), plan_creative, on=['channel_id', 'source_id'], how='inner')
    plan_param.dropna(subset=['image_id'], inplace=True)
    plan_param['image_id'] = plan_param['image_id'].astype(int)
    plan_param['adcreative_template_id'] = plan_param['adcreative_template_id'].astype(int)
    plan_param['profile_id'] = plan_param['profile_id'].apply(lambda x: x if x != x else str(int(x)))
    plan_param['create_time'] = pd.to_datetime(plan_param['create_time'])

    plan_param = plan_param.mask(plan_param.applymap(str).eq('NONE'))  ## NaN值
    plan_param['op_id'] = 13678
    plan_param['flag'] = 'CESHI_GDT'
    plan_param['operation'] = 'disable'  ## 测试阶段尽量手动开
    plan_param['game_name'] = '幸存者挑战' ## TODO:新字段
    plan_param['platform'] = 1 ## TODO:新字段
    
    plan_param['game_id'] = cfg.GDT_MR_GAME
    # plan_param['promoted_object_id'] = '1111059412'  # TODO:跟新包体对应的应用ID
    plan_param['budget_mode'] = 0  ## 不限预算
    plan_param['daily_budget'] = 0  ## 不限预算
    # plan_param['platform'] = plan_param['platform'].map({1: ['ANDROID'], 2: ['IOS']})
    plan_param['ad_account_id'] = plan_param['ad_account_id'].apply(lambda x:str(int(x)))

    ## [SITE_SET_WECHAT]公众号和取值范围在1480、560、720、721、1064五种中adcreative_template_id小程序
    plan_param['site_set_copy'] = plan_param['site_set'].copy()
    plan_param['site_set'] = plan_param['site_set'].map(str)
    plan_param = plan_param[~((plan_param['site_set'] == "['SITE_SET_WECHAT']") & \
                                (~plan_param['adcreative_template_id'].isin([1480, 560, 720, 721, 1064])))]
    ## 落地页ID
    plan_param['page_spec'] = plan_param.apply(
        lambda x: {'override_canvas_head_option': 'OPTION_CREATIVE_OVERRIDE_CANVAS',
                   'page_id': '2230311524'} if x.site_set == "['SITE_SET_MOMENTS']"
        else ({'override_canvas_head_option': 'OPTION_CREATIVE_OVERRIDE_CANVAS',
               'page_id': '2179536949'} if x.site_set == "['SITE_SET_WECHAT']" else np.nan), axis=1)
    plan_param['link_page_spec'] = plan_param.apply(
        lambda x: {'page_id': '2230311524'} if x.site_set == "['SITE_SET_MOMENTS']"
        else ({'page_id': '2179536949'} if x.site_set == "['SITE_SET_WECHAT']" else np.nan), axis=1)

    ## 朋友圈头像ID
    plan_param_1 = plan_param[plan_param['site_set'] == "['SITE_SET_MOMENTS']"]
    plan_param_2 = plan_param[plan_param['site_set'] != "['SITE_SET_MOMENTS']"]
    profile_id_dict = {'7981': '372606', '7982': '372597', '7983': '372591', '7984': '372585', '7985': '372485',
                        '8035': '383952', '8036': '383967', '8037': '383976', '8038': '383987', '8039': '383994',
                        '8082': '408038', '8081': '408049', '8080': '408052', '8079': '408056', '8078': '408059',
                        '8077': '408062', '8076': '408066', '8075': '408069', '8074': '408073', '8073': '408082'}
    plan_param_1['profile_id'] = plan_param_1['ad_account_id'].map(profile_id_dict)
    plan_param = plan_param_1.append(plan_param_2)
    plan_param.reset_index(drop=True, inplace=True)
    ## 目标定向字段映射
    plan_param['intention'] = plan_param['intention_targeting_tags'].apply(lambda x: {'targeting_tags': x})
    plan_param.drop(['intention_targeting_tags'], axis=1, inplace=True)
    ## 兴趣定向组合json
    plan_param['interest'] = plan_param.apply(lambda x:json.loads(
        x[['interest_category_id_list','interest_keyword_list']].rename(index={
            'interest_category_id_list': 'category_id_list',
            'interest_keyword_list': 'keyword_list'
            }).to_json()), axis=1)
    plan_param.drop(['interest_category_id_list', 'interest_keyword_list'], axis=1, inplace=True)
    ## 行为定向组合json
    plan_param['behavior'] = plan_param.apply(lambda x:json.loads(
        x[['behavior_category_id_list','behavior_intensity','behavior_keyword_list','behavior_scene','behavior_time_window']].rename(index={
            'behavior_category_id_list': 'category_id_list',
            'behavior_intensity': 'intensity',
            'behavior_keyword_list': 'keyword_list',
            'behavior_scene': 'scene',
            'behavior_time_window': 'time_window'
            }).to_json()), axis=1)
    plan_param.drop(['behavior_category_id_list', 'behavior_intensity', 'behavior_keyword_list', 'behavior_scene',
                        'behavior_time_window'], axis=1, inplace=True)
    plan_param['behavior'] = plan_param['behavior'].apply(lambda x: [x])
    ## 排除定向组合json
    plan_param['excluded_converted_audience'] = plan_param.apply(lambda x:json.loads(
        x[['conversion_behavior_list', 'excluded_dimension']].to_json()), axis=1)
    plan_param.drop(['conversion_behavior_list', 'excluded_dimension'], axis=1, inplace=True)
    ## 地域定向组合json
    try:
        plan_param['geo_location'] = plan_param.apply(lambda x:json.loads(
            x[['regions', 'location_types']].to_json()), axis=1)
        plan_param.drop(['regions', 'location_types'], axis=1, inplace=True)
    except:
        plan_param['geo_location'] = 1
        plan_param['geo_location'] = plan_param['geo_location'].map({1: {}})
    ## 其他定向组合json
    plan_param['targeting'] = plan_param.apply(lambda x:json.loads(
        x[['device_price', 'app_install_status', 'gender', 'game_consumption_level', 'age', 'network_type', 'excluded_converted_audience', 'geo_location', 'intention', 'interest', 'behavior']].to_json()), axis=1)
    plan_param.drop(['device_price', 'app_install_status', 'gender', 'game_consumption_level', 'age', 'network_type', 'excluded_converted_audience', 'geo_location', 'intention', 'interest', 'behavior'], axis=1, inplace=True)
    
    plan_param['ad_account_id'] = plan_param['ad_account_id'].astype(int)
    plan_param['site_set'] = plan_param['site_set_copy']
    ## 时段周三周四更新凌晨不跑计划
    plan_param['time_series'] = plan_param['time_series'].apply(
        lambda x: x[0:96] + '1111111111000000000011' + x[118:144] + '1111111111000000000011' + x[166:])
    plan_param.drop(['label','page_type','site_set_copy'], axis=1, inplace=True)
    
    return plan_param


def plan_pay_sql(df, media_id, mgame_id):
    """ 付费计划复制 """
    if not len(df):
        return pd.DataFrame([])  ## 没有付费计划时返回空dataframe

    # 1、匹配付费计划
    plan = plan_detail_toutiao(mgame_id) if media_id == 10 else plan_detail_gdt(mgame_id)
    plan = pd.merge(df[['channel_id','source_id','plan_num']], plan, how='inner', on=['channel_id', 'source_id'])

    if not len(plan):
        return pd.DataFrame([])  ## 没有匹配到付费计划时返回空dataframe

    # 2、计划数量上限
    plan = plan.groupby(['image_id']).apply(lambda x:pd.DataFrame(np.repeat(x.values, 1, axis=0), columns=plan.columns) \
                                                    if x['plan_num'].mean() < 50-1*len(x) else x).reset_index(drop=True)  ## 复制3次or复制1次
    plan.drop(['channel_id','source_id','plan_num','create_time'], axis=1, inplace=True)

    return plan, df['plan_id'].values.tolist()


def plan_bid_sql(df, media_id, mgame_id):
    """ 计划出价调整（暂只支持头条） """
    ## 游戏ID确认
    game_ids = list(map(lambda x: x['game_id'], get_game_id(mgame_id)))
    game_ids = [str(i) for i in game_ids]
    game_ids = ','.join(game_ids)

    conn = pymysql.connect(host='db-slave-modeltoufang-001.ch', port=3306, user='model_read',
                            passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8')
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)

    sql = '''
            /*手动查询*/
            SELECT
                *
            FROM
                db_ptom.ptom_third_plan p
            WHERE
                p.create_time >= date( NOW() - INTERVAL 72 HOUR )  ## 最近3天
                AND p.media_id = 10
                AND p.game_id IN ({})
        ''' if media_id == 10 else '''
            /*手动查询*/
                SELECT
                    *
                FROM
                    db_ptom.ptom_third_plan p
                WHERE
                    p.create_time >= date( NOW() - INTERVAL 72 HOUR )  ## 最近3天
                    AND p.media_id = 16
                    AND p.game_id IN ({})
        '''
    cur.execute(sql.format(game_ids))
    plan_param = pd.read_sql(sql.format(game_ids), conn)
    cur.close()
    conn.close()

    ## todo:广点通的bid_amount字段
    plan_param.drop(['inventory_type', 'budget'], axis=1, inplace=True)
    plan_param.dropna(how='all', inplace=True, axis=1)
    plan_param.dropna(subset=['ad_info'], inplace=True)
    plan_param['ad_info'] = plan_param['ad_info'].apply(json.loads)
    temp = plan_param['ad_info'].apply(pd.Series)
    plan_param = pd.concat([plan_param, temp], axis=1)
    plan_param.drop('ad_info', axis=1, inplace=True)
    temp = plan_param['audience'].apply(pd.Series)
    plan_param = pd.concat([plan_param, temp], axis=1)
    plan_param.drop('audience', axis=1, inplace=True)
    temp = plan_param['action'].apply(pd.Series)
    plan_param = pd.concat([plan_param, temp], axis=1)
    plan_param.drop('action', axis=1, inplace=True)
    plan_param.dropna(how='all', inplace=True, axis=1)
    plan_param = plan_param[['channel_id', 'source_id', 'cpa_bid']]

    ## 3、匹配当前计划的cpa_bid
    cpa_bid = pd.merge(df, plan_param, how='inner', on=['channel_id', 'source_id'])

    return cpa_bid


def data_image_create(media_id):
    """ 中间提取：最近3天的素材创建 """
    conn = pymysql.connect(host='db-slave-modelfenxi-001.ch', port=3306, user='model_read',
                               passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8')
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
        /*手动查询*/ 
        SELECT
            image_id,
            create_time
        FROM
            db_data_ptom.ptom_plan
        WHERE
            create_time >= date( NOW() - INTERVAL 72 HOUR )
            AND media_id = {}
    '''
    cur.execute(sql.format(media_id))
    df = pd.read_sql(sql.format(media_id), conn)
    conn.close()
    cur.close()

    return df

def time_coldstart_left(image_id, df):
    """ 中间计算：剩余待测新时间 """
    df = df[df['image_id'] == image_id]

    if not len(df):
        return 0, np.inf  ## 素材创建时间在3天以前

    df = df.sort_values(['create_time'])

    return max((df['create_time'].iloc[0] + datetime.timedelta(days=3) - datetime.datetime.now()).total_seconds(), 0), (datetime.datetime.now() - df['create_time'].iloc[0]).total_seconds()


def data_image_amount(media_id):
    """ 中间提取：最近3天的素材消耗 """
    conn = pymysql.connect(host='db-slave-modelfenxi-001.ch', port=3306, user='model_read',
                               passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8')
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
        /*手动查询*/
        SELECT
            b.channel_id,
            b.source_id,
            b.tdate,
            a.image_id,
            b.amount
        FROM
            db_data_ptom.ptom_plan a
            LEFT JOIN db_stdata.st_lauch_report b ON a.chl_user_id = b.channel_id 
            AND a.source_id = b.source_id 
        WHERE
            a.create_time >= date( NOW() - INTERVAL 120 HOUR )
            AND b.tdate >= date( NOW() - INTERVAL 120 HOUR )
            AND b.media_id = {}
            AND b.tdate_type = 'day'
    '''
    cur.execute(sql.format(media_id))
    df = pd.read_sql(sql.format(media_id), conn)
    df['tdate'] = pd.to_datetime(df['tdate'])
    # df = df.sort_values('tdate')
    # df = df.drop_duplicates(['channel_id', 'source_id'], keep='first')
    df.drop(columns=['channel_id', 'source_id', 'tdate'], inplace=True)
    conn.close()
    cur.close()

    return df

def amount_coldstart_left(image_id, df):
    """ 中间计算：剩余测新待消耗 """
    df = df[df['image_id'] == image_id]

    if not len(df):
        return 0, np.inf  ## 素材创建时间在3天以前

    return max(10000 - df['amount'].sum(), 0), df['amount'].sum()  ## todo：6000直接这里修改未传参


def _filter(x, data, mgame_id):
    """ 中间过滤：每个素材下最佳计划的过滤 """
    if not all(x['create_role_roi'] == 0):  ## 有支付
        y = x[(x['create_role_roi'] > 0) | (x['source_run_date_amount'] > 500)]  ## 有支付或消耗达到500的计划
    else:  ## 没支付
        y = x[(x['source_run_date_amount'] > 300) & (x['create_role_cost'] < 80)] if mgame_id == 1056 else \
            x[(x['source_run_date_amount'] > 300) & (x['create_role_cost'] < 200)]  ## 创角成本低于末日60、帝国200的计划

    if not len(y):
        y = pd.DataFrame(data=data['data'], columns=data['columns'])
        y = y.sort_values(['create_role_cost'])
        y['image_id'] = x['image_id'].values[0]
        y['plan_num'] = x['plan_num'].values[0]
        y = y[(y['source_run_date_amount'] > 2000) & (y['create_role_cost'] < 100)][x.columns]  ## 1、如果上述都不满足，直接从近3天其他素材所有计划中，随机选最佳计划的定向、版位...
        return y.sample(1)

    y = y.sort_values(['create_role_cost'])

    if not len(y[y['source_run_date_amount'] > 1000]):
        return y[y['create_role_cost'] > 0].iloc[0]  ## 2、如果上述满足，选该素材下创角成本最低的计划
    
    y = y[(y['source_run_date_amount'] > 1000) & (y['create_role_cost'] > 0)]  ## 3、如果上述满足，选该素材下消耗1000以上，创角成本最低的计划

    return y.iloc[0]


def image_plan_sql(df, data, media_id, mgame_id):
    """ 素材下所有计划的整体消耗把控 """
    """
    # 素材维度：每个image_id第一个计划创建时间开始，3天后的冷启动时间为节点，减去当前时间，等于剩余测新时间T
    # 素材维度：6000减去每个image_id至今所有计划的amount消耗，等于剩余待消耗量L
    # 素材维度：每个image_id至今所有计划的amount消耗，除以第一个计划创建时间开始至今，等于消耗速率V
    # 素材维度：如果V*T >= L，复制每个素材"source_run_date_amount"达标，且"create_role_cost"最小的计划3条
    # 素材维度：计划数量限制
    """
    ## 1、素材的剩余测新时间
    data1 = data_image_create(media_id)
    time_and_left_series = df['image_id'].drop_duplicates().apply(lambda x:time_coldstart_left(x, data1))
    ## 2、素材的剩余待消耗量
    data2 = data_image_amount(media_id)
    amount_and_left_series = df['image_id'].drop_duplicates().apply(lambda x:amount_coldstart_left(x, data2))
    ## 3、期望消耗不达标的素材
    time_and_left_series = np.asarray(time_and_left_series.sum()).reshape(-1, 2)
    amount_and_left_series = np.asarray(amount_and_left_series.sum()).reshape(-1, 2)
    v = amount_and_left_series[:, 1] / time_and_left_series[:, 1]
    exp = v * time_and_left_series[:, 0]
    image_ids = df['image_id'].drop_duplicates()[exp < amount_and_left_series[:, 0]]  ## 候选素材
    
    ## 4、该素材下最佳计划的复制
    df = df[df.apply(lambda x: True if x['image_id'] in image_ids.values.tolist() else False, axis=1)]  ## 候选素材下的所有计划表现
    df = df.groupby(['image_id']).apply(lambda x:_filter(x, data, mgame_id)).reset_index(drop=True)  ## 候选素材下的最佳计划表现
    
    if not len(df):
        return pd.DataFrame([])  ## 没有不达标素材时返回空dataframe

    ## 5、匹配计划详情
    plan = plan_detail_toutiao(mgame_id) if media_id == 10 else plan_detail_gdt(mgame_id)
    plan.drop(columns=['image_id'], inplace=True)
    plan = pd.merge(df[['channel_id','source_id','image_id','plan_num']], plan, how='inner', on=['channel_id', 'source_id'])  ## 候选素材下的最佳计划详情
    plan['adcreative_elements'] = plan.apply(lambda x:image_elements(x), axis=1)
    plan.dropna(subset=['adcreative_elements'], inplace=True)
    
    if not len(plan):
        return pd.DataFrame([])  ## 素材下没有在跑满足条件的计划时返回空dataframe

    ## 6、计划数量上限
    plan = plan.groupby(['image_id']).apply(lambda x:pd.DataFrame(np.repeat(x.values, 1, axis=0), columns=plan.columns) \
                                                     if x['plan_num'].mean() < 50-1*len(x) else x).reset_index(drop=True)  ## 复制3次or复制1次
    plan.drop(['channel_id', 'source_id', 'plan_num', 'create_time'], axis=1, inplace=True)

    return plan


def model1_label(data):
    """ 中间决策：时间窗口为当日的计划开关规则 """
    ## 每次付费安卓
    if all(data['deep_bid_type'] == 'BID_PER_ACTION') and all(data['platform'] == 1):
        if data.shape[0] != 0:
            data['label'] = data.apply(
                lambda x: 1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount <= 1000) &
                            (x.create_role_cost >= 150) else (
                    1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount > 1000) &
                        (x.create_role_cost >= 100) else (
                        1 if (x.create_role_roi > 0) & (x.create_role_roi <= 0.02) & (x.create_role_cost >= 150) else (
                            1 if x.create_role_cost >= 200 else 0))), axis=1)
    ## 每次付费IOS
    if all(data['deep_bid_type'] == 'BID_PER_ACTION') and all(data['platform'] == 2):
        if data.shape[0] != 0:
            data['label'] = data.apply(
                lambda x: 1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount <= 1000) &
                            (x.create_role_cost >= 250) else (
                    1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount > 1000) &
                        (x.create_role_cost >= 200) else (
                        1 if (x.create_role_roi <= 0.02) & (x.create_role_cost >= 300) else (
                            1 if x.create_role_cost >= 300 else 0))), axis=1)
    ## 其他付费安卓
    if all(data['deep_bid_type'] != 'BID_PER_ACTION') and all(data['platform'] == 1):
        if data.shape[0] != 0:
            data['label'] = data.apply(
                lambda x: 1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount <= 1000) &
                            (x.create_role_cost >= 120) else (
                    1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount > 1000) &
                        (x.create_role_cost >= 70) else (
                        1 if (x.create_role_roi > 0) & (x.create_role_roi <= 0.02) & (x.create_role_cost >= 100) else (
                            1 if x.create_role_cost >= 130 else 0))), axis=1)
    ## 其他付费IOS
    if all(data['deep_bid_type'] != 'BID_PER_ACTION') and all(data['platform'] == 2):
        if data.shape[0] != 0:
            data['label'] = data.apply(
                lambda x: 1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount <= 1000) &
                            (x.create_role_cost >= 200) else (
                    1 if (x.create_role_pay_sum == 0) & (x.source_run_date_amount > 1000) &
                        (x.create_role_cost >= 150) else (
                        1 if (x.create_role_roi <= 0.02) & (x.create_role_cost >= 250) else (
                            1 if x.create_role_cost >= 250 else 0))), axis=1)

    return data


def model1_budget(data_):
    """ 中间决策：时间窗口为当日的计划预算规则 """
    if all(data_['platform'] == 1):
        ## 安卓
        bins = [0, 4500, 9200, 18000, 40000, 76000, 120000, 190000, 240000, 290000, np.inf]
        data_['amount_cate'] = pd.cut(data_['source_run_date_amount'], bins).astype(str)
        
        def check_az(data):
            """ 嵌套：安卓预算逻辑 """
            if all(data['amount_cate'] == '(0.0, 4500.0]'):
                if data.shape[0] != 0:
                    data['budget'] = data.apply(
                        lambda x: 6600 if (x.create_role_pay_sum >= 60) & (x.create_role_pay_num >= 2) else
                        (6600 if x.create_role_pay_sum >= 98 else np.nan), axis=1)
            if all(data['amount_cate'] == '(4500.0, 9200.0]'):
                if data.shape[0] != 0:
                    data['budget'] = data.apply(
                        lambda x: 13000 if (x.create_role_roi >= 0.028) & (x.create_role_pay_num >= 2) else np.nan, axis=1)
            if all(data['amount_cate'] == '(9200.0, 18000.0]'):
                if data.shape[0] != 0:
                    data['budget'] = data.apply(
                        lambda x: 23000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_num >= 3) else
                        (23000 if x.create_role_roi >= 0.05 else np.nan), axis=1)
            if all(data['amount_cate'] == '(18000.0, 40000.0]'):
                if data.shape[0] != 0:
                    data['budget'] = data.apply(
                        lambda x: 50000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_cost <= 4500) else
                        (50000 if (x.create_role_roi >= 0.035) & (x.create_role_pay_cost <= 6000) else
                        np.nan), axis=1)
            if all(data['amount_cate'] == '(40000.0, 76000.0]'):
                if data.shape[0] != 0:
                    data['budget'] = data.apply(
                        lambda x: 95000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_cost <= 4200) else
                        (95000 if (x.create_role_roi >= 0.035) & (x.create_role_pay_cost <= 5500) else
                        np.nan), axis=1)
            if all(data['amount_cate'] == '(76000.0, 120000.0]'):
                if data.shape[0] != 0:
                    data['budget'] = data.apply(
                        lambda x: 148000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_cost <= 4000) else
                        (148000 if (x.create_role_roi >= 0.035) & (x.create_role_pay_cost <= 3500) else
                        np.nan), axis=1)
            if all(data['amount_cate'] == '(120000.0, 190000.0]'):
                if data.shape[0] != 0:
                    data['budget'] = data.apply(
                        lambda x: 220000 if (x.create_role_roi >= 0.031) & (x.create_role_pay_cost <= 4000) else
                        (220000 if (x.create_role_roi >= 0.036) & (x.create_role_pay_cost <= 3500) else
                        np.nan), axis=1)
            if all(data['amount_cate'] == '(190000.0, 240000.0]'):
                if data.shape[0] != 0:
                    data['budget'] = data.apply(
                        lambda x: 275000 if (x.create_role_roi >= 0.032) & (x.create_role_pay_cost <= 3800) else
                        (275000 if (x.create_role_roi >= 0.037) & (x.create_role_pay_cost <= 3300) else
                        np.nan), axis=1)
            if all(data['amount_cate'] == '(240000.0, 290000.0]'):
                if data.shape[0] != 0:
                    data['budget'] = data.apply(
                        lambda x: 320000 if (x.create_role_roi >= 0.032) & (x.create_role_pay_cost <= 3600) else
                        (320000 if (x.create_role_roi >= 0.037) & (x.create_role_pay_cost <= 3100) else
                        np.nan), axis=1)
            if all(data['amount_cate'] == '(290000.0, inf]'):
                if data.shape[0] != 0:
                    data['budget'] = data.apply(
                        lambda x: 450000 if (x.create_role_roi >= 0.033) & (x.create_role_pay_cost <= 3500) else
                        (450000 if (x.create_role_roi >= 0.038) & (x.create_role_pay_cost <= 3000) else
                        np.nan), axis=1)

            data = data.drop(['amount_cate'], axis=1)
            return data
    
        data_ = data_.groupby(['amount_cate']).apply(lambda x: check_az(x))
    
    elif all(data_['platform'] == 2):
        ## IOS
        bins = [0, 5600, 9200, 18000, 40000, 76000, 120000, 190000, 240000, 290000, np.inf]
        data_['amount_cate'] = pd.cut(data_['source_run_date_amount'], bins).astype(str)

        def check_ios(data):
            """ 嵌套：IOS预算逻辑 """
            if all(data['amount_cate'] == '(0.0, 5600.0]'):
                if data.shape[0] != 0:
                    data['budget'] = data.apply(
                        lambda x: 8000 if (x.create_role_pay_sum >= 60) & (x.create_role_pay_num >= 2) else
                        (8000 if x.create_role_pay_sum >= 98 else np.nan), axis=1)
            if all(data['amount_cate'] == '(5600.0, 9200.0]'):
                if data.shape[0] != 0:
                    data['budget'] = data.apply(
                        lambda x: 13000 if (x.create_role_roi >= 0.024) & (x.create_role_pay_num >= 2) else np.nan, axis=1)
            if all(data['amount_cate'] == '(9200.0, 18000.0]'):
                if data.shape[0] != 0:
                    data['budget'] = data.apply(
                        lambda x: 23000 if (x.create_role_roi >= 0.026) & (x.create_role_pay_num >= 3) else
                                           (23000 if x.create_role_roi >= 0.035 else np.nan), axis=1)
            if all(data['amount_cate'] == '(18000.0, 40000.0]'):
                if data.shape[0] != 0:
                    data['budget'] = data.apply(
                        lambda x: 50000 if (x.create_role_roi >= 0.028) & (x.create_role_pay_cost <= 6000) else
                                           (50000 if (x.create_role_roi >= 0.038) & (x.create_role_pay_cost <= 7500) else np.nan), axis=1)
            if all(data['amount_cate'] == '(40000.0, 76000.0]'):
                if data.shape[0] != 0:
                    data['budget'] = data.apply(
                        lambda x: 95000 if (x.create_role_roi >= 0.029) & (x.create_role_pay_cost <= 5500) else
                                           (95000 if (x.create_role_roi >= 0.035) & (x.create_role_pay_cost <= 7000) else np.nan), axis=1)
            if all(data['amount_cate'] == '(76000.0, 120000.0]'):
                if data.shape[0] != 0:
                    data['budget'] = data.apply(
                        lambda x: 148000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_cost <= 5000) else
                                            (148000 if (x.create_role_roi >= 0.035) & (x.create_role_pay_cost <= 6500) else np.nan), axis=1)
            if all(data['amount_cate'] == '(120000.0, 190000.0]'):
                if data.shape[0] != 0:
                    data['budget'] = data.apply(
                        lambda x: 220000 if (x.create_role_roi >= 0.03) & (x.create_role_pay_cost <= 4500) else
                                            (220000 if (x.create_role_roi >= 0.035) & (x.create_role_pay_cost <= 6000) else np.nan), axis=1)
            if all(data['amount_cate'] == '(190000.0, 240000.0]'):
                if data.shape[0] != 0:
                    data['budget'] = data.apply(
                        lambda x: 275000 if (x.create_role_roi >= 0.031) & (x.create_role_pay_cost <= 4500) else
                                            (275000 if (x.create_role_roi >= 0.035) & (x.create_role_pay_cost <= 6000) else np.nan), axis=1)
            if all(data['amount_cate'] == '(240000.0, 290000.0]'):
                if data.shape[0] != 0:
                    data['budget'] = data.apply(
                        lambda x: 320000 if (x.create_role_roi >= 0.031) & (x.create_role_pay_cost <= 4500) else
                                            (320000 if (x.create_role_roi >= 0.035) & (x.create_role_pay_cost <= 6000) else np.nan), axis=1)
            if all(data['amount_cate'] == '(290000.0, inf]'):
                if data.shape[0] != 0:
                    data['budget'] = data.apply(
                        lambda x: 450000 if (x.create_role_roi >= 0.031) & (x.create_role_pay_cost <= 4000) else
                                            (450000 if (x.create_role_roi >= 0.035) & (x.create_role_pay_cost <= 5500) else np.nan), axis=1)
            
            data = data.drop(['amount_cate'], axis=1)
            return data

        data_ = data_.groupby(['amount_cate']).apply(lambda x: check_ios(x))

    else:
        raise ValueError("platform分组取值情况异常")

    return data_


def model_1(df):
    """ 模型1（已经重构） """
    df = df[df['source_run_date_amount'] >= 300]  ## 300以下的消耗不做模型1的判断

    ## 1、计划开关
    df = df.groupby(['deep_bid_type','platform']).apply(lambda x:model1_label(x))  ## 2次分组：'deep_bid_type','platform'两字段直接分组
    ## 2、计划预算
    df = df.groupby(['platform']).apply(lambda x:model1_budget(x)).reset_index(drop=True)  ## 2次分组：先'platform'分组，然后不同'amount'取值的分箱，再'amount'分箱值的分组
    
    df['label'] = df['label'].astype(int)
    df['budget'] = df['budget'].replace(np.nan, 'None')
    df = df.fillna('null')

    return df


def model_2(df, gbdt_m2_win1, gbdt_m2_win2, gbdt_m2_win3):
    """ 模型2（暂未重构） """
    df = df[df['source_run_date_amount'] > 0]  ## 删除消耗为0的数据
    ##  窗口期数据排序
    df.sort_values(by='data_win', inplace=True)
    ##  字段去重
    df.drop_duplicates(subset=['channel_id', 'source_id', 'source_run_date_amount', 'create_role_num'], keep='first', inplace=True)
    ## 对消耗300元以下的不作判断，对数据按平台、win进行分类处理
    data_not_ignore = df[(df['source_run_date_amount'] >= 300) | (df['data_win'] == 1)]
    data_ignore = df.drop(data_not_ignore.index)

    data_win_1 = data_not_ignore[(data_not_ignore['data_win'] == 1) & (data_not_ignore['platform'] == 1)]
    data_win_2 = data_not_ignore[(data_not_ignore['data_win'] == 2) & (data_not_ignore['platform'] == 1)]
    data_win_3 = data_not_ignore[(data_not_ignore['data_win'] == 3) & (data_not_ignore['platform'] == 1)]

    data_win_1_ios = data_not_ignore[(data_not_ignore['data_win'] == 1) & (data_not_ignore['platform'] == 2)]
    data_win_2_ios = data_not_ignore[(data_not_ignore['data_win'] == 2) & (data_not_ignore['platform'] == 2)]
    data_win_3_ios = data_not_ignore[(data_not_ignore['data_win'] == 3) & (data_not_ignore['platform'] == 2)]

    ## win=1预判
    result_win1 = pd.DataFrame()
    temp_win1 = pd.DataFrame()

    ## data_win_1中再按deep_bid_type分组
    data_win_1_pre_action = data_win_1[data_win_1['deep_bid_type'] == 'BID_PER_ACTION']
    data_win_1 = data_win_1[data_win_1['deep_bid_type'] != 'BID_PER_ACTION']

    if data_win_1_pre_action.shape[0] != 0:
        data_win_1_pre_action['label'] = data_win_1_pre_action.apply(lambda x: 1 if x.create_role_num == 0 else (1 if
                                                                                                                 x.create_role_roi < 0.018 else (
            0 if (x.create_role_pay_num > 1) & (
                    x.create_role_roi >= 0.02) & (x.source_run_date_amount >= 500) else (
                0 if (x.create_role_roi >= 0.022) & (x.source_run_date_amount >= 500) else 1 if
                (x.create_role_pay_cost > 8000) | (x.create_role_cost > 220) else 2))), axis=1)

        result_win1 = result_win1.append(
            data_win_1_pre_action[(data_win_1_pre_action['label'] == 1) | (data_win_1_pre_action['label'] == 0)])
        temp_win1 = temp_win1.append(data_win_1_pre_action[data_win_1_pre_action['label'] == 2])

    if data_win_1.shape[0] != 0:
        data_win_1['label'] = data_win_1.apply(lambda x: 1 if x.create_role_num == 0 else (1 if
                                                                                           x.create_role_roi <= 0.018 else (
            0 if (x.create_role_pay_num > 1) & (
                    x.create_role_roi >= 0.02) & (x.source_run_date_amount >= 500) else (
                0 if (x.create_role_roi >= 0.022) & (x.source_run_date_amount >= 500) else 1 if
                (x.create_role_pay_cost > 8000) | (x.create_role_cost > 140) else 2))), axis=1)

        result_win1 = result_win1.append(data_win_1[(data_win_1['label'] == 1) | (data_win_1['label'] == 0)])
        temp_win1 = temp_win1.append(data_win_1[data_win_1['label'] == 2])

    data_win_1_ios_pre_action = data_win_1_ios[data_win_1_ios['deep_bid_type'] == 'BID_PER_ACTION']
    data_win_1_ios = data_win_1_ios[data_win_1_ios['deep_bid_type'] != 'BID_PER_ACTION']

    if data_win_1_ios_pre_action.shape[0] != 0:
        data_win_1_ios_pre_action['label'] = data_win_1_ios_pre_action.apply(
            lambda x: 1 if x.create_role_num == 0 else (1 if
                                                        x.create_role_roi <= 0.016 else (
                0 if (x.create_role_pay_num > 1) & (
                        x.create_role_roi >= 0.018) & (x.source_run_date_amount >= 500) else (
                    0 if (x.create_role_roi >= 0.02) & (x.source_run_date_amount >= 500) else 1 if
                    (x.create_role_pay_cost > 12000) | (x.create_role_cost > 300) else 2))), axis=1)

        result_win1 = result_win1.append(
            data_win_1_ios_pre_action[
                (data_win_1_ios_pre_action['label'] == 1) | (data_win_1_ios_pre_action['label'] == 0)])
        temp_win1 = temp_win1.append(data_win_1_ios_pre_action[data_win_1_ios_pre_action['label'] == 2])

    if data_win_1_ios.shape[0] != 0:
        data_win_1_ios['label'] = data_win_1_ios.apply(lambda x: 1 if x.create_role_num == 0 else (1 if
                                                                                                   x.create_role_roi <= 0.016 else (
            0 if (x.create_role_pay_num > 1) & (
                    x.create_role_roi >= 0.018) & (x.source_run_date_amount >= 500) else (
                0 if (x.create_role_roi >= 0.02) & (x.source_run_date_amount >= 500) else 1 if
                (x.create_role_pay_cost > 12000) | (x.create_role_cost > 200) else 2))), axis=1)

        result_win1 = result_win1.append(
            data_win_1_ios[(data_win_1_ios['label'] == 1) | (data_win_1_ios['label'] == 0)])
        temp_win1 = temp_win1.append(data_win_1_ios[data_win_1_ios['label'] == 2])

    ## win=1 模型预测
    if temp_win1.shape[0] != 0:
        temp_win1['label'] = gbdt_m2_win1.predict(temp_win1[['create_role_cost',
                                                            'create_role_pay_cost', 'create_role_roi',
                                                            'create_role_pay_rate']])
    result_win1_data = pd.concat([result_win1, temp_win1], axis=0)

    ## win=2预判
    result_win2 = pd.DataFrame()
    temp_win2 = pd.DataFrame()
    if data_win_2.shape[0] != 0:
        data_win_2['label'] = data_win_2.apply(lambda x: 1 if x.create_role_num == 0 else (1 if
                                                                                           x.create_role_roi <= 0.033 else (
            0 if (x.create_role_pay_num > 1) & (
                    x.create_role_roi >= 0.035) & (x.source_run_date_amount >= 500) else (
                0 if (x.create_role_roi >= 0.038) & (x.source_run_date_amount >= 500) else 1 if
                (x.create_role_pay_cost > 7000) | (x.create_role_cost > 150) else 2))), axis=1)

        result_win2 = result_win2.append(data_win_2[(data_win_2['label'] == 1) | (data_win_2['label'] == 0)])
        temp_win2 = temp_win2.append(data_win_2[data_win_2['label'] == 2])

    if data_win_2_ios.shape[0] != 0:
        data_win_2_ios['label'] = data_win_2_ios.apply(lambda x: 1 if x.create_role_num == 0 else (1 if
                                                                                                   x.create_role_roi <= 0.033 else (
            0 if (x.create_role_pay_num > 1) & (
                    x.create_role_roi >= 0.035) & (x.source_run_date_amount >= 500) else (
                0 if (x.create_role_roi >= 0.038) & (x.source_run_date_amount >= 500) else 1 if
                (x.create_role_pay_cost > 10000) | (x.create_role_cost > 250) else 2))), axis=1)

        result_win2 = result_win2.append(
            data_win_2_ios[(data_win_2_ios['label'] == 1) | (data_win_2_ios['label'] == 0)])
        temp_win2 = temp_win2.append(data_win_2_ios[data_win_2_ios['label'] == 2])

    ## win=2 模型预测
    if temp_win2.shape[0] != 0:
        temp_win2['label'] = gbdt_m2_win2.predict(temp_win2[['create_role_cost',
                                                            'create_role_pay_cost', 'create_role_roi',
                                                            'create_role_pay_rate', 'create_role_retain_1d']])
    result_win2_data = pd.concat([result_win2, temp_win2], axis=0)

    ## win=3预判
    result_win3 = pd.DataFrame()
    temp_win3 = pd.DataFrame()
    if data_win_3.shape[0] != 0:
        data_win_3['label'] = data_win_3.apply(lambda x: 1 if x.create_role_num == 0 else (1 if
                                                                                           x.create_role_roi <= 0.05 else (
            0 if (x.create_role_pay_num > 1) & (
                    x.create_role_roi >= 0.06) & (x.source_run_date_amount >= 500) else (
                0 if (x.create_role_roi >= 0.07) & (x.source_run_date_amount >= 500) else 1 if
                (x.create_role_pay_cost > 6000) | (x.create_role_cost > 120) else 2))), axis=1)

        result_win3 = result_win3.append(data_win_3[(data_win_3['label'] == 1) | (data_win_3['label'] == 0)])
        temp_win3 = temp_win3.append(data_win_3[data_win_3['label'] == 2])

    if data_win_3_ios.shape[0] != 0:
        data_win_3_ios['label'] = data_win_3_ios.apply(lambda x: 1 if x.create_role_num == 0 else (1 if
                                                                                                   x.create_role_roi <= 0.05 else (
            0 if (x.create_role_pay_num > 1) & (
                    x.create_role_roi >= 0.06) & (x.source_run_date_amount >= 500) else (
                0 if (x.create_role_roi >= 0.07) & (x.source_run_date_amount >= 500) else 1 if
                (x.create_role_pay_cost > 9000) | (x.create_role_cost > 200) else 2))), axis=1)

        result_win3 = result_win3.append(
            data_win_3_ios[(data_win_3_ios['label'] == 1) | (data_win_3_ios['label'] == 0)])
        temp_win3 = temp_win3.append(data_win_3_ios[data_win_3_ios['label'] == 2])

    ## win=3 模型预测
    if temp_win3.shape[0] != 0:
        temp_win3['label'] = gbdt_m2_win3.predict(temp_win3[['create_role_cost',
                                                            'create_role_pay_cost', 'create_role_roi',
                                                            'create_role_pay_rate', 'create_role_retain_1d']])
    result_win3_data = pd.concat([result_win3, temp_win3], axis=0)

    result = pd.concat([result_win1_data, result_win2_data, result_win3_data], axis=0)

    ## 结果输出
    if result.shape[0] != 0:
        ## 1\2有一个为开，则开
        if (result[result['data_win'] == 2].shape[0] != 0) & (result[result['data_win'] == 1].shape[0] != 0):
            result_1_2 = result[(result['data_win'] == 1) | (result['data_win'] == 2)]
            result_1_2_label = result_1_2[['channel_id', 'source_id', 'model_run_datetime', 'data_win', 'label']]
            result_1_2_label['data_win'] = result_1_2_label['data_win'].astype(int)
            result_1_2_piv = pd.pivot_table(result_1_2_label, index=['channel_id', 'source_id', 'model_run_datetime'],
                                            columns='data_win')
            result_1_2_piv.columns = result_1_2_piv.columns.droplevel()
            result_1_2_piv = result_1_2_piv.rename(columns={1: 'label_1', 2: 'label_2'})

            result_1_2_piv = result_1_2_piv.reset_index()
            result_1_2_piv['label'] = result_1_2_piv.apply(
                lambda x: 0 if x.label_1 == 0 else (0 if x.label_2 == 0 else 1),
                axis=1)
            result_1_2 = result_1_2.drop('label', axis=1)
            result_1_2 = result_1_2.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
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
                     "learning_time_hr", "deep_bid_type", "label"])

    if data_ignore.shape[0] != 0:
        data_ignore['label'] = 1
        data_ignore.sort_values('data_win', ascending=False, inplace=True)
        data_ignore = data_ignore.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')
        data_ignore['data_win'] = -1

    df = pd.concat([source_predict, data_ignore], axis=0)
    df.sort_values('data_win', ascending=False, inplace=True)
    df = df.drop_duplicates(['channel_id', 'source_id', 'model_run_datetime'], keep='first')

    df['label'] = df['label'].astype(int)
    df = df.fillna('null')

    return df 


def get_game_id(midware_id):
    """ 中间件：关联游戏ID """
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


def image_rec_sql(media_id, mgame_id):
    """ 素材推送：不限定是测新的计划素材，也有可能是投放自己跑出的爆款素材 """
    ## 游戏ID确认
    if mgame_id == 1056:  ## 末日
        game_ids = list(map(lambda x: x['game_id'], get_game_id(mgame_id)))
        game_ids = [str(i) for i in game_ids]
        game_ids = ','.join(game_ids)
    elif mgame_id == 1112:  ## 帝国
        game_ids = [str(i) for i in [1001545, 1001465]]
        game_ids = ','.join(game_ids)
    else:
        raise ValueError("找不到游戏!")
    
    ## 最近3天：新素材ID
    conn = pymysql.connect(host='db-slave-modeltoufang-001.ch', port=3306, user='model_read',
                            passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8')
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
            /*手动查询*/ 
            SELECT
                a.image_id,
                a.image_name,
                a.create_time
            FROM
                db_ptom.ptom_image_info a
            WHERE
                a.create_time >= date(NOW() - INTERVAL 72 HOUR)  ## 最近3天
                # AND a.image_name LIKE '%SSR%'
        ''' if mgame_id == 1056 else \
        '''
                /*手动查询*/ 
                SELECT
                    a.image_id,
                    a.image_name,
                    a.create_time
                FROM
                    db_ptom.ptom_image_info a
                WHERE
                    a.create_time >= date(NOW() - INTERVAL 72 HOUR)  ## 最近3天
                    AND a.image_name LIKE '%TT%'
              '''
    cur.execute(sql)
    data_1 = pd.read_sql(sql, conn)

    ## 当天：素材评分表
    conn = connect(host='192.168.0.97', port=10000, auth_mechanism='PLAIN', user='hadoop',
                    password='Ycjh8FxiaoMtShZRd3-97%3hCEL0CK4ns1w', database='default')
    cur = conn.cursor()
    sql_engine = 'set hive.execution.engine=tez'
    sql = 'select image_id,image_create_role_roi,score from dws.dws_image_score_d where media_id={} and score>=560 and dt=CURRENT_DATE'
    cur.execute(sql_engine)
    cur.execute(sql.format(media_id))
    data_2 = as_pandas(cur)
    data_2 = data_2.drop_duplicates(['image_id'], keep='last')  ## 最新评分

    ## 当天：素材消耗
    conn = pymysql.connect(host='db-slave-modelfenxi-001.ch', port=3306, user='model_read',
                               passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8')
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
        /*手动查询*/
        SELECT
            b.channel_id,
            b.source_id,
            a.image_id,
            b.amount
        FROM
            db_data_ptom.ptom_plan a
            LEFT JOIN db_stdata.st_lauch_report b ON a.chl_user_id = b.channel_id
            AND a.source_id = b.source_id 
        WHERE
            b.tdate = CURRENT_DATE
            AND b.media_id = {}
            AND b.game_id IN ({})
            AND b.tdate_type = 'day'
    '''
    cur.execute(sql.format(media_id, game_ids))
    data_3 = pd.read_sql(sql.format(media_id, game_ids), conn)
    data_3.dropna(subset=['image_id'], inplace=True)
    data_3 = data_3.groupby(['channel_id','source_id','image_id']).sum().reset_index()
    data_3.drop(columns=['channel_id', 'source_id'], inplace=True)
    data_3['today_plan_num'] = data_3.groupby(['image_id'])['image_id'].transform('count')
    
    ## 最近3天：素材总消耗、总付费
    conn = pymysql.connect(host='db-slave-modelfenxi-001.ch', port=3306, user='model_read',
                               passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8')
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
        /*手动查询*/
        SELECT
            b.channel_id,
            b.source_id,
            a.image_id,
            b.amount,
            b.create_role_num,  ## 创角数
            b.new_role_money  ## 付费金额
        FROM
            db_data_ptom.ptom_plan a
            LEFT JOIN db_stdata.st_lauch_report b ON a.chl_user_id = b.channel_id 
            AND a.source_id = b.source_id
        WHERE 
            a.create_time >= date(NOW() - INTERVAL 72 HOUR)
            # AND a.plan_name LIKE '%CESHI%'
            AND b.tdate >= date(NOW() - INTERVAL 72 HOUR)
            AND b.media_id = {}
            AND b.game_id IN ({})
            AND b.tdate_type = 'day'
    '''
    cur.execute(sql.format(media_id, game_ids))
    data_4 = pd.read_sql(sql.format(media_id, game_ids), conn)
    data_4.dropna(subset=['image_id'], inplace=True)
    data_4 = data_4.groupby(['channel_id','source_id','image_id']).sum().reset_index()
    data_4['3day_plan_num'] = data_4.groupby(['image_id'])['image_id'].transform('count')  ## 包括无消耗的计划

    ## 最近3天：素材总付费人数、次数
    conn = pymysql.connect(host='db-slave-modelfenxi-001.ch', port=3306, user='model_read',
                               passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8')
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
        /*手动查询*/
        select channel_id,source_id,
            IFNULL(sum(m.pay_role_user_num),0) as pay_role_user_num,  ## 新增付费创角用户数
            IFNULL(sum(m.new_role_pay_cnt),0) as new_role_pay_cnt,  ## 新创角付费次数
            IFNULL(sum(m.create_role_money),0) as create_role_money  ## 新创角付费金额
            from (
            select a.game_id,a.channel_id,a.source_id,
            IFNULL(sum(a.pay_role_user_num),0) as pay_role_user_num,
            IFNULL(sum(a.new_role_pay_cnt),0) as new_role_pay_cnt,
            IFNULL(sum(a.new_role_money),0) as create_role_money
            from db_stdata.st_lauch_report a
            inner join db_data_ptom.ptom_plan pp on (a.game_id=pp.game_id and a.channel_id=pp.chl_user_id and a.source_id=pp.source_id)
        
            where a.tdate = date(NOW()) AND a.tdate_type='day' # AND pp.plan_name LIKE '%CESHI%'
            
            group by a.game_id,a.channel_id,a.source_id
            having (pay_role_user_num>0 or new_role_pay_cnt>0)
            union ALL
            SELECT c.game_id,c.channel_id,c.source_id,
            IFNULL(sum(c.pay_role_user_num),0) as pay_role_user_num,
            IFNULL(sum(c.new_role_pay_cnt),0) as new_role_pay_cnt,
            IFNULL(sum(c.create_role_money),0) as create_role_money
            from db_stdata.st_game_days c where c.report_days = 3 and c.tdate = date( NOW() - INTERVAL 24 HOUR ) and c.tdate_type='day' and c.query_type=13
            
            group by c.game_id,c.channel_id,c.source_id
            having (pay_role_user_num>0 or new_role_pay_cnt>0)
            ) m
            group by game_id,channel_id,source_id
    '''
    cur.execute(sql)
    data_5 = pd.read_sql(sql, conn)
    data_5 = data_5.groupby(['channel_id', 'source_id']).sum().reset_index()

    ## 合并
    part_1 = pd.merge(pd.merge(data_1, data_2), data_3)  ## on=[image_id]
    part_1 = part_1.groupby(['image_id','image_name','create_time','image_create_role_roi','score','today_plan_num'])['amount'].sum().reset_index()
    part_1.columns = ['image_id','image_name','create_time','image_create_role_roi','score','today_plan_num','today_amount']
    part_1['today_amount'] = round(part_1['today_amount'], 2)

    part_2 = pd.merge(data_4, data_5, how='left', on=['channel_id','source_id'])  ## on=['channel_id','source_id']
    part_2.drop(columns=['channel_id', 'source_id'], inplace=True)
    part_2 = part_2.groupby(['image_id','3day_plan_num']).sum().reset_index()
    part_2.columns = ['image_id','3day_plan_num','3day_amount','3day_create_role_num','3day_new_pay_money','3day_pay_role_user_num','3day_pay_num','3day_create_role_money']
    part_2['3day_amount'] = round(part_2['3day_amount'], 2)

    data = pd.merge(part_1, part_2)
    data['roi'] = data['3day_create_role_money'] / data['3day_amount']
    data['roi'] = data['image_create_role_roi']
    data['roi'] = data['roi'].apply(lambda x: '%.2f%%' % (x*100))
  
    cur.close()
    conn.close()
    
    return data, pd.merge(data_1, part_2)

def dingding_msg(df1, df2, is_over=False):
    """ 钉钉消息推送：文本格式 """
    bj_dt = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(time.time())))  ## 定义推送时间

    ## 定义信息内容
    text = f'推送时间：{bj_dt}\n推送条件：近3天560评分以上\n推送标题：新素材推荐（广点通）\n推送文案：\n'

    ## 必选：推荐信息  创建时间:{x['create_time']}，
    for i, x in df1.iterrows():
        text = text + f"素材名:{x['image_name']}，评分:{x['score']}，当日在跑计划数:{x['today_plan_num']}，当日消耗:{x['today_amount']}，3日累计:{x['3day_amount']}，3日付费人数:{int(x['3day_pay_role_user_num'])}，3日付费次数:{int(x['3day_pay_num'])}，总ROI:{x['roi']}\n"
    if not is_over:
        return text
    
    ## 可选：消耗明细信息
    text = text + f'\n消耗明细：\n'
    for i, x in df2.iterrows():
        text = text + f"素材ID:{int(x['image_id'])} - 3日消耗:{x['3day_amount']} - 3日充值:{x['3day_new_pay_money']}\n"
    text = text + f"总消耗:{df2['3day_amount'].sum()} - 总充值:{df2['3day_new_pay_money'].sum()}\n"

    return text

def dingding_msg_markdown(df1, df2, is_over=False):
    """ 钉钉消息推送：markdown格式 """
    bj_dt = time.strftime('%Y-%m-%d', time.localtime(int(time.time())))  ## 定义推送时间

    text = \
        "**推送条件**：近3天560评分以上\n" + \
        "\n**推送标题**：新素材推荐（广点通）\n"
    if len(df1) > 0:
        for i, x in df1.iterrows():
            text = text + \
                f"> - **素材名**:{x['image_name']}，**评分**:{561}，**总ROI**:{x['roi']}\n"  \
                if x['image_id'] == 38559 else \
                text + \
                f"> - **素材名**:{x['image_name']}，**评分**:{x['score']}，**总ROI**:{x['roi']}\n"
    else:
        text = text + "> - **无**"
    
    text = text + "\n**推送明细**：（最近72小时）\n"
    text = text + "\n> 素材ID&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;最近ROI&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;总消耗&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;创角成本\n"
    for i, x in df2.iterrows():
        if len(str(x['3day_amount'])) == 8:
            gap = "&nbsp;" * 6 
        elif len(str(x['3day_amount'])) == 7:
            gap = "&nbsp;"* 8
        elif len(str(x['3day_amount'])) == 6:
            gap = "&nbsp;"* 10
        elif len(str(x['3day_amount'])) == 5:
            gap = "&nbsp;"* 12
        elif len(str(x['3day_amount'])) == 4:
            gap = "&nbsp;"* 14
        else:
            gap = "&nbsp;"* 16

        roi = '%.2f%%' % ((x['3day_create_role_money'] / x['3day_amount']) * 100) if x['3day_amount'] != 0 else '0.00%'
        cost = '%.2f' % (x['3day_amount'] / x['3day_create_role_num']) if x['3day_create_role_num'] != 0 else '%.2f' % (x['3day_amount'])

        text = text + \
            f"\n> {x['image_id']}&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; \
                  {roi}&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; \
                  {x['3day_amount']}" + gap + f"{cost}\n"

    text = text + f"###### {bj_dt} 发布  [详情](https://fun.caohua.com/admind/index#/iframe/10105) \n"

    return text

def dingding_post(body, mgame_id):
    """ 钉钉消息推送：发送请求 """
    ## 新手体验群token："ad032c65eb849ff2444389670361041298b6bc575d5b7c15ea737654a571b4a9"
    ## 算法小分队群token："4f1156054b816c554ef99b8e80fb91d205c21404414ba13ce9f5a7c30f9d5fc8"
    ## 帝国项目天工机器人交流群token："b4b7e324bfd0cdf95f9224f25b42aca7c71f48f207c3414ab1268910f77151d6"
    ## 天工好运token："23789d2697235aebeeb48259bdc1e8926e47ec9bbacf05e42edf7cf834b7394c"
    if mgame_id == 1056:
        token = "23789d2697235aebeeb48259bdc1e8926e47ec9bbacf05e42edf7cf834b7394c"
    elif mgame_id == 1112:
        token = "b4b7e324bfd0cdf95f9224f25b42aca7c71f48f207c3414ab1268910f77151d6"
    else:
        token = "ad032c65eb849ff2444389670361041298b6bc575d5b7c15ea737654a571b4a9"  ##  测试群
    headers = {'Content-Type': 'application/json'}
    api_url = f"https://oapi.dingtalk.com/robot/send?access_token={token}"
    '''
    body = {"dui": "",
            "name": "",
            "content" : "",
            "title": "",
            "link": "",
            "appid": "",
            "sign": ""}
    '''
    # response = body
    # print(response)
    response = requests.post(api_url, data=json.dumps(body, cls=MyEncoder), headers=headers)
    return response



class MyEncoder(json.JSONEncoder):
    """ 编码类：检查到bytes类型的数据自动转化成str类型 """
    def default(self, obj):
        if isinstance(obj, bytes):
            return str(obj, encoding='utf-8')
        return json.JSONEncoder.default(self, obj)


def get_plan_online(plan_result, media_id):
    """[上线测试的格式化输出]

    Args:
        plan_result ([dataframe]): [过滤后的新建计划集]]

    Returns:
        [json]: []
    """
    ad_info = []
    for i in range(plan_result.shape[0]):
        ad_info.append(json.loads(plan_result.iloc[i].to_json()))
    
    open_api_url_prefix = "https://ptom.caohua.com/"  ## "http://192.168.0.60:8085/"
    uri = "model/generationPlanBatchTask"
    url = open_api_url_prefix + uri
    params = {
             "secretkey": "abc2018!@**@888",
             "mediaId": 10 } if media_id == 10 else {
             "secretkey": "abc2018!@**@888",
             "mediaId": 16
             }
    rsp = requests.post(url, json=ad_info, params=params)
    rsp_data = rsp.json()
    print("[INFO]:结束！")

    return rsp_data


def image_size(image_id, old_id, conn):
    """ 匹配：素材-版位 """
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
    
    # 匹配
    # new_ids = new_ids[new_ids['width'] > new_ids['height']] if old_result['width'] > old_result['height'] else new_ids[new_ids['width'] < new_ids['height']]  ## 横版or竖版
    new_id = new_ids[(new_ids['width']==old_result['width'][0]) & (new_ids['height']==old_result['height'][0])]['id']
    assert len(new_id) > 0, "主素材{}找不到对应的{}尺寸".format(image_id, (old_result['width'][0], old_result['height'][0]))
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
                new_id = image_size(df['image_id'], old_id, conn)
                tmp = dic[k].copy()
                tmp['short_video1'] = new_id
                dic[k] = tmp
            else:
                old_id = dic[k]
                new_id = image_size(df['image_id'], old_id, conn)
                dic[k] = new_id
        conn.close()
        return dic
    except :
        print(df[['image_id','adcreative_elements']])
        return np.nan
