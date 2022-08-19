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
import random
import json
import time
import ast

from config import get_var
dicParam = get_var()


def getRealData(conn1, media_id, mgame_id):
    """ 初始采集：明细数据 """
    game_ids = list(map(lambda x: x['game_id'], get_game_id(mgame_id)))
    game_ids = [str(i) for i in game_ids]
    game_ids = ','.join(game_ids)
    
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
                    (case when IFNULL(sum(a.create_role_num),0)=0 then IFNULL(sum(a.amount),0) else IFNULL(sum(a.amount),0)*1.0/IFNULL(sum(a.create_role_num),0) end) as create_role_cost,
                    (case when IFNULL(sum(a.amount),0)=0 then IFNULL(sum(a.sum_money),0) else IFNULL(sum(a.sum_money),0)*1.0/IFNULL(sum(a.amount),0) end) as create_role_roi
                from db_stdata.st_lauch_report a where a.tdate>=date( NOW() - INTERVAL 120 HOUR ) and a.tdate_type='day' and a.amount > 0 and a.media_id={}
                group by a.game_id,a.channel_id,a.source_id) c
                    
                        inner join db_data_ptom.ptom_plan pp on (c.product_id=pp.game_id and c.chl_user_id=pp.chl_user_id and c.source_id=pp.source_id)
                    where pp.launch_op_id = 13268
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
        
            where a.tdate_type='day' AND a.media_id={} AND pp.launch_op_id = 13268
            and a.game_id in ({})
            group by a.game_id,a.channel_id,a.source_id
            having (new_role_money>0 or pay_role_user_num>0)
            union ALL
            SELECT c.game_id,c.channel_id,c.source_id,sum(c.create_role_money) new_role_money,
            IFNULL(sum(c.pay_role_user_num),0) as pay_role_user_num
            from db_stdata.st_game_days c where c.report_days = 5 and c.tdate = date( NOW() - INTERVAL 24 HOUR ) and c.tdate_type='day' and c.query_type=13 and c.media_id={}
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
                    where m.tdate>=date( NOW() - INTERVAL 120 HOUR )  and m.tdate_type='day' and m.query_type=19 and m.media_id={} and m.server_id=-1 and m.retain_date in (2)
                    and pp.launch_op_id = 13268
                    group by m.game_id,m.channel_id,m.source_id,m.retain_date
                ) cc
                )bb
                group by bb.game_id,bb.channel_id,bb.source_id
            ) B ON A.product_id=B.game_id AND A.chl_user_id=B.channel_id AND A.source_id=B.source_id
    '''
    result = pd.read_sql(sql.format(media_id, media_id, game_ids, media_id, media_id), conn1)
    result.dropna(subset=['image_id'], inplace=True)
    result['image_id'] = result['image_id'].astype(int)
    result.drop(["learning_type","learning_time_dt","learning_time_hr"], axis=1, inplace=True)
    result.columns = ["plan_id","platform","plan_name","create_time","media_id","image_id","game_id","channel_id","source_id",
                      "source_run_date_amount","create_role_num","create_role_cost","create_role_roi","two_day_create_retain",
                      "new_role_money","new_role_rate","create_role_pay_num","create_role_pay_cost","pay_role_rate"]
    return result


def plan_detail_toutiao(mgame_id):
    """ 中间采集：头条计划参数 """
    game_ids = list(map(lambda x: x['game_id'], get_game_id(mgame_id)))
    game_ids = [str(i) for i in game_ids]
    game_ids = ','.join(game_ids)

    conn = pymysql.connect(host=dicParam['DB_SLAVE_TOUFANG_HOST'], port=int(dicParam['DB_SLAVE_TOUFANG_PORT']), user=dicParam['DB_SLAVE_TOUFANG_USERNAME'],
                           passwd=dicParam['DB_SLAVE_TOUFANG_PASSWORD'], db=dicParam['DB_SLAVE_TOUFANG_DATABASE'])
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)

    # 1、计划参数
    sql = '''
    /*手动查询*/
        SELECT
                * 
            FROM
                db_ptom.ptom_third_plan p
            WHERE
                game_id IN ({})
                AND media_id = 10
                AND create_time>=date( NOW() - INTERVAL 168 HOUR )
    '''
    finalSql = sql.format(game_ids)
    plan_param = pd.read_sql(finalSql, conn)
    
    # 2、计划创意
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
            b.create_time >= date( NOW() - INTERVAL 168 HOUR )
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

    # 3、计划素材
    conn = pymysql.connect(host=dicParam['DB_SLAVE_FENXI_HOST'], port=int(dicParam['DB_SLAVE_FENXI_PORT']), user=dicParam['DB_SLAVE_FENXI_USERNAME'],
                           passwd=dicParam['DB_SLAVE_FENXI_PASSWORD'])
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
            a.create_time >= date(NOW() - INTERVAL 168 HOUR)
            AND a.media_id = 10
            AND a.game_id IN ({})
    '''
    cur.execute(sql.format(game_ids))
    plan_image = pd.read_sql(sql.format(game_ids), conn)

    cur.close()
    conn.close()

    # 4、合并
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
    for col in ['ios_osv']:
        if col in plan_param.columns:
            pass
        else:
            plan_param[col] = np.nan
    plan_param = plan_param[['ad_account_id', 'game_id', 'channel_id', 'source_id',
                             'create_time', 'smart_bid_type', 'hide_if_exists', 'budget',
                             'delivery_range', 'adjust_cpa', 'inventory_type', 'hide_if_converted',
                             'flow_control_mode', 'schedule_time', 'cpa_bid', 'auto_extend_enabled',
                             'gender', 'city', 'platform', 'launch_price',
                             'retargeting_tags_exclude', 'interest_categories',
                             'ac', 'android_osv', 'location_type', 'retargeting_tags_include',
                             'ios_osv', 'interest_action_mode', 'age',
                             'deep_bid_type', 'roi_goal']] if 'roi_goal' in plan_param.columns else \
                              plan_param[['ad_account_id', 'game_id', 'channel_id', 'source_id',
                             'create_time', 'smart_bid_type', 'hide_if_exists', 'budget',
                             'delivery_range', 'adjust_cpa', 'inventory_type', 'hide_if_converted',
                             'flow_control_mode', 'schedule_time', 'cpa_bid', 'auto_extend_enabled',
                             'gender', 'city', 'platform', 'launch_price',
                             'retargeting_tags_exclude', 'interest_categories',
                             'ac', 'android_osv', 'location_type', 'retargeting_tags_include',
                             'ios_osv', 'interest_action_mode', 'age',
                             'deep_bid_type']]

    plan_param['inventory_type'] = plan_param['inventory_type'].apply(lambda x: [] if len(x) == 7 else x)
    plan_param = plan_param.mask(plan_param.applymap(str).eq('NONE'))
    plan_param['retargeting_type'] = np.nan
    plan_param['plan_auto_task_id'] = "11002,10998,12098"
    plan_param['district'] = 'CITY'
    plan_param['op_id'] = 13268
    plan_param['operation'] = 'disable'  # TODO：测试阶段尽量手动开
    plan_param['flag'] = plan_param['smart_bid_type'].apply(lambda x:'CEXIN_COPY_NB' if x == 'SMART_BID_NO_BID' else 'CEXIN_COPY')
    plan_param['convertIndex'] = plan_param['deep_bid_type'].apply(lambda x: 23 if x == 'BID_PER_ACTION'
                                                                            else 24 if x == 'ROI_COEFFICIENT'
                                                                                else np.nan)
    # TODO：（后续新游新包，这里需要追加...）
    plan_param['web_url'] = plan_param['game_id'].apply(lambda x:'https://www.chengzijianzhan.com/tetris/page/7090839861226012685/'
                                                            if x == 1001917 else
                                                            ('https://www.chengzijianzhan.com/tetris/page/7091531861361098782/'
                                                            if x == 1001888 else
                                                            ('https://www.chengzijianzhan.com/tetris/page/7094874276537434149/'
                                                            if x == 1001890 else
                                                            ('https://www.chengzijianzhan.com/tetris/page/7044279938891825160/'
                                                            if x == 1001703 else ''))))

    plan = pd.merge(plan_param, plan_creative, on=['channel_id', 'source_id'], how='left')
    plan = pd.merge(plan, plan_image, on=['channel_id', 'source_id'], how='left')

    plan.dropna(subset=['image_id'], inplace=True)

    return plan


def plan_detail_gdt(mgame_id):
    """ 中间采集：广点通最近3天的计划详情 """
    ## 游戏ID确认
    game_ids = list(map(lambda x: x['game_id'], get_game_id(mgame_id)))
    game_ids = [str(i) for i in game_ids]
    game_ids = ','.join(game_ids)

    conn = pymysql.connect(host=dicParam['DB_SLAVE_TOUFANG_HOST'], port=int(dicParam['DB_SLAVE_TOUFANG_PORT']), user=dicParam['DB_SLAVE_TOUFANG_USERNAME'],
                           passwd=dicParam['DB_SLAVE_TOUFANG_PASSWORD'], db=dicParam['DB_SLAVE_TOUFANG_DATABASE'])
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
    plan_param['op_id'] = 13268
    plan_param['flag'] = 'CESHI_GDT'
    plan_param['operation'] = 'disable'  ## 测试阶段尽量手动开
    plan_param['game_name'] = '幸存者挑战' ## TODO:新字段
    plan_param['platform'] = 1
    
    plan_param['game_id'] = 0
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


def plan_pay_sql(df, media_id, mgame_id, ad_account_dict):
    """ 动作：优质计划复制 """
    if not len(df):
        raise ValueError("无满足条件的优质计划!")
    
    # 1、匹配计划
    plan = plan_detail_toutiao(mgame_id) if media_id == 10 else plan_detail_gdt(mgame_id)
    plan = pd.merge(df[['plan_name','channel_id','source_id']], plan, how='inner', on=['channel_id', 'source_id'])
    if not len(plan):
        raise ValueError("无满足条件的优质计划!")
    
    print(plan['plan_name'].value_counts())
    # 2、复制计划
    # plan = plan.groupby(['image_id']).apply(lambda x:pd.DataFrame(np.repeat(x.values, 1, axis=0), columns=plan.columns)).reset_index(drop=True)  ## 复制1次
    plan['convertIndex'] = plan['convertIndex'].astype(int)
    plan['image_id'] = plan['image_id'].astype(int)
    plan['budget'] = plan['budget'].apply(np.ceil)

    # 3、账号更换
    plan.drop(['plan_name','channel_id','source_id','create_time'], axis=1, inplace=True)
    # plan['ad_account_id'] = plan['game_id'].apply(lambda x:random.choice(ad_account_dict[x]))  # !!!!测新账号
    # plan['adjust_cpa'] = plan['adjust_cpa'].apply(lambda x:int(x) if (x==0.0 or x==1.0) else x)  
    # for col in plan.columns:
    #     print(col, type(plan[col].iloc[0]))
    return plan, df['plan_id'].values.tolist()


def image_plan_sql(df, data, media_id, mgame_id):
    """ 动作：素材消耗控制与计划复制 """
    # TODO：源码暂时没必要那么复杂
    return []


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


def image_rec_sql(media_id, mgame_id):
    """ 素材推送：指标统计 """
    # 游戏ID确认
    if mgame_id == 1056:  ## 末日
        game_ids = list(map(lambda x: x['game_id'], get_game_id(mgame_id)))
        game_ids = [str(i) for i in game_ids]
        game_ids = ','.join(game_ids)
    elif mgame_id == 1112:  ## 帝国
        game_ids = [str(i) for i in [1001862,1001788]]
        game_ids = ','.join(game_ids)
    elif mgame_id == 1136:  ## 大东家
        game_ids = [str(i) for i in [1001917,1001888,1001890,1002005]]
        game_ids = ','.join(game_ids)
    else:
        raise ValueError("找不到游戏!")
    
    # 测新素材
    def imaging(media_id, game_ids):
        conn = pymysql.connect(host=dicParam['DB_SLAVE_TOUFANG_HOST'], port=int(dicParam['DB_SLAVE_TOUFANG_PORT']), user=dicParam['DB_SLAVE_TOUFANG_USERNAME'],
                               passwd=dicParam['DB_SLAVE_TOUFANG_PASSWORD'], db=dicParam['DB_SLAVE_TOUFANG_DATABASE'])
        cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
        sql = '''
        /*手动查询*/
            SELECT
                p.image_id,
                a.image_name,
                a.create_time
            FROM
                db_ptom.ptom_plan p LEFT JOIN db_ptom.ptom_image_info a
                ON p.image_id = a.image_id
            WHERE
                p.game_id IN ({})
                # AND p.plan_name like 'TT_%'  # 'CX_'
                AND p.media_id = {}
                AND p.launch_op_id = 13268 # 13678
                AND p.create_time>=date( NOW() - INTERVAL 120 HOUR )
            GROUP BY
                p.image_id
        '''
        cur.execute(sql.format(game_ids, media_id))
        data_1 = pd.read_sql(sql.format(game_ids, media_id), conn)
        data_1['image_name'] = data_1['image_name'].apply(lambda x:x.split('=')[0])
        data_1['image_name'] = data_1['image_name'].apply(lambda x:x.split('-')[0])
        # 关闭链接
        cur.close()
        conn.close()
        return data_1

    # 素材评分
    def scoring(media_id):
        conn = connect(host=dicParam['HIVE_HOST'], port=int(dicParam['HIVE_PORT']), auth_mechanism=dicParam['HIVE_AUTH_MECHANISM'], user=dicParam['HIVE_USERNAME'],
                       password=dicParam['HIVE_PASSWORD'], database=dicParam['HIVE_DATABASE'])
        cur = conn.cursor()
        sql_engine = 'set hive.execution.engine=tez'
        sql = 'select image_id,image_create_role_roi,score from dws.dws_image_score_d where media_id={} and dt=CURRENT_DATE and score>0'
        cur.execute(sql_engine)
        cur.execute(sql.format(media_id))
        data_2 = as_pandas(cur)
        data_2 = data_2.drop_duplicates(['image_id'], keep='last')  # 最新评分
        # 关闭链接
        cur.close()
        conn.close()
        return data_2

    # 素材明细
    def detailing(media_id, image_ids):
        conn = pymysql.connect(host=dicParam['DB_SLAVE_FENXI_HOST'], port=int(dicParam['DB_SLAVE_FENXI_PORT']), user=dicParam['DB_SLAVE_FENXI_USERNAME'],
                               passwd=dicParam['DB_SLAVE_FENXI_PASSWORD'])
        cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
        sql = '''
            /*手动查询*/
            SELECT
                b.image_id,
                IFNULL(sum(a.create_role_num),0) as create_role_num,
                IFNULL(sum(a.pay_role_user_num),0) as pay_role_user_num,
                IFNULL(sum(a.amount),0) as amount,
                IFNULL(sum(a.new_role_money),0) as new_role_money,
                IFNULL(sum(a.sum_money),0) as sum_money,
                a.tdate
            FROM
                db_stdata.st_lauch_report a LEFT JOIN db_data_ptom.ptom_plan b ON a.channel_id = b.chl_user_id 
                AND a.source_id = b.source_id
            WHERE
                a.tdate>=date( NOW() - INTERVAL 72 HOUR )
                AND a.tdate_type='day'
                AND a.media_id={}
                AND b.image_id={}
            GROUP BY
                a.tdate
        '''
        result = pd.DataFrame()
        for image_id in image_ids:
            cur.execute(sql.format(media_id, image_id))
            result = result.append(pd.read_sql(sql.format(media_id, image_id), conn))
        result.reset_index(drop=True, inplace=True)

        # 累计
        data_3 = result.groupby('image_id')[['create_role_num','pay_role_user_num','amount','new_role_money','sum_money']].sum().reset_index()
        data_3.loc[data_3['pay_role_user_num'] == 0, 'pay_role_user_num'] = 1
        # 每日
        result['roi_day'] = result['new_role_money'] / result['amount']
        result['roi_day'][np.isinf(result['roi_day'])] = np.nan
        result.dropna(subset=['roi_day'], axis=0, inplace=True)
        data_4 = result.groupby('image_id')['roi_day'].mean().reset_index()  # [image_id, roi_day]
        
        # 关闭链接
        cur.close()
        conn.close()
        return pd.merge(data_3, data_4)
   
    def remaining(image_id, dt):  
        conn = pymysql.connect(host=dicParam['DB_SLAVE_FENXI_HOST'], port=int(dicParam['DB_SLAVE_FENXI_PORT']), user=dicParam['DB_SLAVE_FENXI_USERNAME'],
                               passwd=dicParam['DB_SLAVE_FENXI_PASSWORD'])
        cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
        sql = '''
            select bb.image_id,
            IFNULL(sum(bb.next_day_retain),0) as next_day_retain,
            IFNULL(sum(bb.three_day_retain),0) as three_day_retain,
            IFNULL(sum(bb.seven_day_retain),0) as seven_day_retain,
            bb.tdate
            from (
            select cc.image_id,
            (case when retain_date=2 then retain_rate end) as next_day_retain,
            (case when retain_date=3 then retain_rate end) as three_day_retain,
            (case when retain_date=7 then retain_rate end) as seven_day_retain,
            cc.tdate
            from (
            select pp.image_id,m.retain_date,DATE_FORMAT(m.tdate,'%Y-%m-%d') as tdate,
            (case when IFNULL(sum(m.new_payrole_num),0)=0 then 0 else IFNULL(sum(m.new_payrole_retain_num),0)/sum(m.new_payrole_num) end) as retain_rate
            from db_stdata.st_game_retain m
            INNER JOIN db_data_ptom.ptom_plan pp on m.game_id=pp.game_id and m.channel_id=pp.chl_user_id and m.source_id=pp.source_id
            where m.tdate='{}' and m.tdate_type='day' and m.query_type=19 and m.server_id=-1 and m.retain_date in(1,2,3,7)
            and pp.image_id={}
            group by pp.image_id,m.retain_date
            ) cc
            ) bb group by bb.image_id
        '''
        cur.execute(sql.format(dt, image_id))
        data_5 = pd.read_sql(sql.format(dt, image_id), conn)
        # 关闭链接
        cur.close()
        conn.close()
        return data_5
    
    # TODO：合并字段
    cexin_images = imaging(media_id, game_ids)  # ID、名称、新建时间
    cexin_scores = scoring(media_id)  # ID、评分、素材创角ROI
    cexin_detail = detailing(media_id, cexin_images['image_id'].values)  # ID、创角数、付费用户数、总消耗、新创角付费金额、总付费流水
    
    data = pd.merge(pd.merge(cexin_images, cexin_scores), cexin_detail)
    data.sort_values(by=['score'], ascending=False, inplace=True)
    data['cost'] = data['amount'] / data['pay_role_user_num']
    # data['roi'] = data['sum_money'] / data['amount']
    data['roi'] = data['image_create_role_roi']

    data['amount'] = data['amount'].apply(lambda x: '%.2f' % (x))
    data['cost'] = data['cost'].apply(lambda x: '%.2f' % (x))
    data['roi'] = data['roi'].apply(lambda x: '%.2f%%' % (x*100))
    data['roi_day'] = data['roi_day'].apply(lambda x: '%.2f%%' % (x*100))
    
    return data, data[data['score'] >= 580].reset_index()


def dingding_msg_markdown(df_all, df_rec, media_id, mgame_id):
    """ 消息内容：markdown格式 """
    bj_dt = time.strftime('%Y-%m-%d', time.localtime(int(time.time())))  ## 定义推送时间
    mapping_game = {1056:'末日血战', 1112:'帝国战魂', 1136:'我是大东家'}
    mapping_media = {10:'头条', 16:'广点通'}

    text = "**推送游戏**：{}（测试）\n".format(mapping_game[mgame_id]) + "\n**推送标题**：测新数据（{}）\n".format(mapping_media[media_id])
    
    if len(df_rec) > 0:
        for _, x in df_rec.iterrows():
            text =  text + \
                f"> - **素材名**:{x['image_name']}，**评分**:{x['score']}，**总ROI**:{x['roi']}\n"
    else:
        text = text + "> - **无**"
    
    text = text + "\n**指标明细**：机器人跑过的素材\n"
    text = text + "\n> 素材ID&nbsp; \
                       素材名&nbsp; \
                       上新时间&nbsp; \
                       累计消耗(3日)&nbsp; \
                       付费成本(3日平均)&nbsp; \
                       首日ROI(3日平均)&nbsp; \
                       累计ROI&nbsp; \
                       素材评分\n"
    for _, x in df_all.iterrows():
        text = text + \
            f"\n> {x['image_id']}&nbsp; \
                  {x['image_name']}&nbsp; \
                  {x['create_time']}&nbsp; \
                  {x['amount']}&nbsp; \
                  {x['cost']}&nbsp; \
                  {x['roi_day']}&nbsp; \
                  {x['roi']}&nbsp; \
                  {x['score']}\n"

    text = text + f"###### {bj_dt} 发布  [详情](https://fun.caohua.com/admind/index#/iframe/10105) \n"

    return text


def dingding_post(body, mgame_id):
    """ 请求参数：webhook """
    # 新手体验群token："ad032c65eb849ff2444389670361041298b6bc575d5b7c15ea737654a571b4a9"
    # 算法小分队token："4f1156054b816c554ef99b8e80fb91d205c21404414ba13ce9f5a7c30f9d5fc8"
    # 天工好运token："23789d2697235aebeeb48259bdc1e8926e47ec9bbacf05e42edf7cf834b7394c"
    # 帝国项目天工机器人token："b4b7e324bfd0cdf95f9224f25b42aca7c71f48f207c3414ab1268910f77151d6"
    # 广分天工小分队token："b00f1de3904a0221256e5475402b75aa7ebe6ca2908e5b995729cbdf486e7e01"
    if mgame_id == 1056:
        token = "ad032c65eb849ff2444389670361041298b6bc575d5b7c15ea737654a571b4a9"
    elif mgame_id == 1112:
        token = "ad032c65eb849ff2444389670361041298b6bc575d5b7c15ea737654a571b4a9"
    elif mgame_id == 1136:
        token = "ad032c65eb849ff2444389670361041298b6bc575d5b7c15ea737654a571b4a9"
    else:
        token = "ad032c65eb849ff2444389670361041298b6bc575d5b7c15ea737654a571b4a9"
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
    
    open_api_url_prefix = "https://ptom.caohua.com/"
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
    print("[INFO]：结束！")

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
    conn = pymysql.connect(host=dicParam['DB_SLAVE_TOUFANG_HOST'], port=int(dicParam['DB_SLAVE_TOUFANG_PORT']), user=dicParam['DB_SLAVE_TOUFANG_USERNAME'],
                           passwd=dicParam['DB_SLAVE_TOUFANG_PASSWORD'], db=dicParam['DB_SLAVE_TOUFANG_DATABASE'])
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


def get_game_id(midware_id):
    """ 中间件：关联游戏ID """
    conn = pymysql.connect(host=dicParam['DB_SLAVE_FENXI_HOST'], port=int(dicParam['DB_SLAVE_FENXI_PORT']), user=dicParam['DB_SLAVE_FENXI_USERNAME'],
                           passwd=dicParam['DB_SLAVE_FENXI_PASSWORD'])
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
        SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = {} AND dev_game_id IS NOT NULL 
    '''
    cur.execute(sql.format(midware_id))
    result_df = cur.fetchall()
    cur.close()
    conn.close()
    return result_df


class MyEncoder(json.JSONEncoder):
    """ 编码类：检查到bytes类型的数据自动转化成str类型 """
    def default(self, obj):
        if isinstance(obj, bytes):
            return str(obj, encoding='utf-8')
        return json.JSONEncoder.default(self, obj)