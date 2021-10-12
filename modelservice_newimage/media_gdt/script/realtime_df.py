# -*- coding:utf-8 -*-
"""
   File Name：     realtime_df.py
   Description :   计划创建相关：实时数据采集（广点通-末日）
   Author :        royce.mao
   date：          2021/7/5 17:20
"""

from impala.dbapi import connect
from impala.util import as_pandas
import pandas as pd
import numpy as np
import pymysql
import json
import os
# os.chdir(os.path.dirname(os.getcwd()))

'''
get_plan_info()：过滤了部分计划：一对多计划、纯买激活的计划
get_creative()：修改了创意输出格式要求
deep_bide_type：字段广点通媒体不再考虑  ## df = df[df['deep_bid_type'].isin(['BID_PER_ACTION', 'ROI_COEFFICIENT'])]

建计划字段完全不同了：
1）game_id 不变
   预算 - 不限制预算
2）budget_mode 为0
3）daily_budget 为0
   出价 - 出价及方式 直接从df_create随机采样
4）'optimization_goal', 'bid_strategy', 'bid_amount', 'deep_conversion_type', 'deep_conversion_behavior_spec','deep_conversion_worth_spec'
   扩量方式 直接从df_create随机采样
5）'expand_enabled'、'expand_targeting'
   定向 直接从df_create随机采样
6）'device_price', 'app_install_status', 'gender', 'game_consumption_level', 'age', 'network_type','conversion_behavior_list', 'excluded_dimension', 'location_types', 'regions','intention_targeting_tags', 'interest_category_id_list', 'interest_keyword_list','behavior_category_id_list', 'behavior_intensity','behavior_keyword_list', 'behavior_scene', 'behavior_time_window'
   创意 将df_create按image_id分组，对应image_id的创意字段只从对应image_id的df_create中随机采样
7）'image_id', 'site_set', 'deep_link_url', 'adcreative_template_id', 'page_spec', 'page_type', 'link_page_spec','link_name_type', 'link_page_type', 'promoted_object_id', 'profile_id', 'promoted_object_type','automatic_site_enabled', 'label', 'adcreative_elements'   
   时间区间 按照time_series离散取值的counts加权采样
8）time_series

'''

class DataRealTime(object):
    """ 实时数据采集类 """
    def __init__(self, ):
        pass
 
    @staticmethod
    def get_plan_info(game_id):
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
                    AND p.create_time >= date( NOW() - INTERVAL 1440 HOUR )
                    AND p.create_time <= date(NOW())
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

    @staticmethod
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

    @staticmethod
    def get_launch_report(game_id):
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
                a.create_time >= date( NOW() - INTERVAL 720 HOUR ) 
                AND b.tdate >= date( NOW() - INTERVAL 720 HOUR ) 
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

    @staticmethod
    def get_creative(game_id):
        game_id = list(map(lambda x: x['game_id'], game_id))
        game_id = [str(i) for i in game_id]
        game_id = ','.join(game_id)

        conn = pymysql.connect(host='db-slave-modeltoufang-001.ch', port=3306, user='model_read',
                               passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8')
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

    @staticmethod
    def get_now_plan_roi(game_id):
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
                b.tdate >= date( NOW() - INTERVAL 120 HOUR ) 
                AND b.tdate_type = 'day' 
                AND b.media_id = 16
                AND b.game_id IN ({}) 
                AND b.amount >= 500 
                AND b.pay_role_user_num >= 1
                AND b.new_role_money >= 65
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
        # result_df = result_df[result_df['roi'] >= 0.03]
        return result_df

    @staticmethod
    def get_score_images():
        """ image素材评分表 """
        conn = connect(host='192.168.0.97', port=10000, auth_mechanism='PLAIN', user='hadoop',
                    password='Ycjh8FxiaoMtShZRd3-97%3hCEL0CK4ns1w', database='default')
        cursor = conn.cursor()
        # sql_engine = 'set hive.execution.engine=tez'
        sql = 'select image_id,label_ids,model_run_datetime,score from dws.dws_image_score_d where media_id=16 and score>=550 and dt=CURRENT_DATE'
        # cursor.execute(sql_engine)
        cursor.execute(sql)
        result = as_pandas(cursor)
        result['label_ids'] = result['label_ids'].astype(str)
        result['label_ids'] = result['label_ids'].apply(lambda x: x.strip('-1;') if '-1' in x else x)
        result['label_ids'] = pd.to_numeric(result['label_ids'], errors='coerce')

        # 关闭链接
        cursor.close()
        conn.close()

        return result


    @staticmethod
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


    def __call__(self, cfg):
        """ 采集 """
        game_id_dict = get_game_id(1056)

        plan_info = self.get_plan_info(game_id_dict)
        image_info = self.get_image_info()
        launch_report = self.get_launch_report(game_id_dict)

        creative_info = self.get_creative(game_id_dict)
        now_plan_roi = self.get_now_plan_roi(game_id_dict)

        # 计划
        plan_info = self.get_plan_json(plan_info)
        # plan_info.dropna(subset=['cpa_bid'], inplace=True)
        # 素材
        image_info.dropna(subset=['image_id'], inplace=True)
        image_info['image_id'] = image_info['image_id'].astype(int)
        # 运营
        launch_report = launch_report
        # 创意
        creative_info['creative_param'] = creative_info['creative_param'].apply(json.loads)
        temp = creative_info['creative_param'].apply(pd.Series)
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
        plan_info_current = plan_info[plan_info['create_time'] >= pd.datetime.now() - pd.DateOffset(8)]
        image_info = image_info[image_info['image_id'].notna()]
        image_info['image_id'] = image_info['image_id'].astype(int)

        # 返回二 df_create
        df_create = pd.merge(plan_info_current, image_info, on=['channel_id', 'source_id', 'image_id'], how='left')
        df_create = pd.merge(df_create, now_plan_roi.drop(['ad_account_id'], axis=1), on=['channel_id', 'source_id'],
                            how='inner')
        df_create = df_create[df_create['site_set'].notna()]
        df_create.reset_index(drop=True, inplace=True)

        # 返回一 df
        df = pd.merge(plan_info, image_info, on=['channel_id', 'source_id', 'image_id'], how='left')
        df.dropna(subset=['image_id'], inplace=True)
        df = pd.merge(df, launch_report, on=['channel_id', 'source_id'], how='left')
        df.drop(df[df['tdate'].isna()].index, inplace=True)
        df = df[df['amount'] >= 500]

        df['plan_label'] = df.apply(lambda x: 1 if x.new_role_money / x.amount >= 0.02 else 0, axis=1)
        df['y'] = df.apply(lambda x: 1 if x.new_role_money / x.amount >= cfg.ROI or x.roi >= cfg.ROI else 0, axis=1)  # 打标签
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
        df.reset_index(drop=True, inplace=True)

        # 返回三 df_image
        df_image = self.get_score_images()
        df_image.dropna(subset=['image_id', 'score'], inplace=True)
        df_image = df_image[['image_id', 'model_run_datetime', 'score']]
        df_image.columns = ['image_id', 'create_time', 'image_score']
        df_image.drop_duplicates(inplace=True)

        df_image['create_time'] = pd.to_datetime(df_image['create_time'], format='%Y-%m-%d')
        df_image['create_time'] = df_image['create_time'].apply(lambda x: x.date())
        
        return df, df_create, df_image


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

# ==============修正函数================

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
        new_id = new_ids[(new_ids['width']==old_result['width'][0]) & (new_ids['height']==old_result['height'][0])]['id']
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
    except:
        print(df[['image_id','adcreative_elements']])
        return np.nan

def location_type(dic):
    """ 修正：地域定向 """
    try:
        dic = eval(dic)
    except:
        dic = dic
        
    dic['geo_location']['location_types'] = ['LIVE_IN'] if dic['geo_location']['location_types']==['RECENTLY_IN'] else dic['geo_location']['location_types']
    
    return dic
