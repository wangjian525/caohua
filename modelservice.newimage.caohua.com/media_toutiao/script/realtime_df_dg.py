# -*- coding:utf-8 -*-
"""
   File Name：     realtime_df.py
   Description :   计划创建相关：实时数据采集（帝国）
   Author :        royce.mao
   date：          2021/7/1 11:09
"""

from impala.dbapi import connect
from impala.util import as_pandas
import pandas as pd
import pymysql
import json
import os
# os.chdir(os.path.dirname(os.getcwd()))


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
                    * 
                FROM
                    db_ptom.ptom_third_plan p
                WHERE
                    game_id IN ({})
                    AND media_id = 10
                    AND create_time>='2021-01-01'
                    AND create_time<= date(NOW())
                                AND plan_id >= (
                                    select plan_id from db_ptom.ptom_plan
                                    where create_time >= '2021-05-07'
                                    and create_time <= '2021-05-08'
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
                a.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1112 AND dev_game_id IS NOT NULL ) 
                AND a.media_id = 10 
                AND a.create_time >= '2020-12-01' 
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
                a.create_time >= '2020-12-01' 
                AND b.tdate >= '2020-12-01' 
                AND b.tdate_type = 'day' 
                AND b.media_id = 10
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
                JSON_EXTRACT( a.creative_param, '$.ad_keywords' ) AS ad_keywords,
                JSON_EXTRACT( a.creative_param, '$.title_list' ) AS title_list,
                JSON_EXTRACT( a.creative_param, '$.third_industry_id' ) AS third_industry_id 
            FROM
                db_ptom.ptom_batch_ad_task a
                LEFT JOIN db_ptom.ptom_plan b ON a.plan_name = b.plan_name 
            WHERE
                a.media_id = 10 
                AND b.create_time >= date( NOW() - INTERVAL 720 HOUR )    # 近30天
                AND a.game_id IN ({}) 
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
                b.channel_id,
                b.source_id,
                b.tdate,
                b.amount,
                b.new_role_money,
                b.new_role_money / b.amount AS roi,
                b.pay_role_user_num / b.create_role_num AS pay_rate 
            FROM
                db_stdata.st_lauch_report b

            WHERE
                b.tdate >= date( NOW() - INTERVAL 120 HOUR ) 
                AND b.tdate_type = 'day' 
                AND b.media_id = 10 
                AND b.game_id IN ({}) 
                AND b.amount >= 500 
                AND b.pay_role_user_num >= 2 
                AND b.new_role_money >= 70
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
        # result_df = result_df[result_df['roi'] >= 0.03]
        return result_df

    @staticmethod
    def get_score_images():
        """ image素材评分表 """
        conn = connect(host='192.168.0.97', port=10000, auth_mechanism='PLAIN', user='hadoop',
                    password='Ycjh8FxiaoMtShZRd3-97%3hCEL0CK4ns1w', database='default')
        cursor = conn.cursor()
        sql_engine = 'set hive.execution.engine=tez'
        sql = 'select image_id,label_ids,model_run_datetime,score from dws.dws_image_score_d where media_id=10'
        cursor.execute(sql_engine)
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
        plan_info = plan_info[['ad_account_id', 'game_id', 'channel_id', 'source_id',
                               'create_time', 'smart_bid_type', 'hide_if_exists', 'budget',
                               'delivery_range', 'adjust_cpa', 'inventory_type', 'hide_if_converted',
                               'flow_control_mode', 'schedule_time', 'cpa_bid', 'auto_extend_enabled',
                               'gender', 'city', 'platform', 'launch_price',
                               'retargeting_tags_exclude', 'interest_categories',
                               'ac', 'android_osv', 'location_type', 'retargeting_tags_include',
                               'retargeting_type', 'ios_osv', 'interest_action_mode', 'age',
                               'action_categories', 'action_days', 'action_scene', 'deep_bid_type', 'roi_goal']]
        return plan_info


    def __call__(self, cfg):
        """ 采集 """
        game_id_dict = get_game_id(cfg.DG_GAME_ID)
        
        plan_info = pd.read_csv('./data/ptom_third_plan.csv')
        # image_info = pd.read_csv('./data/image_info.csv')
        # launch_report = pd.read_csv('./data/launch_report.csv') 

        plan_info_new = self.get_plan_info(game_id_dict)
        image_info = self.get_image_info()
        launch_report = self.get_launch_report(game_id_dict)

        creative_info = self.get_creative(game_id_dict)
        now_plan_roi = self.get_now_plan_roi(game_id_dict)

        # 计划
        plan_info = plan_info.append(plan_info_new)
        plan_info = self.get_plan_json(plan_info)
        plan_info.dropna(subset=['cpa_bid'], inplace=True)
        # 素材
        # image_info = image_info.append(image_info_new)
        image_info.dropna(subset=['image_id'], inplace=True)
        image_info['image_id'] = image_info['image_id'].astype(int)
        # 运营
        # launch_report = launch_report.append(launch_report_new)
        # 创意
        creative_info['title_list'] = creative_info['title_list'].fillna('[]')
        creative_info['ad_keywords'] = creative_info['ad_keywords'].fillna('[]')
        creative_info['title_list'] = creative_info['title_list'].apply(json.loads)
        creative_info['ad_keywords'] = creative_info['ad_keywords'].apply(json.loads)
        now_plan_roi = pd.merge(now_plan_roi, creative_info, on=['channel_id', 'source_id'], how='left')
        now_plan_roi = now_plan_roi.loc[now_plan_roi.apply(lambda x:False if pd.isnull(x['third_industry_id']) else True, axis=1)]
        # now_plan_roi.drop(['tdate', 'amount', 'new_role_money', 'roi', 'pay_rate'], axis=1, inplace=True)
        
        # 返回一 df
        df = pd.merge(plan_info, image_info, on=['channel_id', 'source_id'], how='left')
        df.dropna(subset=['image_id'], inplace=True)
        df = pd.merge(df, launch_report, on=['channel_id', 'source_id'], how='left')
        df.drop(df[df['tdate'].isna()].index, inplace=True)

        df = df[df['amount'] >= 500]  ## 消耗500以上的计划

        df.dropna(subset=['roi'], inplace=True)
        df.dropna(subset=['new_role_money'], inplace=True)
        df.dropna(subset=['amount'], inplace=True)

        df['platform'] = df['platform'].astype(str)
        df['platform'] = df['platform'].map({"['ANDROID']": 1, "['IOS']": 2})
        df = df[df['deep_bid_type'].isin(['BID_PER_ACTION'])]  ## 'ROI_COEFFICIENT'
        df = df.loc[df.apply(lambda x:True if not None in x['inventory_type'] else False, axis=1)]  ## 过滤None异常
        df['label'] = df.apply(lambda x: 1 if x.new_role_money / x.amount >= cfg.ROI or x.roi >= cfg.ROI else 0, axis=1)  # 打标签
        df['ad_account_id'] = df['ad_account_id'].astype('int')
        df['image_id'] = df['image_id'].astype('int')
        df.rename(columns={'tdate': 'create_date'}, inplace=True)
        df['create_date'] = pd.to_datetime(df['create_date'])
        df['create_time'] = pd.to_datetime(df['create_time'])

        df = df[df['label_ids'].notnull() & (df['label_ids'] != '-1') & (df['label_ids'] != '\"-1')]
        df['label_ids'] = df['label_ids'].str.replace('-1;', '')
        df['label_ids'] = df['label_ids'].str.replace('-1,', '')
        df['label_ids'] = df['label_ids'].str.replace('\"', '')
        df['label_ids'] = df['label_ids'].str.replace(';', ',')
        df['label_ids'] = df['label_ids'].apply(lambda x: x.split(','))

        # df = pd.merge(df, now_plan_roi, on=['channel_id', 'source_id'], how='inner')

        df.drop(['channel_id', 'source_id', 'amount', 'new_role_money'], axis=1, inplace=True)
        df = df.mask(df.applymap(str).eq('[]'))  ## 空list替换为NaN
        df = df.mask(df.applymap(str).eq('NONE'))  ## NONE替换为NaN

        df.reset_index(drop=True, inplace=True)

        # 返回二 df_create
        plan_info['create_time'] = pd.to_datetime(plan_info['create_time'])
        plan_info_current = plan_info[plan_info['create_time'] >= pd.datetime.now() - pd.DateOffset(15)]
        
        df_create = pd.merge(plan_info_current, image_info, on=['channel_id', 'source_id'], how='left')
        df_create = pd.merge(df_create, now_plan_roi, on=['channel_id', 'source_id'], how='inner')
        df_create['platform'] = df_create['platform'].astype(str)
        df_create['platform'] = df_create['platform'].map({"['ANDROID']": 1, "['IOS']": 2})
        df_create['platform'] = df_create['platform'].astype(int)

        df_create.dropna(subset=['image_id'], inplace=True)
        df_create['image_id'] = df_create['image_id'].astype(int)
        df_create = df_create[df_create['platform'] == 1]
        df_create.rename(columns={'tdate': 'create_date'}, inplace=True)
        df_create['create_date'] = pd.to_datetime(df['create_date'])
        df_create['create_time'] = pd.to_datetime(df['create_time'])
        # df_create = df_create[df_create['deep_bid_type'].isin(['BID_PER_ACTION', 'ROI_COEFFICIENT', 'ROI_PACING'])]
        df_create.drop(['channel_id', 'source_id', 'amount', 'new_role_money'], axis=1, inplace=True)
        df_create['label'] = 1

        df_create.reset_index(drop=True, inplace=True)

        # 返回三 df_image
        df_image = self.get_score_images()
        df_image.dropna(subset=['image_id', 'score'], inplace=True)
        df_image = df_image[['image_id', 'model_run_datetime', 'score']]
        df_image.columns = ['image_id', 'create_time', 'image_score']
        df_image.drop_duplicates(inplace=True)

        df_image['create_time'] = pd.to_datetime(df_image['create_time'], format='%Y-%m-%d')
        df_image['create_time'] = df_image['create_time'].apply(lambda x: x.date())
        
#         df.to_csv("./df_.csv")
#         df_create.to_csv("./df_create.csv")
#         df_image.to_csv("./df_image.csv")
        return df, df_create, df_image, now_plan_roi


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