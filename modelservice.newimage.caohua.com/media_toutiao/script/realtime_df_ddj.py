# -*- coding:utf-8 -*-
"""
   File Name：     realtime_df.py
   Description :   计划创建相关：实时数据采集（大东家）
   Author :        royce.mao
   date：          2021/6/9 10:00
"""

from impala.util import as_pandas
import pandas as pd
import numpy as np
import pymysql
import json

from config import get_var
dicParam = get_var()


class DataRealTime(object):
    """ 实时数据采集类 """
    def __init__(self, ):
        pass
 
    @staticmethod
    def get_plan_info():
        game_id = get_game_id()
        game_id = list(map(lambda x: x['game_id'], game_id))
        game_id = [str(i) for i in game_id]
        game_id = ','.join(game_id)
        conn = pymysql.connect(host=dicParam['DB_SLAVE_TOUFANG_HOST'], port=int(dicParam['DB_SLAVE_TOUFANG_PORT']), user=dicParam['DB_SLAVE_TOUFANG_USERNAME'],
                               passwd=dicParam['DB_SLAVE_TOUFANG_PASSWORD'], db=dicParam['DB_SLAVE_TOUFANG_DATABASE'])
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
                    AND create_time>=date( NOW() - INTERVAL 1440 HOUR )
                    AND create_time<= date(NOW())
                                AND plan_id >= (
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
                a.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1136 AND dev_game_id IS NOT NULL ) 
                AND a.media_id = 10 
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
    def get_creative():
        game_id = get_game_id()
        game_id = list(map(lambda x: x['game_id'], game_id))
        game_id = [str(i) for i in game_id]
        game_id = ','.join(game_id)

        conn = pymysql.connect(host=dicParam['DB_SLAVE_TOUFANG_HOST'], port=int(dicParam['DB_SLAVE_TOUFANG_PORT']), user=dicParam['DB_SLAVE_TOUFANG_USERNAME'],
                               passwd=dicParam['DB_SLAVE_TOUFANG_PASSWORD'], db=dicParam['DB_SLAVE_TOUFANG_DATABASE'])
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
    def get_now_plan_roi():
        conn = pymysql.connect(host=dicParam['DB_SLAVE_FENXI_HOST'], port=int(dicParam['DB_SLAVE_FENXI_PORT']), user=dicParam['DB_SLAVE_FENXI_USERNAME'],
                               passwd=dicParam['DB_SLAVE_FENXI_PASSWORD'])
        cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
        sql = '''
            SELECT
            b.channel_id,
            b.source_id
            FROM
                db_stdata.st_lauch_report b LEFT JOIN db_data_ptom.ptom_plan c ON c.chl_user_id = b.channel_id 
                AND c.source_id = b.source_id
            WHERE
                b.tdate >= date( NOW() - INTERVAL 720 HOUR )  # 30天内20个点的好计划
                AND b.tdate_type = 'day'
                AND b.media_id = 10
                AND (b.game_id = 1001917 OR b.game_id = 1001888 OR b.game_id = 1001890) 
                AND b.amount >= 2000
                # AND b.amount <= 300000
                AND b.pay_role_user_num >= 1
                AND b.new_role_money >= 100
                AND (b.new_role_money / b.amount)>=0.2
                '''
        cur.execute(sql)
        result = as_pandas(cur)
        # 关闭链接
        cur.close()
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

        for col in ['ios_osv']:
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


    def __call__(self, ):
        """ 采集 """
        plan_info = self.get_plan_info()
        plan_info = self.get_plan_json(plan_info)

        image_info = self.get_image_info()
        image_info.dropna(subset=['image_id'], inplace=True)
        image_info['image_id'] = image_info['image_id'].astype(int)

        launch_report = self.get_launch_report()

        df = pd.merge(plan_info, image_info, on=['channel_id', 'source_id'], how='left')
        df.dropna(subset=['image_id'], inplace=True)
        df = pd.merge(df, launch_report, on=['channel_id', 'source_id'], how='left')
        df.drop(df[df['tdate'].isna()].index, inplace=True)
        df = df[df['amount'] >= 500]

        df['platform'] = df['platform'].astype(str)
        df['platform'] = df['platform'].map({"['ANDROID']": 1, "['IOS']": 2})
        df['label'] = df.apply(lambda x: 1 if x.new_role_money / x.amount >= 0.03 else (0 if x.new_role_money / x.amount < 0.015 else -1), axis=1)

        df['ad_account_id'] = df['ad_account_id'].astype('int')
        df['image_id'] = df['image_id'].astype('int')
        df.rename(columns={'tdate': 'create_date'}, inplace=True)
        df['create_date'] = pd.to_datetime(df['create_date'])
        df['create_time'] = pd.to_datetime(df['create_time'])

        df.drop(['budget', 'cpa_bid', 'channel_id', 'source_id', 'amount', 'roi', 'pay_rate',
                'new_role_money'], axis=1, inplace=True)

        plan_info['create_time'] = pd.to_datetime(plan_info['create_time'])
        plan_info_current = plan_info[plan_info['create_time'] >= pd.datetime.now() - pd.DateOffset(60)]

        creative_info = self.get_creative()
        creative_info['title_list'] = creative_info['title_list'].fillna('[]')
        creative_info['ad_keywords'] = creative_info['ad_keywords'].fillna('[]')
        creative_info['title_list'] = creative_info['title_list'].apply(json.loads)
        creative_info['ad_keywords'] = creative_info['ad_keywords'].apply(json.loads)

        now_plan_roi = self.get_now_plan_roi()
        now_plan_roi = pd.merge(now_plan_roi, creative_info, on=['channel_id', 'source_id'], how='left')

        df_create = pd.merge(plan_info_current, image_info, on=['channel_id', 'source_id'], how='left')
        df_create = pd.merge(df_create, now_plan_roi, on=['channel_id', 'source_id'], how='inner')

        df_create['platform'] = df_create['platform'].astype(str)
        df_create['platform'] = df_create['platform'].map({"['ANDROID']": 1, "['IOS']": 2})
        df_create['platform'] = df_create['platform'].astype(int)

        df_create.dropna(subset=['image_id'], inplace=True)
        df_create['image_id'] = df_create['image_id'].astype(int)

        df_create = df_create[df_create['platform'] == 1]

        # 填充 android_osv  ios_osv
        df_create['android_osv'] = df_create['android_osv'].fillna('NONE')
        df_create['ios_osv'] = df_create['ios_osv'].fillna('NONE')
        df_create['channel_id'] = df_create['channel_id'].map(str)

        # df_create跑量方式均衡
        df_create_ROI = df_create[df_create['deep_bid_type'] == 'ROI_COEFFICIENT']
        try:
            df_create_PER = df_create[df_create['deep_bid_type'] == 'BID_PER_ACTION'].sample(int(len(df_create_ROI)) * 2)
        except:
            df_create_PER = df_create[df_create['deep_bid_type'] == 'BID_PER_ACTION']

        df_create = df_create_PER.append(df_create_ROI)
        df_create.reset_index(drop=True, inplace=True)
        print("候选计划跑量方式分布：", df_create['deep_bid_type'].value_counts())

        return df_create, df, image_info


def get_game_id():
    """ 中间件关联游戏ID """
    conn = pymysql.connect(host=dicParam['DB_SLAVE_FENXI_HOST'], port=int(dicParam['DB_SLAVE_FENXI_PORT']), user=dicParam['DB_SLAVE_FENXI_USERNAME'],
                           passwd=dicParam['DB_SLAVE_FENXI_PASSWORD'])
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
        SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1136 AND dev_game_id IS NOT NULL 
    '''
    cur.execute(sql)
    result_df = cur.fetchall()
    cur.close()
    conn.close()
    return result_df
