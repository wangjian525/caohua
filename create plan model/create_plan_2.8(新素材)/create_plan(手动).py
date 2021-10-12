import pandas as pd
import numpy as np
import json
from sklearn.model_selection import train_test_split
from sklearn.decomposition import PCA
from sklearn.preprocessing import LabelEncoder, MinMaxScaler
import gc
import warnings
import requests
import random

warnings.filterwarnings('ignore')
import lightgbm as lgb
from impala.dbapi import connect
from impala.util import as_pandas
import pymysql
import logging

warnings.filterwarnings('ignore')
logger = logging.getLogger('CreatePlan')


#
# 打包接口
#
class CreatePlan:
    def POST(self):
        # 处理POST请求
        logging.info("do service")
        try:
            ret = self.Process()
            logging.info(ret)
            return ret
        except Exception as e:
            logging.error(e)
            ret = json.dumps({"code": 500, "msg": str(e)})
            return ret

    # 任务处理函数
    def Process(self):
        main_model()
        ret = json.dumps({"code": 200, "msg": "success!", "data": "create plan is  success"})
        return ret


def get_game_id():
    conn = pymysql.connect(host='192.168.0.79', port=3306, user='readonly',
                           passwd='Kc&r+z1ql9x8za4kzfk761weq8ozGv3ZpI;RMe,#+s%c>t')
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
        SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1056 AND dev_game_id IS NOT NULL 
    '''
    cur.execute(sql)
    result_df = cur.fetchall()
    cur.close()
    conn.close()
    return result_df


# 获取近期所有计划(30天)
def get_plan_info():
    game_id = get_game_id()
    game_id = list(map(lambda x: x['game_id'], game_id))
    game_id = [str(i) for i in game_id]
    game_id = ','.join(game_id)
    conn = pymysql.connect(host='192.168.0.65', port=3306, user='readonly',
                           passwd='Kc&r+z1ql9x8za4kzfk761weq8ozGv3ZpI;RMe,#+s%c>t', db='db_ptom')
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
                AND create_time>=date( NOW() - INTERVAL 720 HOUR )
                AND create_time<= date(NOW())
                            AND plan_id >= (
                                select plan_id from db_ptom.ptom_plan
                                where create_time >= date( NOW() - INTERVAL 720 HOUR )
                                and create_time <= date( NOW() - INTERVAL 696 HOUR )
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
    conn = pymysql.connect(host='192.168.0.79', port=3306, user='readonly',
                           passwd='Kc&r+z1ql9x8za4kzfk761weq8ozGv3ZpI;RMe,#+s%c>t')
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
            AND a.media_id = 10 
            AND a.create_time >= date( NOW() - INTERVAL 720 HOUR )
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


# 获取计划运营指标
# def get_launch_report():
#     game_id = get_game_id()
#     game_id = list(map(lambda x: x['game_id'], game_id))
#     game_id = [str(i) for i in game_id]
#     game_id = ','.join(game_id)
#
#     conn = pymysql.connect(host='192.168.0.79', port=3306, user='readonly',
#                            passwd='Kc&r+z1ql9x8za4kzfk761weq8ozGv3ZpI;RMe,#+s%c>t')
#     cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
#     sql = '''
#         /*手动查询*/
#         SELECT
#             a.chl_user_id AS channel_id,
#             a.source_id AS source_id,
#             b.tdate,
#             b.amount,
#             b.new_role_money,
#             b.new_role_money / b.amount as roi,
#             b.pay_role_user_num / b.create_role_num as pay_rate
#         FROM
#             db_data_ptom.ptom_plan a
#             LEFT JOIN db_stdata.st_lauch_report b ON a.chl_user_id = b.channel_id
#             AND a.source_id = b.source_id
#         WHERE
#             a.create_time >= date( NOW() - INTERVAL 720 HOUR )
#             AND b.tdate >= date( NOW() - INTERVAL 720 HOUR )
#             AND b.tdate_type = 'day'
#             AND b.media_id = 10
#             AND b.game_id IN ({})
#             AND b.amount >= 500
#     '''
#     finalSql = sql.format(game_id)
#     cur.execute(finalSql)
#     result_df = pd.read_sql(finalSql, conn)
#     cur.close()
#     conn.close()
#     result_df['tdate'] = pd.to_datetime(result_df['tdate'])
#     result_df = result_df.sort_values('tdate')
#     result_df = result_df.drop_duplicates(['channel_id', 'source_id'], keep='first')
#
#     return result_df


# 解析json
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
                           'ios_osv', 'interest_action_mode', 'age',
                           'action_categories', 'action_days', 'action_scene', 'deep_bid_type', 'roi_goal']]
    return plan_info


def get_all_data():
    # 读取历史数据

    plan_info = get_plan_info()
    image_info = get_image_info()
    # launch_report = get_launch_report()
    plan_info = get_plan_json(plan_info)

    image_info.dropna(subset=['image_id'], inplace=True)
    image_info['image_id'] = image_info['image_id'].astype(int)
    return plan_info, image_info


# 获取近期计划的运营数据
def get_now_plan_roi():
    game_id = get_game_id()
    game_id = list(map(lambda x: x['game_id'], game_id))
    game_id = [str(i) for i in game_id]
    game_id = ','.join(game_id)

    conn = pymysql.connect(host='192.168.0.79', port=3306, user='readonly',
                           passwd='Kc&r+z1ql9x8za4kzfk761weq8ozGv3ZpI;RMe,#+s%c>t')
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
            LEFT JOIN db_stdata.st_lauch_report b ON a.chl_user_id = b.channel_id 
            AND a.source_id = b.source_id 
        WHERE
            b.tdate >= date( NOW() - INTERVAL 24 HOUR ) 
            AND b.tdate_type = 'day' 
            AND b.media_id = 10
            AND b.channel_id = 21361
            AND b.game_id IN ({}) 
            AND b.amount >= 500 
            AND b.pay_role_user_num >= 1 
            AND b.new_role_money >= 90 
            AND ( b.new_role_money / b.amount )>= 0.025
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


# 获取近期优化计划的创意数据
def get_creative():
    game_id = get_game_id()
    game_id = list(map(lambda x: x['game_id'], game_id))
    game_id = [str(i) for i in game_id]
    game_id = ','.join(game_id)

    conn = pymysql.connect(host='192.168.0.65', port=3306, user='readonly',
                           passwd='Kc&r+z1ql9x8za4kzfk761weq8ozGv3ZpI;RMe,#+s%c>t')
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


# 获取近期机器人计划的image_id
def get_current_image():
    game_id = get_game_id()
    game_id = list(map(lambda x: x['game_id'], game_id))
    game_id = [str(i) for i in game_id]
    game_id = ','.join(game_id)

    conn = pymysql.connect(host='192.168.0.79', port=3306, user='readonly',
                           passwd='Kc&r+z1ql9x8za4kzfk761weq8ozGv3ZpI;RMe,#+s%c>t')
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
        /*手动查询*/ 
            SELECT
                image_id 
            FROM
                db_data_ptom.ptom_plan 
            WHERE
                chl_user_id = 21206 
                AND media_id = 10 
                AND create_time >= date(NOW() - INTERVAL 24 HOUR )
                AND game_id IN ({})
    '''
    finalSql = sql.format(game_id)
    cur.execute(finalSql)
    result = pd.read_sql(finalSql, conn)
    result = result[result['image_id'].notna()]
    result['image_id'] = result['image_id'].map(int)
    result = result['image_id'].value_counts()
    result = result[result >= 4]
    cur.close()
    conn.close()
    return result


# 构造新计划  , 幸存危机
def create_plan(df, current_image):
    # 选ad_account_id、image_id每个账号+素材8条
    ad_account_id_group = np.array([7770, 7771, 7772, 7773])

    #     get_current_image = get_current_image()
    image_id_group = np.setdiff1d(df['image_id'].unique(), current_image)
    #     image_id_group = df['image_id'].unique()
    print(image_id_group)
    print(len(image_id_group))
    if len(image_id_group) != 0:

        plan_df = df[['game_id', 'image_id', 'platform', 'android_osv', 'ios_osv', 'budget',
                      'retargeting_tags_include', 'retargeting_tags_exclude',
                      'interest_action_mode', 'action_scene', 'action_days', 'action_categories',
                      'interest_categories', 'deep_bid_type', 'roi_goal', 'smart_bid_type',
                      'adjust_cpa', 'cpa_bid', 'delivery_range', 'city', 'location_type', 'gender', 'age', 'ac',
                      'launch_price', 'auto_extend_enabled', 'hide_if_exists', 'hide_if_converted', 'schedule_time',
                      'flow_control_mode', 'inventory_type', 'ad_keywords', 'title_list', 'third_industry_id']]

        plan = pd.DataFrame()
        for ad_account in ad_account_id_group:
            plan_df['ad_account_id'] = ad_account
            plan = plan.append(plan_df)

        # 选platform测试期默认[ANDROID]
        plan['platform'] = plan['platform'].apply(lambda x: '[ANDROID]' if x == 1 else '[IOS]')

        # 选budget\cpa_bid
        plan['budget'] = plan['platform'].apply(lambda x: 3300 if x == '[ANDROID]' else 4000)
        plan['cpa_bid'] = round(plan['cpa_bid'] * 1.01, 2)
        # 选android_osv
        count_df = pd.DataFrame(data=df['android_osv'].value_counts()).reset_index()
        count_df.columns = ['col', 'counts']
        count_df['pct'] = count_df['counts'] / count_df['counts'].sum()
        plan['android_osv'] = plan['platform'].apply(lambda x: 'NONE' if x == '[IOS]' else
        np.random.choice(count_df['col'].values, 1, p=count_df['pct'].values)[0])

        # 选ios_osv
        count_df = pd.DataFrame(data=df['ios_osv'].value_counts()).reset_index()
        count_df.columns = ['col', 'counts']
        count_df['pct'] = count_df['counts'] / count_df['counts'].sum()
        plan['ios_osv'] = plan['platform'].apply(lambda x: 'NONE' if x == '[ANDROID]' else
        np.random.choice(count_df['col'].values, 1, p=count_df['pct'].values)[0])

        # 选
        cols = ['ac', 'launch_price', 'auto_extend_enabled', 'hide_if_exists', 'hide_if_converted', 'schedule_time',
                'flow_control_mode']
        for col in cols:
            count_df = pd.DataFrame(data=df[col].value_counts()).reset_index()
            count_df.columns = ['col', 'counts']
            count_df['pct'] = count_df['counts'] / count_df['counts'].sum()
            plan[col] = plan.apply(lambda x: np.random.choice(count_df['col'].values, 1, p=count_df['pct'].values)[0],
                                   axis=1)

        # 选inventory_type 创意投放位置
        count_df = pd.DataFrame(data=df['inventory_type'].value_counts()).reset_index()
        count_df.columns = ['col', 'counts']
        count_df['col'] = count_df['col'].apply(lambda x: list(filter(None, x)))

        count_df['pct'] = count_df['counts'] / count_df['counts'].sum()
        plan['inventory_type'] = plan.apply(
            lambda x: np.random.choice(count_df['col'].values, 1, p=count_df['pct'].values)[0], axis=1)
        plan['create_time'] = pd.to_datetime(pd.datetime.now())
        plan['create_date'] = pd.to_datetime(pd.datetime.now().date())

    else:
        plan = pd.DataFrame()

    return plan


def get_train_df():
    plan_info, image_info = get_all_data()

    plan_info['create_time'] = pd.to_datetime(plan_info['create_time'])
    plan_info_current = plan_info[plan_info['create_time'] >= pd.datetime.now() - pd.DateOffset(8)]

    creative_info = get_creative()
    creative_info['title_list'] = creative_info['title_list'].fillna('[]')
    creative_info['ad_keywords'] = creative_info['ad_keywords'].fillna('[]')
    creative_info['title_list'] = creative_info['title_list'].apply(json.loads)
    creative_info['ad_keywords'] = creative_info['ad_keywords'].apply(json.loads)

    now_plan_roi = get_now_plan_roi()
    now_plan_roi = pd.merge(now_plan_roi, creative_info, on=['channel_id', 'source_id'], how='left')

    df_create = pd.merge(plan_info_current, image_info, on=['channel_id', 'source_id'], how='left')
    df_create = pd.merge(df_create, now_plan_roi.drop('ad_account_id', axis=1), on=['channel_id', 'source_id'],
                         how='inner')
    df_create['platform'] = df_create['platform'].astype(str)
    df_create['platform'] = df_create['platform'].map({"['ANDROID']": 1, "['IOS']": 2})
    df_create['platform'] = df_create['platform'].astype(int)

    df_create.dropna(subset=['image_id'], inplace=True)
    df_create['image_id'] = df_create['image_id'].astype(int)

    df_create = df_create[df_create['platform'] == 1]

    current_image = get_current_image().index
    plan_create = create_plan(df_create, current_image)

    return plan_create


def get_ad_create(plan_result):
    ad_info = []
    for i in range(plan_result.shape[0]):
        ad_info.append(json.loads(plan_result.iloc[i].to_json()))
    open_api_url_prefix = "https://ptom.caohua.com/"
    uri = "model/generationPlanBatchTask"
    url = open_api_url_prefix + uri
    params = {
        "secretkey": "abc2018!@**@888",
        "mediaId": 10
    }
    rsp = requests.post(url, json=ad_info, params=params)
    rsp_data = rsp.json()
    print('结束....')
    return rsp_data


def main_model():
    plan_create = get_train_df()
    if plan_create.shape[0] != 0:
        plan_result = plan_create.drop(['create_time', 'create_date'], axis=1)
        plan_result['convertIndex'] = plan_result['deep_bid_type'].apply(lambda x: 13 if x == 'BID_PER_ACTION' else 14)

        plan_result['budget'] = plan_result.apply(lambda x: x.budget if x.budget >= x.cpa_bid else x.cpa_bid, axis=1)
        plan_result['budget'] = plan_result['budget'].apply(np.ceil)
        # plan_result['cpa_bid'] = plan_result['cpa_bid'].apply(lambda x: x if x >= 550 else random.randint(550, 580))
        # plan_result['cpa_bid'] = plan_result['cpa_bid'].apply(lambda x: random.randint(550, 580))
        # plan_result['operation'] = 'disable'
        plan_result['web_url'] = 'https://www.chengzijianzhan.com/tetris/page/6977185861965266981/'
        plan_result['op_id'] = 13268
        plan_result['flag'] = 'N'
        # 人群包报错，直接先赋值为[]
        # plan_result['retargeting_tags_include'] = [[] for _ in range(len(plan_result))]
        # plan_result['retargeting_tags_exclude'] = [[] for _ in range(len(plan_result))]
        # 周三周四凌晨更新，不跑计划
        plan_result['schedule_time'] = plan_result['schedule_time'].apply(
            lambda x: x[0:96] + '1111111111000000000001' + x[118:144] + '1111111111000000000001' + x[166:])

        rsp_data = get_ad_create(plan_result)
        plan_result.to_csv('./plan_result.csv', index=0)  # 保存创建日志

        print(rsp_data)


if __name__ == '__main__':
    main_model()
