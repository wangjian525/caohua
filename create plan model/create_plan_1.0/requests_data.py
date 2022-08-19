import requests
import pandas as pd
import numpy as np
import json
import warnings

warnings.filterwarnings('ignore')
from impala.dbapi import connect
from impala.util import as_pandas
import pymysql


# 获取score_image (分数大于550的image_id)
def get_score_image():
    conn = connect(host='192.168.0.88', port=10000, auth_mechanism='PLAIN', user='hadoop',
                   password='Ycjh8FxiaoMtShZRd3-97%3hCEL0CK4ns1w', database='default')
    cursor = conn.cursor()
    sql_engine = 'set hive.execution.engine=tez'
    sql = 'select image_id from dws.dws_image_score_d where media_id=10 and score>=550 and dt>=date_sub(CURRENT_DATE,1) group by image_id'
    cursor.execute(sql_engine)
    cursor.execute(sql)
    result = as_pandas(cursor)
    result['image_id'].values
    # 关闭链接
    cursor.close()
    conn.close()
    return result['image_id'].values


# 获取近期（7天内）所有计划
def get_plan_info():
    conn = pymysql.connect(host='192.168.0.65', port=3306, user='readonly',
                           passwd='Kc&r+z1ql9x8za4kzfk761weq8ozGv3ZpI;RMe,#+s%c>t', db='db_ptom')
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
       SELECT
                * 
            FROM
                db_ptom.ptom_third_plan p
            WHERE
                game_id IN (
            1000840,
            1000862,
            1000869,
            1000935,
            1000947,
            1000960,
            1001049,
            1001058,
            1001063,
            1001079,
            1001059,
            1000954,
            1000993,
            1000994,
            1000992,
            1001258,
            1001294,
            1001295,
            1001310,
            1001155,
            1001257,
            1001379,
            1001193,
            1001400,
            1001401,
            1001402,
            1001439,
            1001413,
            1001414,
            1001420,
            1001425,
            1001426,
            1001430,
            1001431,
            1001259,
            1000985,
            1001454,
            1001455,
            1001457,
            1001460,
            1001194,
            1001484,
            1001440,
            1001540,
            1001541
                ) 
                AND media_id = 10
                AND create_time>=(NOW() - interval 168 hour)
                            AND plan_id >= (
                                select plan_id from ptom_plan
                                where create_time >= (NOW() - interval 168 hour)
                                and create_time <= (NOW() - interval 144 hour)
                                limit 1
                            )
    '''
    cur.execute(sql)
    result_df = pd.read_sql(sql, conn)
    cur.close()
    conn.close()
    return result_df


# 获取近期计划的运营数据
def get_now_plan_roi():
    conn = pymysql.connect(host='192.168.0.79', port=3306, user='readonly',
                           passwd='Kc&r+z1ql9x8za4kzfk761weq8ozGv3ZpI;RMe,#+s%c>t')
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
        SELECT
            a.channel_id as 'channel_id',
            a.source_id as 'source_id',
            a.amount as 'amount',
            a.create_role_num as 'create_role_num',
            b.new_role_money as 'new_role_money',
            b.pay_role_user_num as 'pay_role_user_num'
        FROM
            (
            SELECT
                a.channel_id,
                a.source_id,
                sum(a.amount) as 'amount',
                sum(a.create_role_num) as 'create_role_num' 
            FROM
                db_stdata.st_lauch_report a
            WHERE
                a.tdate_type = 'day' 
                AND a.tdate >= date(NOW() - interval 168 hour)
                AND a.tdate<=date(NOW() - interval 24 hour)
                AND a.amount > 0 
                AND a.media_id = 10 
                AND a.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1056 AND dev_game_id IS NOT NULL )
                GROUP BY a.channel_id,a.source_id
                ) a
            LEFT JOIN (
            SELECT
                c.channel_id,
                c.source_id,
                sum( c.create_role_money ) new_role_money,
                IFNULL( sum( c.pay_role_user_num ), 0 ) AS pay_role_user_num 
            FROM
                db_stdata.st_game_days c
            WHERE
                c.report_days = 7 
                AND c.tdate = date(NOW() - interval 24 hour)
                AND c.tdate_type = 'day' 
                AND c.query_type = 13 
                AND c.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1056 AND dev_game_id IS NOT NULL ) 
            GROUP BY
                c.channel_id,
                c.source_id 
            HAVING
                ( new_role_money > 0 OR pay_role_user_num > 0 ) 
            ) b ON a.source_id = b.source_id 
            AND a.channel_id = b.channel_id 
    '''
    cur.execute(sql)
    result_df = pd.read_sql(sql, conn)
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
            AND a.create_time >= '2020-10-01' 
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


def get_ad_info():
    # 数据获取
    plan_info = get_plan_info()
    now_plan_roi = get_now_plan_roi()
    image_info = get_image_info()
    score_image = get_score_image()
    plan_info.drop('inventory_type', axis=1, inplace=True)
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
                           'action_categories', 'action_days', 'action_scene']]
    df = pd.merge(plan_info, image_info, on=['channel_id', 'source_id'], how='left')
    df = pd.merge(df, now_plan_roi, on=['channel_id', 'source_id'], how='inner')
    df = df[df['pay_role_user_num'].notna()]
    df['roi'] = df['new_role_money'] / df['amount']
    df['pay_cost'] = df['amount'] / df['pay_role_user_num']

    df['platform'] = df['platform'].astype(str)
    df['platform'] = df['platform'].map({"['ANDROID']": 1, "['IOS']": 2})
    df['platform'] = df['platform'].astype(int)
    df['pay_cost_ok'] = df.apply(lambda x: 1 if (x.platform == 1) & (x.pay_cost <= 5000) else (1 if
                                                                                               (x.platform == 2) & (
                                                                                                           x.pay_cost <= 8000) else 0),
                                 axis=1)
    df['amount_ok'] = df.apply(lambda x: 1 if (x.platform == 1) & (x.amount >= 5000) else (1 if
                                                                                           (x.platform == 2) & (
                                                                                                       x.amount >= 8000) else 0),
                               axis=1)
    df = df[(df['amount_ok'] == 1) & (df['pay_cost_ok'] == 1)]
    df = df[df['roi'] > 0.01]

    df.dropna(subset=['image_id'], inplace=True)
    df['image_id'] = df['image_id'].astype(int)
    df['pay_rate'] = df['pay_role_user_num'] / df['create_role_num']

    df = df[df['platform'] == 1]  # 测试，只取安卓数据

    # 选ad_account_id、image_id、game_id，每个账号+素材8条
    ad_account_id_group = np.array([6620, 6621, 6867, 6868])
    image_id_group = np.intersect1d(df['image_id'].unique(), score_image)
    plan = pd.DataFrame()
    for ad_account in ad_account_id_group:
        for image in image_id_group:
            #         print(image)
            temp = pd.DataFrame({'ad_account_id': [ad_account], 'image_id': [image]})
            plan = plan.append(temp)
    #         print(temp)
    plan = pd.DataFrame(np.repeat(plan.values, 8, axis=0), columns=plan.columns)
    plan['game_id'] = 1001379

    # 选platform测试期默认[ANDROID]
    plan['platform'] = '[ANDROID]'

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

    # 选budget
    plan['budget'] = plan['platform'].apply(lambda x: 3300 if x == '[ANDROID]' else 4000)

    # 选'retargeting_type','retargeting_tags_include','retargeting_tags_exclude'  人群包定向
    sample_df = df[['retargeting_type', 'retargeting_tags_include', 'retargeting_tags_exclude', 'pay_rate']]
    sample_df = sample_df.sample(n=plan.shape[0], replace=True, weights=sample_df['pay_rate']).drop('pay_rate',
                                                                                                    axis=1).reset_index(
        drop=True)
    plan = pd.concat([plan, sample_df], axis=1)

    # 选'interest_action_mode','action_scene','action_days','action_categories' ,'interest_categories' 行为兴趣
    sample_df = df[
        ['interest_action_mode', 'action_scene', 'action_days', 'action_categories', 'interest_categories', 'pay_rate']]
    sample_df = sample_df.sample(n=plan.shape[0], replace=True, weights=sample_df['pay_rate']).drop('pay_rate',
                                                                                                    axis=1).reset_index(
        drop=True)
    plan = pd.concat([plan, sample_df], axis=1)

    # 选'smart_bid_type','adjust_cpa','cpa_bid'出价方式
    sample_df = df[['smart_bid_type', 'adjust_cpa', 'cpa_bid', 'pay_rate']]
    sample_df = sample_df[sample_df['cpa_bid'] >= 1500]
    sample_df = sample_df.sample(n=plan.shape[0], replace=True, weights=sample_df['pay_rate']).drop('pay_rate',
                                                                                                    axis=1).reset_index(
        drop=True)
    plan = pd.concat([plan, sample_df], axis=1)

    cols = ['delivery_range',  'city', 'location_type', 'gender', 'age', 'ac', 'launch_price',
            'auto_extend_enabled', 'hide_if_exists', 'hide_if_converted', 'schedule_time', 'flow_control_mode']
    for col in cols:
        count_df = pd.DataFrame(data=df[col].value_counts()).reset_index()
        count_df.columns = ['col', 'counts']
        count_df['pct'] = count_df['counts'] / count_df['counts'].sum()
        plan[col] = plan.apply(lambda x: np.random.choice(count_df['col'].values, 1, p=count_df['pct'].values)[0],
                               axis=1)
    # 选inventory_type 创意投放位置
    count_df = pd.DataFrame(data=df['inventory_type'].value_counts()).reset_index()
    count_df.columns = ['col', 'counts']

    for i in range(count_df.shape[0]):
        values = count_df.loc[i, 'col']

        for value in values:
            if value == None:
                values.remove(value)
        a = np.empty(1, dtype=object)
        a[0] = values
        count_df.loc[i, 'col'] = a
    count_df['pct'] = count_df['counts'] / count_df['counts'].sum()
    plan['inventory_type'] = plan.apply(
        lambda x: np.random.choice(count_df['col'].values, 1, p=count_df['pct'].values)[0], axis=1)

    plan_sample = plan.sample(5)  # 先取5个测试

    ad_info = []
    for i in range(plan_sample.shape[0]):
        ad_info.append(json.loads(plan_sample.iloc[i].to_json()))
    return ad_info


def get_ad_create():
    open_api_url_prefix = "http://192.168.0.60:8085/"
    uri = "model/generationPlanBatchTask"
    url = open_api_url_prefix + uri
    data = get_ad_info()
    params = {
        "secretkey": "abc2018!@**@888"
    }
    rsp = requests.post(url, json=data, params=params)
    rsp_data = rsp.json()
    print('回传参数成功....')
    return rsp_data


def get_crowd_pack():
    open_api_url_prefix = "http://192.168.0.60:8085/"
    uri = "model/getAllValidCrowdPacks"
    url = open_api_url_prefix + uri

    params = {
        "secretkey": "abc2018!@**@888"
    }

    rsp = requests.get(url, params=params)
    rsp_data = rsp.json()
    return rsp_data


# 获取全部有效的人群包
# start = time.time()
# print(get_crowd_pack())
# end = time.time()
# print('本次运行消耗时间: %s Seconds' % (end-start))
# 根据传递参数，生成计划批量任务

if __name__ == '__main__':
    get_ad_create()
