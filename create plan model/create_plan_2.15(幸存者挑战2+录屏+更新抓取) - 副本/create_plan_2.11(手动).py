import pandas as pd
import numpy as np
import json
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
import gc
import warnings
import requests
import random
import ast

warnings.filterwarnings('ignore')
import lightgbm as lgb
from impala.dbapi import connect
from impala.util import as_pandas
import pymysql
import logging


def get_game_id():
    conn = pymysql.connect(host='db-slave-modelfenxi-001.ch', port=3306, user='model_read',
                           passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8')
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
        SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1056 AND dev_game_id IS NOT NULL 
    '''
    cur.execute(sql)
    result_df = cur.fetchall()
    cur.close()
    conn.close()
    return result_df


# 获取近期所有计划('2021年4月23号开始')
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
            a.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1056 AND dev_game_id IS NOT NULL ) 
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


# 获取计划运营指标
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
    launch_report = get_launch_report()

    plan_info = get_plan_json(plan_info)

    image_info.dropna(subset=['image_id'], inplace=True)
    image_info['image_id'] = image_info['image_id'].astype(int)
    return plan_info, image_info, launch_report

# 获取不漏点标签
def get_legal_image():
    conn = pymysql.connect(host='db-slave-modeltoufang-001.ch', port=3306, user='model_read',
                           passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8', db='db_ptom')
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
       SELECT
            image_id 
        FROM
            db_ptom.ptom_image_info a 
        WHERE
            FIND_IN_SET( 235, label_ids ) 

    '''
    cur.execute(sql)
    result_df = pd.read_sql(sql, conn)
    cur.close()
    conn.close()
    return result_df['image_id'].values


# 获取score_image (分数大于550的image_id)
def get_score_image():
    conn = connect(host='192.168.0.97', port=10000, auth_mechanism='PLAIN', user='hadoop',
                   password='Ycjh8FxiaoMtShZRd3-97%3hCEL0CK4ns1w', database='default')
    cursor = conn.cursor()
    sql_engine = 'set hive.execution.engine=tez'
    sql = '''select image_id,label_ids from dws.dws_image_score_d where media_id=10 and score>=530 and dt=CURRENT_DATE and label_ids=27 group by image_id,label_ids'''
    cursor.execute(sql_engine)
    cursor.execute(sql)
    result = as_pandas(cursor)
    result['label_ids'] = result['label_ids'].astype(str)
    result['label_ids'] = result['label_ids'].apply(lambda x: x.strip('-1;') if '-1' in x else x)
    result['label_ids'] = pd.to_numeric(result['label_ids'], errors='coerce')
    # result = result[result['label_ids'].isin([234])]
    #     print(result)
    # 关闭链接
    cursor.close()
    conn.close()

    return result['image_id'].values


# 获取7日素材评分 (分数大于60的image_id)
def get_score_imag_7():
    conn = connect(host='192.168.0.97', port=10000, auth_mechanism='PLAIN', user='hadoop',
                   password='Ycjh8FxiaoMtShZRd3-97%3hCEL0CK4ns1w', database='default')
    cursor = conn.cursor()
    sql_engine = 'set hive.execution.engine=tez'
    sql = 'select image_id from tmp_data.tmp_ra_media_platform_image where dt= `current_date`() and platform=1 and media_id=10 and score>=30 and mgame_id=1056 and 7_amount>=1000'
    cursor.execute(sql_engine)
    cursor.execute(sql)
    result = as_pandas(cursor)

    # 关闭链接
    cursor.close()
    conn.close()

    return result['image_id'].values


# 获取近期所有计划的消耗情况
def get_amount_info():
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
                tdate,
                channel_id,
                source_id,
                game_id,
                media_id,
                platform,
                amount 
            FROM
                db_stdata.st_lauch_report 
            WHERE
                game_id IN ({}) 
                AND tdate_type = 'day' 
                AND tdate >= date( NOW() - INTERVAL 7 DAY )
                AND tdate <= date( NOW() - INTERVAL 1 DAY )
                AND media_id = 10
                AND platform = 1
    '''
    finalSql = sql.format(game_id)
    result_df = pd.read_sql(finalSql, conn)
    cur.close()
    conn.close()

    return result_df


def get_data_7():
    game_id = get_game_id()
    game_id = list(map(lambda x: x['game_id'], game_id))
    game_id = [str(i) for i in game_id]
    game_id = ','.join(game_id)
    conn = connect(host='192.168.0.97', port=10000, auth_mechanism='PLAIN', user='hadoop',
                   password='Ycjh8FxiaoMtShZRd3-97%3hCEL0CK4ns1w', database='default')
    cursor = conn.cursor()
    sql = '''
        SELECT
            user_id,
            game_id,
            channel_id,
            source_id,
            role_id,
            platform,
            media_id,
            pay_num,
            pay_sum,
            create_role_time,
            created_role_day,
            pay_sum AS pay_7_pred,
            dt 
        FROM
            tmp_data.tmp_roles_portrait_info_train2 
        WHERE
            dt = CURRENT_DATE 
            AND created_role_day = 7
            AND media_id = 10
            AND platform = 1
            and game_id IN ({})
    '''
    finalSql = sql.format(game_id)
    cursor.execute(finalSql)
    result = as_pandas(cursor)

    # 关闭链接
    cursor.close()
    conn.close()

    return result


def get_data_1_6():
    game_id = get_game_id()
    game_id = list(map(lambda x: x['game_id'], game_id))
    game_id = [str(i) for i in game_id]
    game_id = ','.join(game_id)
    conn = connect(host='192.168.0.97', port=10000, auth_mechanism='PLAIN', user='hadoop',
                   password='Ycjh8FxiaoMtShZRd3-97%3hCEL0CK4ns1w', database='default')
    cursor = conn.cursor()
    sql = '''
            SELECT
                user_id,
                game_id,
                channel_id,
                source_id,
                role_id,
                platform,
                media_id,
                pay_num,
                pay_sum,
                create_role_time,
                created_role_day,
                pay_7_pred,
                dt 
            FROM
                tmp_data.tmp_roles_portrait_info_predict 
            WHERE
                dt = CURRENT_DATE
                AND media_id = 10
                AND platform = 1
                and game_id IN ({})

    '''
    finalSql = sql.format(game_id)
    cursor.execute(finalSql)
    result = as_pandas(cursor)

    # 关闭链接
    cursor.close()
    conn.close()

    return result


# 获取近期计划的运营数据
def get_now_plan_roi():
    # 获取消耗数据
    amount_info = get_amount_info()
    # 获取回款预测数据
    df_roi_1 = get_data_7()
    df_roi_2 = get_data_1_6()
    df_roi = df_roi_1.append(df_roi_2)
    # 求计划付费成本，回款率，付费人数
    df_roi['create_role_time'] = pd.to_datetime(df_roi['create_role_time']).dt.date
    df_roi['pay_num'] = df_roi['pay_num'].replace(0, np.nan)
    source_df_1 = pd.DataFrame({'7_pay_sum':df_roi.groupby(['channel_id','source_id'])['pay_7_pred'].sum()}).reset_index()
    source_df_2 = pd.DataFrame({'pay_num':df_roi.groupby(['channel_id','source_id'])['pay_num'].count()}).reset_index()
    source_df_3 = pd.DataFrame({'amount':amount_info.groupby(['channel_id','source_id'])['amount'].sum()}).reset_index()
    source_df_3 = source_df_3[source_df_3['amount']>0]
    source_df = pd.merge(source_df_1,source_df_2,on=['channel_id','source_id'],how='outer')
    source_df = pd.merge(source_df,source_df_3,on=['channel_id','source_id'],how='outer')
    source_df = source_df.fillna(0)
    source_df = source_df[source_df['amount']>0]
    source_df['roi'] = source_df['7_pay_sum'] / source_df['amount']
    source_df['pay_cost'] = source_df['amount'] / source_df['pay_num']
    # 选择相关指标达标的计划
    result = source_df[(source_df['pay_num'] >= 1) & (source_df['pay_cost'] <= 7000) & (source_df['roi'] >= 0.07) & (source_df['amount'] >= 200)]
    return result


# 获取近期优化计划的创意数据
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


def get_manager_id():
    conn = pymysql.connect(host='db-slave-modelfenxi-001.ch', port=3306, user='model_read',
                           passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8')
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
    /*手动查询*/
        SELECT
            channel_id,
            manager_id 
        FROM
            db_data.dim_channel_info
    '''
    result_df = pd.read_sql(sql, conn)
    cur.close()
    conn.close()

    return result_df


# 构造新计划  , 7323, 7324, 7325, 7326   6987, 6989, 6990, 6981, 6866, 6837, 7184
def create_plan(df, score_image,image_7):
    # 选ad_account_id、image_id每个账号+素材8条
    game_id = 1001703
    # df = df[df['game_id'] == game_id]
    #     df = df[df['game_id'].isin([1001379, 1001703, 1001756, 1001772])]
    # ad_account_id_group = np.array([9872, 9873, 9874, 9875, 9876])
    ad_account_id_group = np.array([6987])
    image_id_group = np.intersect1d(df['image_id'].unique(), score_image)
    image_id_group = np.intersect1d(image_id_group, image_7)
    image_id_group = list(filter(lambda x: x >= 32861, image_id_group))
    print(('image_7', image_7))
    print('score_image', score_image)
    print('df_image', df['image_id'].unique())
    print('image_id_group', image_id_group)
    df = df[df['deep_bid_type'].isin(['BID_PER_ACTION', 'ROI_COEFFICIENT'])]
    df = df[df['image_id'].isin(image_id_group)]

    plan = pd.DataFrame()
    for ad_account in ad_account_id_group:
        for image in image_id_group:
            #         print(image)
            temp = pd.DataFrame({'ad_account_id': [ad_account], 'image_id': [image]})
            plan = plan.append(temp)
    #         print(temp)
    plan = pd.DataFrame(np.repeat(plan.values, 10, axis=0), columns=plan.columns)

    # game_image = df[['game_id', 'image_id']].drop_duplicates()
    # plan = pd.merge(plan, game_image, on='image_id', how='left')
    plan['game_id'] = game_id

    # 选platform测试期默认[ANDROID]
    # plan['platform'] = df['platform'].iloc[0]
    plan['platform'] = 1
    plan['platform'] = plan['platform'].apply(lambda x: '[ANDROID]' if x == 1 else '[IOS]')

    # 选android_osv
    count_df = pd.DataFrame(data=df['android_osv'].value_counts()).reset_index()
    count_df.columns = ['col', 'counts']
    count_df['pct'] = count_df['counts'] / count_df['counts'].sum()
    plan['android_osv'] = plan['platform'].apply(
        lambda x: 'NONE' if x == '[IOS]' else np.random.choice(count_df['col'].values, 1, p=count_df['pct'].values)[0])

    # 选ios_osv
    count_df = pd.DataFrame(data=df['ios_osv'].value_counts()).reset_index()
    count_df.columns = ['col', 'counts']
    count_df['pct'] = count_df['counts'] / count_df['counts'].sum()
    plan['ios_osv'] = plan['platform'].apply(
        lambda x: 'NONE' if x == '[ANDROID]' else np.random.choice(count_df['col'].values, 1, p=count_df['pct'].values)[
            0])

    # 选budget
    plan['budget'] = plan['platform'].apply(lambda x: 3300 if x == '[ANDROID]' else 4000)

    # 选'ad_keywords', 'title_list', 'third_industry_id'  创意
    sample_df = df[['manager_id', 'ad_keywords', 'title_list', 'third_industry_id']]
    sample_df = sample_df.sample(n=plan.shape[0], replace=True).reset_index(drop=True)
    plan = pd.concat([plan, sample_df], axis=1)
    plan = plan.rename(columns={'manager_id': 'manager_id_1'})

    # 选'retargeting_tags_include','retargeting_tags_exclude'  人群包定向 版位
    sample_df = df[
        ['manager_id', 'inventory_type', 'retargeting_tags_include', 'retargeting_tags_exclude', 'delivery_range',
         'city',
         'location_type', 'gender', 'age', 'ac', 'launch_price', 'auto_extend_enabled', 'hide_if_exists',
         'hide_if_converted',
         'schedule_time', 'flow_control_mode']]
    sample_df = sample_df.sample(n=plan.shape[0], replace=True).reset_index(drop=True)
    sample_df['inventory_type'] = sample_df['inventory_type'].apply(lambda x: list(filter(None, x)))
    plan = pd.concat([plan, sample_df], axis=1)
    plan = plan.rename(columns={'manager_id': 'manager_id_2'})

    # 选'interest_action_mode','action_scene','action_days','action_categories' ,'interest_categories' 行为兴趣
    sample_df = df[['manager_id', 'interest_action_mode', 'action_scene', 'action_days', 'action_categories',
                    'interest_categories']]
    sample_df = sample_df.sample(n=plan.shape[0], replace=True).reset_index(drop=True)
    plan = pd.concat([plan, sample_df], axis=1)
    plan = plan.rename(columns={'manager_id': 'manager_id_3'})

    # 选'deep_bid_type','roi_goal','smart_bid_type','adjust_cpa','cpa_bid'出价方式
    sample_df = df[['manager_id', 'deep_bid_type', 'roi_goal', 'smart_bid_type', 'adjust_cpa', 'cpa_bid']]
    sample_df = sample_df[sample_df['deep_bid_type'].isin(['ROI_COEFFICIENT', 'BID_PER_ACTION'])]
    sample_df = sample_df.sample(n=plan.shape[0], replace=True).reset_index(drop=True)
    plan = pd.concat([plan, sample_df], axis=1)
    plan = plan.rename(columns={'manager_id': 'manager_id_4'})

    # 计划归因channel_id
    plan['attribute'] = plan.apply(lambda x: [x.manager_id_1, x.manager_id_2, x.manager_id_3, x.manager_id_4], axis=1)
    plan.drop(['manager_id_1', 'manager_id_2', 'manager_id_3', 'manager_id_4'], axis=1, inplace=True)

    plan['create_time'] = pd.to_datetime(pd.datetime.now())
    plan['create_date'] = pd.to_datetime(pd.datetime.now().date())

    return plan


# 对列表内容进行编码降维
def get_mutil_feature(data):
    cols = ['inventory_type', 'age', 'city', 'retargeting_tags_include', 'retargeting_tags_exclude', 'ac',
            'interest_categories',
            'action_scene', 'action_categories']
    for col in cols:
        if col in ['inventory_type', 'age']:
            data[col] = data[col].apply(lambda x: x if x == x else [])
            data = data.join(data[col].str.join('|').str.get_dummies().add_prefix(col + '_').reset_index(drop=True))
            data.drop(col, axis=1, inplace=True)

        else:
            data[col] = data[col].apply(lambda x: x if x == x else [])
            data[col] = data[col].apply(lambda x: [str(i) for i in x])
            data[col] = data[col].astype(str)
            le = LabelEncoder()
            data[col] = le.fit_transform(data[col])

    gc.collect()
    return data


def get_train_df():
    plan_info, image_info, launch_report = get_all_data()

    df = pd.merge(plan_info, image_info, on=['channel_id', 'source_id'], how='left')
    df.dropna(subset=['image_id'], inplace=True)
    df = pd.merge(df, launch_report, on=['channel_id', 'source_id'], how='left')
    df.drop(df[df['tdate'].isna()].index, inplace=True)
    df = df[df['amount'] >= 500]

    df['platform'] = df['platform'].astype(str)
    df['platform'] = df['platform'].map({"['ANDROID']": 1, "['IOS']": 2})
    df['label'] = df.apply(lambda x: 1 if x.new_role_money / x.amount >= 0.02 else 0, axis=1)
    df['ad_account_id'] = df['ad_account_id'].astype('int')
    df['image_id'] = df['image_id'].astype('int')
    df.rename(columns={'tdate': 'create_date'}, inplace=True)
    df['create_date'] = pd.to_datetime(df['create_date'])
    df['create_time'] = pd.to_datetime(df['create_time'])

    df.drop(['budget', 'cpa_bid', 'channel_id', 'source_id', 'amount', 'roi', 'pay_rate',
             'new_role_money'], axis=1, inplace=True)

    plan_info['create_time'] = pd.to_datetime(plan_info['create_time'])
    plan_info_current = plan_info[plan_info['create_time'] >= pd.datetime.now() - pd.DateOffset(60)]

    creative_info = get_creative()
    creative_info['title_list'] = creative_info['title_list'].fillna('[]')
    creative_info['ad_keywords'] = creative_info['ad_keywords'].fillna('[]')
    creative_info['title_list'] = creative_info['title_list'].apply(json.loads)
    creative_info['ad_keywords'] = creative_info['ad_keywords'].apply(json.loads)

    now_plan_roi = get_now_plan_roi()
    now_plan_roi.to_csv('./now_plan_roi.csv')
    now_plan_roi = pd.merge(now_plan_roi, creative_info, on=['channel_id', 'source_id'], how='left')

    score_image = get_score_image()
    image_7 = get_score_imag_7()
    # score_image = get_legal_image()

    df_create = pd.merge(plan_info_current, image_info, on=['channel_id', 'source_id'], how='left')
    df_create = pd.merge(df_create, now_plan_roi, on=['channel_id', 'source_id'], how='inner')

    df_create['platform'] = df_create['platform'].astype(str)
    df_create['platform'] = df_create['platform'].map({"['ANDROID']": 1, "['IOS']": 2})
    df_create['platform'] = df_create['platform'].astype(int)

    df_create.dropna(subset=['image_id'], inplace=True)
    df_create['image_id'] = df_create['image_id'].astype(int)

    df_create = df_create[df_create['platform'] == 1]

    # 填充 android_osv   ios_osv
    df_create['android_osv'] = df_create['android_osv'].fillna('NONE')
    df_create['ios_osv'] = df_create['ios_osv'].fillna('NONE')
    df_create['channel_id'] = df_create['channel_id'].map(str)
    manager_id = get_manager_id()
    manager_id['channel_id'] = manager_id['channel_id'].map(str)
    # manager_id['manager_id'] = manager_id['manager_id'].map(str)
    df_create = pd.merge(df_create, manager_id, on='channel_id', how='left')

    # print('df_create_shape', df_create.shape)
    df_create.to_csv('./df_create.csv')

    plan_create = create_plan(df_create, score_image, image_7)

    # print('plan_create', plan_create.shape)
    image_info.dropna(subset=['image_id'], inplace=True)
    image_info['image_id'] = image_info['image_id'].astype(int)
    plan_create = pd.merge(plan_create, image_info[['image_id', 'label_ids']].drop_duplicates(), on='image_id',
                           how='left')

    plan_create_train = plan_create.drop(
        ['budget', 'cpa_bid', 'ad_keywords', 'title_list', 'third_industry_id', 'attribute'],
        axis=1)
    plan_create_train['platform'] = plan_create_train['platform'].map({'[ANDROID]': 1, '[IOS]': 2})

    df['train_label'] = 1
    plan_create_train['train_label'] = 0
    plan_create_train['label'] = -1
    df = df[df['create_time'] >= pd.datetime.now() - pd.DateOffset(180)]
    # print('df', df.shape)
    # print('plan_create_train', plan_create_train.shape)
    df = df.append(plan_create_train)

    df['create_date'] = pd.to_datetime(df['create_date'])
    df['ad_im_sort_id'] = df.groupby(['ad_account_id', 'image_id'])['create_time'].rank()
    df['ad_game_sort_id'] = df.groupby(['ad_account_id', 'game_id'])['create_time'].rank()
    df['im_ad_sort_id'] = df.groupby(['image_id', 'ad_account_id'])['create_time'].rank()

    df = get_mutil_feature(df)

    cat_cols = ['ad_account_id', 'game_id', 'schedule_time', 'delivery_range', 'flow_control_mode',
                'smart_bid_type', 'hide_if_converted', 'gender', 'location_type', 'launch_price',
                'android_osv', 'ios_osv', 'interest_action_mode', 'action_days', 'image_id', 'label_ids',
                'deep_bid_type']
    from itertools import combinations
    from tqdm import tqdm_notebook
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
        "mediaId": 10
    }
    rsp = requests.post(url, json=ad_info, params=params)
    rsp_data = rsp.json()
    print('结束....')
    return rsp_data


def main_model():
    df, plan_create = get_train_df()
    # 训练
    train_data = df[df['train_label'] == 1]
    test_data = df[df['train_label'] == 0]

    train_data = train_data.drop(['train_label', 'create_time', 'create_date'], axis=1)
    test_data = test_data.drop(['train_label', 'create_time', 'create_date'], axis=1)
    target = train_data['label']
    features = train_data.drop(['label'], axis=1)
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
                      valid_sets=[train_data, val_data], verbose_eval=-1)

    # 预测
    features_test = test_data.drop(['label'], axis=1)
    y_predict = model.predict(features_test)

    plan_create['prob'] = y_predict
    threshold = pd.Series(y_predict).sort_values(ascending=False).reset_index(drop=True)[int(y_predict.shape[0] * 0.9)]

    plan_result = plan_create[plan_create['prob'] >= threshold]
    plan_result['rank_ad_im'] = plan_result.groupby(['ad_account_id', 'image_id'])['prob'].rank(ascending=False,
                                                                                                method='first')
    plan_result = plan_result[plan_result['rank_ad_im'] <= 1]

    plan_create['rank_ad_im'] = plan_create.groupby(['ad_account_id', 'image_id'])['prob'].rank(ascending=False,
                                                                                                method='first')
    plan_result_pr = plan_create[plan_create['rank_ad_im'] <= 1]
    ad_num = plan_result['ad_account_id'].value_counts()
    for ad in np.setdiff1d(plan_create['ad_account_id'].values, ad_num[ad_num > 2].index):
        add_plan = plan_result_pr[plan_result_pr['ad_account_id'] == ad].sort_values('prob', ascending=False)[0:3]
        plan_result = plan_result.append(add_plan)

    plan_result['weight'] = plan_result.groupby(['ad_account_id'])['game_id'].transform('count')
    # 只跑每次付费
    plan_result = plan_result[plan_result['deep_bid_type'] == 'BID_PER_ACTION']  ## TODO

    # ad_account_id_group = np.array([9872, 9873, 9874, 9875, 9876])  ## TODO
    ad_account_id_group = np.array([6987])
    plan_result_n = pd.DataFrame()
    for account_id in ad_account_id_group:
        plan_result_ = plan_result[plan_result['ad_account_id'] == account_id]
        # print(plan_result_.shape)
        plan_num = plan_result_['image_id'].nunique()
        if plan_num >= 5:
            plan_num = 5
        plan_result_ = plan_result_.sample(plan_num)
        plan_result_n = plan_result_n.append(plan_result_)
    plan_result = plan_result_n

    # if plan_result.shape[0] > 40:
    #     plan_result = plan_result.sample(40, weights=plan_result['weight'])
    # if plan_result.shape[0] > 12:
    #     plan_result = plan_result.sample(12)

    plan_result = plan_result.drop(['create_time', 'create_date', 'prob', 'rank_ad_im', 'label_ids', 'weight'], axis=1)
    plan_result['convertIndex'] = plan_result['deep_bid_type'].apply(lambda x: 13 if x == 'BID_PER_ACTION' else 14)

    # 优选广告位
    plan_result['inventory_type'] = plan_result['inventory_type'].map(str)
    plan_result['inventory_type'] = plan_result['inventory_type'].apply(lambda x: "[]" if x ==
                  "['INVENTORY_UNION_SLOT', 'INVENTORY_AWEME_FEED', 'INVENTORY_FEED', 'INVENTORY_UNION_SPLASH_SLOT', "
                  "'INVENTORY_VIDEO_FEED', 'INVENTORY_HOTSOON_FEED', 'INVENTORY_TOMATO_NOVEL']" else x)
    # 只跑优选广告位 ## TODO
    # plan_result['inventory_type'] = plan_result['inventory_type'].apply(lambda x: "[]")

    plan_result['inventory_type'] = plan_result['inventory_type'].apply(ast.literal_eval)

    plan_result['budget'] = plan_result.apply(lambda x: x.budget if x.budget >= x.cpa_bid else x.cpa_bid, axis=1)
    plan_result['budget'] = plan_result['budget'].apply(np.ceil)
    # plan_result['cpa_bid'] = plan_result['cpa_bid'].apply(lambda x: x if x >= 520 else random.randint(520, 550))
    plan_result['cpa_bid'] = plan_result['cpa_bid'].apply(lambda x: random.randint(760, 800))
    # plan_result['cpa_bid'] = plan_result['cpa_bid'].apply(lambda x: random.randint(650, 680))
    # plan_result['operation'] = 'disable'
    plan_result['web_url'] = 'https://www.chengzijianzhan.com/tetris/page/7044279938891825160/'
    plan_result['plan_auto_task_id'] = "11002,10998,12098"
    plan_result['op_id'] = 13268
    plan_result['district'] = 'CITY'
    plan_result['flag'] = '录屏'

    # 人群包报错，直接先赋值为[]
    # plan_result['retargeting_tags_include'] = [[] for _ in range(len(plan_result))]
    # plan_result['retargeting_tags_exclude'] = [[] for _ in range(len(plan_result))]
    # 周三周四凌晨更新，不跑计划
    plan_result['schedule_time'] = plan_result['schedule_time'].apply(lambda x: "111111111111111111111111111111111111111111"
                                                                            "111111111111111111111111111111111111111111"
                                                                            "111111111111111111111100000000001111111111"
                                                                            "111111111111111111111111111100000000001111"
                                                                            "111111111111111111111111111111111111111111"
                                                                            "111111111111111111111111111111111111111111"
                                                                            "111111111111111111111111111111111111111111"
                                                                            "111111111111111111111111111111111111111111")

    plan_result.to_csv('./plan_result.csv', index=0)  # 保存创建日志
    print('计划数量%d' % plan_result.shape[0])
    rsp_data = get_ad_create(plan_result)
    print(rsp_data)


if __name__ == '__main__':
    main_model()
