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
        SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1056 AND dev_game_id IS NOT NULL 
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
        /*手动查询*/
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

    if 'deep_conversion_behavior_spec' not in plan_info.columns:
        plan_info['deep_conversion_behavior_spec'] = np.nan

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


# 获取近期60天优化计划的创意数据
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
def get_score_image():
    conn = connect(host='192.168.0.97', port=10000, auth_mechanism='PLAIN', user='hadoop',
                   password='Ycjh8FxiaoMtShZRd3-97%3hCEL0CK4ns1w', database='default')
    cursor = conn.cursor()
    sql_engine = 'set hive.execution.engine=tez'
    sql = 'select image_id,label_ids from dws.dws_image_score_d where media_id=16 and score>=520 and dt=CURRENT_DATE group by image_id,label_ids'
    cursor.execute(sql_engine)
    cursor.execute(sql)
    result = as_pandas(cursor)
    result['label_ids'] = result['label_ids'].astype(str)
    result['label_ids'] = result['label_ids'].apply(lambda x: x.strip('-1;') if '-1' in x else x)
    result['label_ids'] = pd.to_numeric(result['label_ids'], errors='coerce')

    # 关闭链接
    cursor.close()
    conn.close()

    return result['image_id'].values


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
        /*手动查询*/
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
            b.tdate >= date( NOW() - INTERVAL 168 HOUR ) 
            AND b.tdate_type = 'day' 
            AND b.media_id = 16 
            AND b.game_id IN ({}) 
            AND b.amount >= 200 
            AND b.pay_role_user_num >= 1 
            AND b.new_role_money >= 24
            AND (b.new_role_money / b.amount)>=0.01
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


def create_plan(df, image_adcreative_elements):
    # 选ad_account_id、image_id每个账号+素材8条
    game_id = 1001379  ## 选择包：n1计划-IOS联运-IOS联运
    plan = image_adcreative_elements.copy()
    plan = plan.reset_index(drop=True)
    plan['game_id'] = game_id

    # 选择预算，不限制预算
    plan['budget_mode'] = 0
    plan['daily_budget'] = 0

    sample_df = df[
        ['expand_enabled', 'expand_targeting', 'device_price', 'app_install_status', 'gender', 'game_consumption_level',
         'age', 'network_type', 'conversion_behavior_list', 'excluded_dimension', 'location_types', 'regions',
         'intention_targeting_tags', 'interest_category_id_list', 'interest_keyword_list',
         'behavior_category_id_list', 'behavior_intensity', 'behavior_keyword_list', 'behavior_scene',
         'behavior_time_window',
         'site_set', 'deep_link_url', 'page_spec', 'page_type', 'link_page_spec', 'adcreative_elements',
         'link_name_type', 'link_page_type', 'promoted_object_id', 'profile_id', 'promoted_object_type',
         'automatic_site_enabled', 'label',
         'optimization_goal', 'bid_strategy', 'bid_amount', 'deep_conversion_type', 'deep_conversion_behavior_spec',
         'deep_conversion_worth_spec', 'time_series']]
    sample_df = sample_df.sample(n=plan.shape[0], replace=True).reset_index(drop=True)

    plan = pd.concat([plan, sample_df], axis=1)

    #     plan['site_set'] = plan['site_set'].apply(lambda x:ast.literal_eval(x) if x != "nan" and not pd.isnull(x) else [])  ## TODO:支持自动化版位修改
    plan['promoted_object_id'] = '1111059412'  ## TODO
    plan = plan.reset_index(drop=True)

    return plan


def get_plan():
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
    plan_info_current = plan_info[plan_info['create_time'] >= pd.datetime.now() - pd.DateOffset(30)]  ## TODO:时间线拉长
    now_plan_roi = get_now_plan_roi()
    score_image = get_score_image()
    image_info = image_info[image_info['image_id'].notna()]
    image_info['image_id'] = image_info['image_id'].astype(int)
    df_create = pd.merge(plan_info_current, image_info, on=['channel_id', 'source_id', 'image_id'], how='left')
    df_create = pd.merge(df_create, now_plan_roi.drop(['ad_account_id'], axis=1), on=['channel_id', 'source_id'],
                         how='inner')
    # game_id = 1001379  ## 选择包：幸存者挑战
    df_create = df_create[df_create['game_id'].isin([1001379, 1001757, 1001841])]

    image_id_group = np.intersect1d(df_create['image_id'].unique(), score_image)
    print('匹配的素材:', image_id_group)
    image_adcreative_elements = df_create[['image_id', 'adcreative_template_id']]
    image_adcreative_elements = image_adcreative_elements[image_adcreative_elements['image_id'].isin(image_id_group)]
    image_adcreative_elements = image_adcreative_elements.drop_duplicates(subset='image_id', keep='first')

    df_create['site_set'] = df_create['site_set'].map(str)
    df_create_s = df_create[(df_create['site_set'] == "['SITE_SET_MOMENTS']") & (df_create['adcreative_template_id'] == 721)]  ## TODO
    print('df_create_s', df_create_s.shape)
    plan_create = create_plan(df_create_s, image_adcreative_elements)

    ad_account_id_group = np.array([8082, 8854, 8817, 8843, 8077, 8839])
    # num_pre = int(plan_create.shape[0] / len(ad_account_id_group))
    plan_num = 5
    plan_result = pd.DataFrame()
    for ad_account_id in ad_account_id_group:
        plan_result_ = plan_create.sample(n=plan_num, replace=False).reset_index(drop=True)
        plan_result_['ad_account_id'] = ad_account_id
        plan_result = plan_result.append(plan_result_)

    return plan_result


def get_ad_create(plan_result):
    ad_info = []
    for i in range(plan_result.shape[0]):
        ad_info.append(json.loads(plan_result.iloc[i].to_json()))
    open_api_url_prefix = "https://ptom.caohua.com/"
    # open_api_url_prefix = "http://192.168.0.60:8085/"
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


def image_size(image_id):
    """ 匹配：素材-版位 """
    conn = pymysql.connect(host='db-slave-modeltoufang-001.ch', port=3306, user='model_read',
                           passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8', db='db_ptom')
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
    size = (720, 1280)
    # 匹配
    new_id = new_ids[(new_ids['width'] == size[0]) & (new_ids['height'] == size[1])]['id']
    assert len(new_id) > 0, "主素材{}找不到对应的{}尺寸".format(image_id, (size[0], size[1]))
    return new_id.values[0]


def main_model():
    plan_result = get_plan()
    plan_result['site_set'] = "['SITE_SET_MOMENTS']"  ## TODO 固定跑朋友圈
    plan_result['site_set'] = plan_result['site_set'].map(str)
    # [SITE_SET_WECHAT] 公众号和小程序adcreative_template_id只跑1480、560、720、721、1064五种
    plan_result = plan_result[~((plan_result['site_set'] == "['SITE_SET_WECHAT']") & (
        ~plan_result['adcreative_template_id'].isin([1480, 560, 720, 721, 1064])))]

    plan_result['location_types'] = plan_result['location_types'].apply(lambda x: ['LIVE_IN'] if x == x else x)

    # 修改落地页ID
    plan_result['page_spec'] = plan_result.apply(
        lambda x: {'override_canvas_head_option': 'OPTION_CREATIVE_OVERRIDE_CANVAS',
                   'page_id': '2230311524'} if x.site_set == "['SITE_SET_MOMENTS']"
        else ({'override_canvas_head_option': 'OPTION_CREATIVE_OVERRIDE_CANVAS',
               'page_id': '2179536949'} if x.site_set == "['SITE_SET_WECHAT']" else np.nan), axis=1)
    plan_result['link_page_spec'] = plan_result.apply(
        lambda x: {'page_id': '2230311524'} if x.site_set == "['SITE_SET_MOMENTS']"
        else ({'page_id': '2179536949'} if x.site_set == "['SITE_SET_WECHAT']" else np.nan), axis=1)

    # 朋友圈头像ID
    plan_result['ad_account_id'] = plan_result['ad_account_id'].map(str)
    plan_result_1 = plan_result[plan_result['site_set'] == "['SITE_SET_MOMENTS']"]
    plan_result_2 = plan_result[plan_result['site_set'] != "['SITE_SET_MOMENTS']"]
    profile_id_dict = {'7981': '372606', '7982': '372597', '7983': '372591', '7984': '372585', '7985': '372485',
                       '8035': '383952', '8036': '383967', '8037': '383976', '8038': '383987', '8039': '383994',
                       '8082': '408038', '8081': '408049', '8080': '408052', '8079': '408056', '8078': '408059',
                       '8077': '408062', '8076': '408066', '8075': '408069', '8074': '408073', '8073': '408082',
                       '8814': '508418', '8815': '508430', '8816': '508441', '8817': '508450', '8818': '508455',
                       '8819': '508460', '8820': '508465', '8821': '508478', '8822': '508484', '8823': '508490',
                       '8824': '508498', '8825': '508500', '8826': '508506', '8827': '508513', '8828': '508525',
                       '8829': '508528', '8830': '508531', '8831': '508535', '8832': '508540', '8833': '508550',
                       '8834': '508557', '8835': '508565', '8836': '508572', '8837': '508577', '8838': '508580',
                       '8839': '508584', '8840': '508591', '8841': '508596', '8842': '508602', '8843': '508603',
                       '8844': '508608', '8845': '508611', '8846': '508620', '8847': '508634', '8848': '508646',
                       '8854': '519213', '8855': '519237', '8856': '519243', '8857': '519248', '8858': '519259',
                       '8859': '519267', '8860': '519273', '8743': '519293', '8742': '519300', '8741': '519305',
                       '8740': '519312', '8739': '519323', '8738': '519335', '8737': '519346', '8736': '519356',
                       '8735': '519363', '8734': '519369', '8733': '519377', '8732': '519385', '8731': '519393',
                       '8730': '519399', '8729': '519407', '8728': '519409', '8727': '519414', '8726': '519423',
                       '8725': '519429', '8724': '519438'}
    plan_result_1['profile_id'] = plan_result_1['ad_account_id'].map(profile_id_dict)

    plan_result = plan_result_1.append(plan_result_2)
    plan_result = plan_result.reset_index(drop=True)

    # 年龄定向
    plan_result['age'] = plan_result['age'].apply(lambda x: [{'min': 20, 'max': 50}])

    for i in range(plan_result.shape[0]):
        a = plan_result.loc[i, 'adcreative_elements'].copy()
        image_detail_id = image_size(plan_result.loc[i, 'image_id'])
        a['video'] = image_detail_id
        plan_result.loc[i, 'adcreative_elements'] = str(a)

    # 固定为roi出价方式  ##TODO
    plan_result['optimization_goal'] = plan_result['optimization_goal'].apply(lambda x: 'OPTIMIZATIONGOAL_APP_ACTIVATE')  ## TODO  固定ROI
    plan_result['deep_conversion_type'] = plan_result['optimization_goal'].apply(lambda x: 'DEEP_CONVERSION_WORTH' if x == 'OPTIMIZATIONGOAL_APP_ACTIVATE' else np.nan)
    plan_result['deep_conversion_worth_spec'] = plan_result['optimization_goal'].apply(lambda x: {'goal': 'GOAL_1DAY_PURCHASE_ROAS', 'expected_roi': 0.02} if x == 'OPTIMIZATIONGOAL_APP_ACTIVATE' else np.nan)
    plan_result['bid_amount'] = plan_result['optimization_goal'].apply(lambda x: random.randint(9000, 10000) if x == 'OPTIMIZATIONGOAL_APP_ACTIVATE'
                            else (random.randint(95000, 100000) if x == 'OPTIMIZATIONGOAL_APP_PURCHASE'
                                  else (random.randint(310000, 320000))))

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
    plan_result['plan_auto_task_id'] = "11427,12063"
    plan_result['op_id'] = 13268
    plan_result['flag'] = 'GDT'
    plan_result['game_name'] = '幸存者挑战'
    plan_result['platform'] = 1
    plan_result['ad_account_id'] = plan_result['ad_account_id'].astype(int)
    plan_result['site_set'] = plan_result['site_set'].apply(ast.literal_eval)
    plan_result['adcreative_template_id'] = 721  # 朋友圈竖版素材
    plan_result['link_name_type'] = 'DOWNLOAD_APP'  # 下载app
    # 开启一键起量
    plan_result['auto_acquisition_enabled'] = True
    plan_result['auto_acquisition_budget'] = 6666
    # 开启自动扩量
    plan_result['expand_enabled'] = True
    plan_result['expand_targeting'] = plan_result['expand_targeting'].apply(lambda x: ['age'])
    # # 开启加速投放
    plan_result['speed_mode'] = 'SPEED_MODE_FAST'
    # 固定品牌名
    plan_result = plan_result.reset_index(drop=True)
    plan_result['adcreative_elements'] = plan_result['adcreative_elements'].apply(ast.literal_eval)
    for i in range(plan_result.shape[0]):
        a = plan_result.loc[i, 'adcreative_elements'].copy()
        a['brand'] = {'brand_name': '幸存者挑战', 'brand_img': 81958}
        plan_result.loc[i, 'adcreative_elements'] = str(a)

    plan_result['adcreative_elements'] = plan_result['adcreative_elements'].apply(ast.literal_eval)

    # 周三周四更新，凌晨不跑计划
    # plan_result['time_series'] = plan_result['time_series'].apply(
    #     lambda x: x[0:96] + '1111111111000000000011' + x[118:144] + '1111111111000000000011' + x[166:])
    plan_result['time_series'] = plan_result['time_series'].apply(lambda x: "111111111111111111111111111111111111111111"
                                                                            "111111111111111111111111111111111111111111"
                                                                            "111111111111111111111100000000001111111111"
                                                                            "111111111111111111111111111100000000001111"
                                                                            "111111111111111111111111111111111111111111"
                                                                            "111111111111111111111111111111111111111111"
                                                                            "111111111111111111111111111111111111111111"
                                                                            "111111111111111111111111111111111111111111")
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
