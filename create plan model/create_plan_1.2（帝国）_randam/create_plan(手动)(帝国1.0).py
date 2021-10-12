import pandas as pd
import numpy as np
import json
from sklearn.model_selection import train_test_split
from sklearn.decomposition import PCA
from sklearn.preprocessing import LabelEncoder, MinMaxScaler
import gc
import warnings
import requests

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
        SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1112 AND dev_game_id IS NOT NULL 
    '''
    cur.execute(sql)
    result_df = cur.fetchall()
    cur.close()
    conn.close()
    return result_df


# 获取近期所有计划('2021年5月7号开始')
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
                AND create_time>='2021-05-07'
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


def get_launch_report():
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


# 获取近期账号维度窗口期统计数据
def getAdData(conn, begin, end, n):
    originSql = '''
              SELECT
                a.ad_account_id AS 'ad_account_id',
                ifnull( a.ad_amount, 0 ) AS 'ad_amount',
                ifnull( a.ad_create_role_num, 0 ) AS 'ad_create_role_num',
                ifnull( a.ad_new_role_money, 0 ) AS 'ad_new_role_money',
                ifnull( a.ad_pay_role_user_num, 0 ) AS 'ad_pay_role_user_num',
                ifnull( a.ad_source_num, 0 ) AS 'ad_source_num',
                IFNULL( b.ad_valid_source_num, 0 ) AS 'ad_valid_source_num',
                (
                CASE

                        WHEN ifnull( a.ad_create_role_num, 0 )= 0 THEN
                        0 ELSE IFNULL( a.ad_pay_role_user_num, 0 ) / ifnull( a.ad_create_role_num, 0 ) 
                    END 
                    ) AS 'ad_pay_rate',
                    (
                    CASE

                            WHEN ifnull( a.ad_create_role_num, 0 )= 0 THEN
                            0 ELSE IFNULL( a.ad_amount, 0 ) / ifnull( a.ad_create_role_num, 0 ) 
                        END 
                        ) AS 'ad_create_role_cost',
                        (
                        CASE

                                WHEN ifnull( a.ad_pay_role_user_num, 0 )= 0 THEN
                                0 ELSE IFNULL( a.ad_amount, 0 ) / ifnull( a.ad_pay_role_user_num, 0 ) 
                            END 
                            ) AS 'ad_create_role_pay_cost',
                            (
                            CASE

                                    WHEN ifnull( a.ad_source_num, 0 )= 0 THEN
                                    0 ELSE IFNULL( b.ad_valid_source_num, 0 ) / ifnull( a.ad_source_num, 0 ) 
                                END 
                                ) AS 'ad_valid_source_rate',
                                (
                                CASE

                                        WHEN ifnull( a.ad_amount, 0 )= 0 THEN
                                        0 ELSE IFNULL( a.ad_new_role_money, 0 ) / ifnull( a.ad_amount, 0 ) 
                                    END 
                                    ) AS 'ad_create_role_roi' 
                                FROM
                                    (
                                    SELECT
                                        a.ad_account_id,
                                        ifnull( sum( a.amount ), 0 ) AS 'ad_amount',
                                        ifnull( sum( a.create_role_num ), 0 ) AS 'ad_create_role_num',
                                        ifnull( sum( b.new_role_money ), 0 ) AS 'ad_new_role_money',
                                        ifnull( sum( b.pay_role_user_num ), 0 ) AS 'ad_pay_role_user_num',
                                        count( DISTINCT a.source_id ) AS 'ad_source_num' 
                                    FROM
                                        (
                                        SELECT
                                            b.ad_account_id,
                                            b.doc_id,
                                            a.game_id,
                                            a.channel_id,
                                            a.source_id,
                                            a.amount,
                                            a.create_role_num 
                                        FROM
                                            db_stdata.st_lauch_report a
                                            INNER JOIN db_data_ptom.ptom_plan b ON a.game_id = b.game_id 
                                            AND a.source_id = b.source_id 
                                            AND a.channel_id = b.chl_user_id 
                                        WHERE
                                            a.tdate_type = 'day' 
                                            AND a.tdate >= '{begin}' 
                                            AND a.tdate <= '{end}' AND a.amount > 100 
                                            AND a.media_id = 10 
                                        AND a.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1112 AND dev_game_id IS NOT NULL )) a
                                        LEFT JOIN (
                                        SELECT
                                            c.game_id,
                                            c.channel_id,
                                            c.source_id,
                                            b.ad_account_id,
                                            sum( c.create_role_money ) new_role_money,
                                            IFNULL( sum( c.pay_role_user_num ), 0 ) AS pay_role_user_num 
                                        FROM
                                            db_stdata.st_game_days c
                                            INNER JOIN db_data_ptom.ptom_plan b ON c.game_id = b.game_id 
                                            AND c.source_id = b.source_id 
                                            AND c.channel_id = b.chl_user_id 
                                        WHERE
                                            c.report_days = {n} 
                                            AND c.tdate = '{end}' 
                                            AND c.tdate_type = 'day' 
                                            AND c.query_type = 13 
                                            AND c.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1112 AND dev_game_id IS NOT NULL ) 
                                        GROUP BY
                                            c.game_id,
                                            c.channel_id,
                                            c.source_id 
                                        HAVING
                                            ( new_role_money > 0 OR pay_role_user_num > 0 ) 
                                        ) b ON a.source_id = b.source_id 
                                        AND a.channel_id = b.channel_id 
                                    GROUP BY
                                        a.ad_account_id 
                                    ) a
                                    LEFT JOIN (
                                    SELECT
                                        b.ad_account_id,
                                        count( DISTINCT b.plan_id ) AS 'ad_valid_source_num' 
                                    FROM
                                        db_stdata.st_lauch_report a
                                        INNER JOIN db_data_ptom.ptom_plan b ON a.game_id = b.game_id 
                                        AND a.source_id = b.source_id 
                                        AND a.channel_id = b.chl_user_id 
                                    WHERE
                                        a.tdate_type = 'day' 
                                        AND a.tdate >= '{begin}' 
                                        AND a.tdate <= '{end}' AND a.amount > 100  
                                        AND a.media_id = 10 
                                        AND a.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1112 AND dev_game_id IS NOT NULL ) 
                                        AND a.pay_role_user_num > 0 
                                    GROUP BY
                                    b.ad_account_id 
                ) b ON A.ad_account_id = B.ad_account_id
    '''
    finalSql = originSql.format(begin=begin, end=end, n=n)
    result = pd.read_sql(finalSql, conn)
    return result


# 获取素材报表数据
def getImData(conn, begin, end, n):
    originSql = '''
            SELECT
                a.image_id AS 'image_id',
                ifnull( a.im_amount, 0 ) AS 'im_amount',
                ifnull( a.im_create_role_num, 0 ) AS 'im_create_role_num',
                ifnull( a.im_new_role_money, 0 ) AS 'im_new_role_money',
                ifnull( a.im_pay_role_user_num, 0 ) AS 'im_pay_role_user_num',
                ifnull( a.im_source_num, 0 ) AS 'im_source_num',
                IFNULL( b.im_valid_source_num, 0 ) AS 'im_valid_source_num',
                (
                CASE

                        WHEN ifnull( a.im_create_role_num, 0 )= 0 THEN
                        0 ELSE IFNULL( a.im_pay_role_user_num, 0 ) / ifnull( a.im_create_role_num, 0 ) 
                    END 
                    ) AS 'im_pay_rate',
                    (
                    CASE

                            WHEN ifnull( a.im_create_role_num, 0 )= 0 THEN
                            0 ELSE IFNULL( a.im_amount, 0 ) / ifnull( a.im_create_role_num, 0 ) 
                        END 
                        ) AS 'im_create_role_cost',
                        (
                        CASE

                                WHEN ifnull( a.im_pay_role_user_num, 0 )= 0 THEN
                                0 ELSE IFNULL( a.im_amount, 0 ) / ifnull( a.im_pay_role_user_num, 0 ) 
                            END 
                            ) AS 'im_create_role_pay_cost',
                            (
                            CASE

                                    WHEN ifnull( a.im_source_num, 0 )= 0 THEN
                                    0 ELSE IFNULL( b.im_valid_source_num, 0 ) / ifnull( a.im_source_num, 0 ) 
                                END 
                                ) AS 'im_valid_source_rate',
                                (
                                CASE

                                        WHEN ifnull( a.im_amount, 0 )= 0 THEN
                                        0 ELSE IFNULL( a.im_new_role_money, 0 ) / ifnull( a.im_amount, 0 ) 
                                    END 
                                    ) AS 'image_create_role_roi' 
                                FROM
                                    (
                                    SELECT
                                        a.image_id,
                                        ifnull( sum( a.amount ), 0 ) AS 'im_amount',
                                        ifnull( sum( a.create_role_num ), 0 ) AS 'im_create_role_num',
                                        ifnull( sum( b.new_role_money ), 0 ) AS 'im_new_role_money',
                                        ifnull( sum( b.pay_role_user_num ), 0 ) AS 'im_pay_role_user_num',
                                        count( DISTINCT a.source_id ) AS 'im_source_num' 
                                    FROM
                                        (
                                        SELECT
                                            b.image_id,
                                            a.game_id,
                                            a.channel_id,
                                            a.source_id,
                                            a.amount,
                                            a.create_role_num 
                                        FROM
                                            db_stdata.st_lauch_report a
                                            INNER JOIN db_data_ptom.ptom_plan b ON a.game_id = b.game_id 
                                            AND a.source_id = b.source_id 
                                            AND a.channel_id = b.chl_user_id                                        
                                        WHERE
                                            a.tdate_type = 'day' 
                                            AND a.tdate >= '{begin}' 
                                            AND a.tdate <= '{end}' AND a.amount > 100 
                                            AND b.image_id IS NOT NULL 
                                            AND b.image_id <> '' 
                                            AND a.media_id = 10 
                                        AND a.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1112 AND dev_game_id IS NOT NULL )) a
                                        LEFT JOIN (
                                        SELECT
                                            c.game_id,
                                            c.channel_id,
                                            c.source_id,
                                            b.image_id,
                                            sum( c.create_role_money ) new_role_money,
                                            IFNULL( sum( c.pay_role_user_num ), 0 ) AS pay_role_user_num 
                                        FROM
                                            db_stdata.st_game_days c
                                            INNER JOIN db_data_ptom.ptom_plan b ON c.game_id = b.game_id 
                                            AND c.source_id = b.source_id 
                                            AND c.channel_id = b.chl_user_id 
                                        WHERE
                                            c.report_days = {n} 
                                            AND c.tdate = '{end}' 
                                            AND c.tdate_type = 'day' 
                                            AND c.query_type = 13 
                                            AND b.image_id IS NOT NULL 
                                            AND b.image_id <> '' 
                                            AND c.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1112 AND dev_game_id IS NOT NULL ) 
                                        GROUP BY
                                            c.game_id,
                                            c.channel_id,
                                            c.source_id 
                                        HAVING
                                            ( new_role_money > 0 OR pay_role_user_num > 0 ) 
                                        ) b ON a.source_id = b.source_id 
                                        AND a.channel_id = b.channel_id 
                                    GROUP BY
                                        a.image_id 
                                    ) a
                                    LEFT JOIN (
                                    SELECT
                                        b.image_id,
                                        count( DISTINCT b.plan_id ) AS 'im_valid_source_num' 
                                    FROM
                                        db_stdata.st_lauch_report a
                                        INNER JOIN db_data_ptom.ptom_plan b ON a.game_id = b.game_id 
                                        AND a.source_id = b.source_id 
                                        AND a.channel_id = b.chl_user_id 
                                    WHERE
                                        a.tdate_type = 'day' 
                                        AND a.tdate >= '{begin}' 
                                        AND a.tdate <= '{end}' AND a.amount > 100 
                                        AND b.image_id IS NOT NULL 
                                        AND b.image_id <> '' 
                                        AND a.media_id = 10 
                                        AND a.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1112 AND dev_game_id IS NOT NULL ) 
                                        AND a.pay_role_user_num > 0 
                                    GROUP BY
                                        b.image_id 
                                    ) b ON A.image_id = B.image_id
    '''
    finalSql = originSql.format(begin=begin, end=end, n=n)
    result = pd.read_sql(finalSql, conn)
    return result


def etl_data(start, end, n):
    '''
    获取账号、素材、账号+素材维度的数据
    :return:
    '''
    # 链接数据库，并创建游标
    conn1 = pymysql.connect(host='192.168.0.79', port=3306, user='readonly',
                            passwd='Kc&r+z1ql9x8za4kzfk761weq8ozGv3ZpI;RMe,#+s%c>t', db='db_data')
#     columns = ['ad_account_id', 'ad_amount', 'ad_create_role_num', 'ad_new_role_money',
#                'ad_pay_role_user_num', 'ad_source_num', 'ad_valid_source_num',
#                'ad_pay_rate', 'ad_create_role_cost', 'ad_create_role_pay_cost',
#                'ad_valid_source_rate', 'ad_create_role_roi', 'model_run_datetime']
    result_im = pd.DataFrame()
    result_ad = pd.DataFrame()
#     result_ad_im = pd.DataFrame()
    date_list = pd.date_range(start=start, end=end)
    for date in date_list:
        end = date
        begin = date - pd.Timedelta(days=n - 1)
        end = str(end).split(' ')[0]
        begin = str(begin).split(' ')[0]
        # 获取素材窗口期数据
        ImData = getImData(conn1, begin, end, n=n)
        ImData['model_run_datetime'] = date + pd.Timedelta(days=1)
        # 获取账号窗口期数据
        AdData = getAdData(conn1, begin, end, n=n)
        AdData['model_run_datetime'] = date + pd.Timedelta(days=1)
#         # 获取账号+素材窗口期数据
#         AdImData = getAdImData(conn1, begin, end, n=n)
#         AdImData['model_run_datetime'] = date + pd.Timedelta(days=1)
        result_im = result_im.append(ImData)
        result_ad = result_ad.append(AdData)
#         result_ad_im = result_ad_im.append(AdImData)

    conn1.close()
    return result_im, result_ad


def fill_date(df, col):
    result_df = pd.DataFrame()
    for im_id in df[col].unique():
        temp = df[df[col] == im_id]
        im_date = temp['create_date'].values
        all_date = pd.date_range(temp['create_date'].min() - pd.DateOffset(days=1), temp['create_date'].max(), freq='D')
        new_date = pd.DataFrame(np.setdiff1d(all_date, im_date), columns=['create_date'])
        new_date[col] = im_id
        temp = pd.concat([temp, new_date])
        temp.sort_values(by='create_date', inplace=True)
        temp = temp.reset_index(drop=True)
        temp = temp.fillna(method='ffill')
        temp = temp.fillna(method='bfill')
        result_df = result_df.append(temp)
    return result_df


# 解析json
def get_plan_json(plan_info):
    plan_info.drop(['inventory_type','budget'], axis=1, inplace=True)
    plan_info.dropna(how='all', inplace=True, axis=1)
    plan_info.dropna(subset=['ad_info'],inplace=True)
    # 解析json
    plan_info['ad_info'] = plan_info['ad_info'].apply(json.loads)
    temp = plan_info['ad_info'].apply(pd.Series)
    plan_info = pd.concat([plan_info,temp], axis=1)
    plan_info.drop('ad_info', axis=1, inplace=True)
    temp = plan_info['audience'].apply(pd.Series)
    plan_info = pd.concat([plan_info,temp], axis=1)
    plan_info.drop('audience', axis=1, inplace=True)
    temp = plan_info['action'].apply(pd.Series)
    plan_info = pd.concat([plan_info,temp], axis=1)
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
           'action_categories', 'action_days', 'action_scene','deep_bid_type','roi_goal']]
    return plan_info


def get_all_data():
    # 读取历史数据
    plan_info = pd.read_csv('./hist_data/ptom_third_plan.csv')
    #     image_info = pd.read_csv('./hist_data/image_info.csv')
    #     launch_report = pd.read_csv('./hist_data/launch_report.csv')

    plan_info_new = get_plan_info()
    image_info = get_image_info()
    launch_report = get_launch_report()

    plan_info = plan_info.append(plan_info_new)
    plan_info = get_plan_json(plan_info)
    #     image_info = image_info.append(image_info_new)
    #     launch_report = launch_report.append(launch_report_new)
    image_info.dropna(subset=['image_id'], inplace=True)
    image_info['image_id'] = image_info['image_id'].astype(int)
    return plan_info, image_info, launch_report


# 获取score_image ()
def get_score_image():
    conn = connect(host='192.168.0.88', port=10000, auth_mechanism='PLAIN', user='hadoop',
                   password='Ycjh8FxiaoMtShZRd3-97%3hCEL0CK4ns1w', database='default')
    cursor = conn.cursor()
    sql_engine = 'set hive.execution.engine=tez'
    sql = 'select image_id,label_ids from dws.dws_image_score_d where media_id=10 and score>=580 and dt=CURRENT_DATE group by image_id,label_ids'
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

    conn = pymysql.connect(host='192.168.0.79', port=3306, user='readonly',
                           passwd='Kc&r+z1ql9x8za4kzfk761weq8ozGv3ZpI;RMe,#+s%c>t')
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
            AND b.new_role_money >= 90
            AND (b.new_role_money / b.amount)>=0.03
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


def create_plan(df, score_image):
    # 选ad_account_id、image_id每个账号+素材8条
    ad_account_id_group = np.array([7590, 7591, 7592, 7593, 7594])
    image_id_group = np.intersect1d(df['image_id'].unique(), score_image)
    # print(image_id_group)
    df = df[df['deep_bid_type'].isin(['BID_PER_ACTION', 'ROI_COEFFICIENT'])]
    df = df[df['image_id'].isin(image_id_group)]
    game_id = 1001545
    df = df[df['game_id'] == game_id]

    plan = pd.DataFrame()
    for ad_account in ad_account_id_group:
        for image in image_id_group:
            #         print(image)
            temp = pd.DataFrame({'ad_account_id': [ad_account], 'image_id': [image]})
            plan = plan.append(temp)
    #         print(temp)
    plan = pd.DataFrame(np.repeat(plan.values, 8, axis=0), columns=plan.columns)

    # game_image = df[['game_id', 'image_id']].drop_duplicates()
    # plan = pd.merge(plan, game_image, on='image_id', how='left')
    plan['game_id'] = game_id

    # 选platform测试期默认[ANDROID]
    plan['platform'] = df['platform'].iloc[0]
    plan['platform'] = plan['platform'].apply(lambda x: '[ANDROID]' if x == 1 else '[IOS]')

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

    # 选'ad_keywords', 'title_list', 'third_industry_id'  创意
    sample_df = df[['ad_keywords', 'title_list', 'third_industry_id']]
    sample_df = sample_df.sample(n=plan.shape[0], replace=True).reset_index(drop=True)
    plan = pd.concat([plan, sample_df], axis=1)

    # 选'retargeting_type','retargeting_tags_include','retargeting_tags_exclude'  人群包定向
    sample_df = df[['retargeting_type', 'retargeting_tags_include', 'retargeting_tags_exclude']]
    sample_df = sample_df.sample(n=plan.shape[0], replace=True).reset_index(drop=True)
    plan = pd.concat([plan, sample_df], axis=1)

    # 选'interest_action_mode','action_scene','action_days','action_categories' ,'interest_categories' 行为兴趣
    sample_df = df[['interest_action_mode', 'action_scene', 'action_days', 'action_categories', 'interest_categories']]
    sample_df = sample_df.sample(n=plan.shape[0], replace=True).reset_index(drop=True)
    plan = pd.concat([plan, sample_df], axis=1)

    # 选'deep_bid_type','roi_goal','smart_bid_type','adjust_cpa','cpa_bid'出价方式
    sample_df = df[['deep_bid_type', 'roi_goal', 'smart_bid_type', 'adjust_cpa', 'cpa_bid']]
    sample_df = sample_df[sample_df['deep_bid_type'].isin(['ROI_COEFFICIENT', 'BID_PER_ACTION'])]
    sample_df = sample_df.sample(n=plan.shape[0], replace=True).reset_index(drop=True)
    plan = pd.concat([plan, sample_df], axis=1)

    # 选
    cols = ['delivery_range', 'city', 'location_type', 'gender', 'age', 'ac', 'launch_price',
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
            data = data.join(data[col].str.join('|').str.get_dummies().add_prefix(col + '_'))
            data.drop(col, axis=1, inplace=True)
        elif col in ['city', 'retargeting_tags_include', 'retargeting_tags_exclude', 'interest_categories',
                     'action_categories']:
            data[col] = data[col].apply(lambda x: x if x == x else [])
            data[col] = data[col].apply(lambda x: [str(i) for i in x])
            temp = data[col].str.join('|').str.get_dummies()
            #         print(temp.shape[1])
            pca = PCA(n_components=0.9)
            temp = pca.fit_transform(temp.values)
            temp = pd.DataFrame(temp, columns=[col + str(i) for i in range(temp.shape[1])])
            #         print(temp.shape[1])
            data = data.join(temp)
            del temp
            data.drop(col, axis=1, inplace=True)
        else:
            data[col] = data[col].apply(lambda x: x if x == x else [])
            data[col] = data[col].apply(lambda x: [str(i) for i in x])
            data = data.join(data[col].str.join('|').str.get_dummies().add_prefix(col + '_'))
            data.drop(col, axis=1, inplace=True)

    gc.collect()
    return data


def get_train_df():
    # 获取账号、素材、账号+素材维度的数据
    # 读取近期数据
    train_im_1, train_ad_1 = etl_data(start='2021-05-09',
                                      end=str((pd.datetime.now() - pd.DateOffset(1)).date()), n=1)
    train_im_1.columns = [i + '_' + str(1) for i in train_im_1.columns]
    train_ad_1.columns = [i + '_' + str(1) for i in train_ad_1.columns]
    train_im_1 = train_im_1.rename(columns={'model_run_datetime_' + str(1): 'create_date',
                                            'image_id_' + str(1): 'image_id'})
    train_ad_1 = train_ad_1.rename(columns={'model_run_datetime_' + str(1): 'create_date',
                                            'ad_account_id_' + str(1): 'ad_account_id'})
    # n=3
    train_im_3, train_ad_3 = etl_data(start='2021-05-09',
                                      end=str((pd.datetime.now() - pd.DateOffset(1)).date()), n=3)
    train_im_3.columns = [i + '_' + str(3) for i in train_im_3.columns]
    train_ad_3.columns = [i + '_' + str(3) for i in train_ad_3.columns]
    train_im_3 = train_im_3.rename(columns={'model_run_datetime_' + str(3): 'create_date',
                                            'image_id_' + str(3): 'image_id'})
    train_ad_3 = train_ad_3.rename(columns={'model_run_datetime_' + str(3): 'create_date',
                                            'ad_account_id_' + str(3): 'ad_account_id'})
    # n=5
    train_im_5, train_ad_5 = etl_data(start='2021-05-09',
                                      end=str((pd.datetime.now() - pd.DateOffset(1)).date()), n=5)
    train_im_5.columns = [i + '_' + str(5) for i in train_im_5.columns]
    train_ad_5.columns = [i + '_' + str(5) for i in train_ad_5.columns]
    train_im_5 = train_im_5.rename(columns={'model_run_datetime_' + str(5): 'create_date',
                                            'image_id_' + str(5): 'image_id'})
    train_ad_5 = train_ad_5.rename(columns={'model_run_datetime_' + str(5): 'create_date',
                                            'ad_account_id_' + str(5): 'ad_account_id'})
    # n=7
    train_im_7, train_ad_7 = etl_data(start='2021-05-09',
                                      end=str((pd.datetime.now() - pd.DateOffset(1)).date()), n=7)
    train_im_7.columns = [i + '_' + str(7) for i in train_im_7.columns]
    train_ad_7.columns = [i + '_' + str(7) for i in train_ad_7.columns]
    train_im_7 = train_im_7.rename(columns={'model_run_datetime_' + str(7): 'create_date',
                                            'image_id_' + str(7): 'image_id'})
    train_ad_7 = train_ad_7.rename(columns={'model_run_datetime_' + str(7): 'create_date',
                                            'ad_account_id_' + str(7): 'ad_account_id'})

    # 读取历史数据
    train_im_data_1 = pd.read_csv('./hist_data/im_data/train_data_1.csv')
    train_im_data_3 = pd.read_csv('./hist_data/im_data/train_data_3.csv')
    train_im_data_5 = pd.read_csv('./hist_data/im_data/train_data_5.csv')
    train_im_data_7 = pd.read_csv('./hist_data/im_data/train_data_7.csv')
    train_ad_data_1 = pd.read_csv('./hist_data/ad_data/train_data_1.csv')
    train_ad_data_3 = pd.read_csv('./hist_data/ad_data/train_data_3.csv')
    train_ad_data_5 = pd.read_csv('./hist_data/ad_data/train_data_5.csv')
    train_ad_data_7 = pd.read_csv('./hist_data/ad_data/train_data_7.csv')

    # 合并数据
    train_im_data_1 = train_im_data_1.append(train_im_1)
    train_im_data_3 = train_im_data_3.append(train_im_3)
    train_im_data_5 = train_im_data_5.append(train_im_5)
    train_im_data_7 = train_im_data_7.append(train_im_7)
    train_ad_data_1 = train_ad_data_1.append(train_ad_1)
    train_ad_data_3 = train_ad_data_3.append(train_ad_3)
    train_ad_data_5 = train_ad_data_5.append(train_ad_5)
    train_ad_data_7 = train_ad_data_7.append(train_ad_7)

    for i in [1, 3, 5, 7]:
        exec("train_im_data_%d['image_id'] = train_im_data_%d['image_id'].astype('int')" % (i, i))
        exec("train_im_data_%d['create_date'] = pd.to_datetime(train_im_data_%d['create_date'])" % (i, i))
        exec("train_ad_data_%d['ad_account_id'] = train_ad_data_%d['ad_account_id'].astype('int')" % (i, i))
        exec("train_ad_data_%d['create_date'] = pd.to_datetime(train_ad_data_%d['create_date'])" % (i, i))

    train_im_data_1 = fill_date(train_im_data_1, col='image_id')
    train_im_data_3 = fill_date(train_im_data_3, col='image_id')
    train_im_data_5 = fill_date(train_im_data_5, col='image_id')
    train_im_data_7 = fill_date(train_im_data_7, col='image_id')
    train_ad_data_1 = fill_date(train_ad_data_1, col='ad_account_id')
    train_ad_data_3 = fill_date(train_ad_data_3, col='ad_account_id')
    train_ad_data_5 = fill_date(train_ad_data_5, col='ad_account_id')
    train_ad_data_7 = fill_date(train_ad_data_7, col='ad_account_id')

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

    df.drop(['budget', 'cpa_bid', 'channel_id', 'source_id', 'amount', 'roi', 'pay_rate', 'new_role_money'], axis=1, inplace=True)

    plan_info['create_time'] = pd.to_datetime(plan_info['create_time'])
    plan_info_current = plan_info[plan_info['create_time'] >= pd.datetime.now() - pd.DateOffset(8)]

    creative_info = get_creative()
    creative_info['title_list'] = creative_info['title_list'].fillna('[]')
    creative_info['ad_keywords'] = creative_info['ad_keywords'].fillna('[]')
    creative_info['title_list'] = creative_info['title_list'].apply(json.loads)
    creative_info['ad_keywords'] = creative_info['ad_keywords'].apply(json.loads)

    now_plan_roi = get_now_plan_roi()
    now_plan_roi = pd.merge(now_plan_roi, creative_info, on=['channel_id', 'source_id'], how='left')

    score_image = get_score_image()

    df_create = pd.merge(plan_info_current, image_info, on=['channel_id', 'source_id'], how='left')
    df_create = pd.merge(df_create, now_plan_roi, on=['channel_id', 'source_id'], how='inner')

    df_create['platform'] = df_create['platform'].astype(str)
    df_create['platform'] = df_create['platform'].map({"['ANDROID']": 1, "['IOS']": 2})
    df_create['platform'] = df_create['platform'].astype(int)

    df_create.dropna(subset=['image_id'], inplace=True)
    df_create['image_id'] = df_create['image_id'].astype(int)

    df_create = df_create[df_create['platform'] == 1]
    # print('df_create_shape', df_create.shape)
    df_create.to_csv('./df_create.csv')

    plan_create = create_plan(df_create, score_image)

    # print('plan_create', plan_create.shape)

    image_info.dropna(subset=['image_id'], inplace=True)
    image_info['image_id'] = image_info['image_id'].astype(int)
    plan_create = pd.merge(plan_create, image_info[['image_id', 'label_ids']].drop_duplicates(), on='image_id',
                           how='left')

    plan_create_train = plan_create.drop(['budget', 'cpa_bid', 'ad_keywords', 'title_list', 'third_industry_id'],
                                         axis=1)
    plan_create_train['platform'] = plan_create_train['platform'].map({'[ANDROID]': 1, '[IOS]': 2})
    df['train_label'] = 1
    plan_create_train['train_label'] = 0
    plan_create_train['label'] = -1
    df = df[df['create_time'] >= pd.datetime.now() - pd.DateOffset(180)]
    print('df', df.shape)
    print('plan_create_train', plan_create_train.shape)
    df = df.append(plan_create_train)

    df = pd.merge(df, train_im_data_1, on=['image_id', 'create_date'], how='left', validate='many_to_one')
    df = pd.merge(df, train_im_data_3, on=['image_id', 'create_date'], how='left', validate='many_to_one')
    df = pd.merge(df, train_im_data_5, on=['image_id', 'create_date'], how='left', validate='many_to_one')
    df = pd.merge(df, train_im_data_7, on=['image_id', 'create_date'], how='left', validate='many_to_one')

    df = pd.merge(df, train_ad_data_1, on=['ad_account_id', 'create_date'], how='left', validate='many_to_one')
    df = pd.merge(df, train_ad_data_3, on=['ad_account_id', 'create_date'], how='left', validate='many_to_one')
    df = pd.merge(df, train_ad_data_5, on=['ad_account_id', 'create_date'], how='left', validate='many_to_one')
    df = pd.merge(df, train_ad_data_7, on=['ad_account_id', 'create_date'], how='left', validate='many_to_one')

    cost_col = ['im_create_role_cost_1', 'im_create_role_cost_3',
                'im_create_role_cost_5', 'im_create_role_cost_7',
                'ad_create_role_cost_1', 'ad_create_role_cost_3',
                'ad_create_role_cost_5', 'ad_create_role_cost_7',
                'im_create_role_pay_cost_1', 'im_create_role_pay_cost_3',
                'im_create_role_pay_cost_5', 'im_create_role_pay_cost_7',
                'ad_create_role_pay_cost_1', 'ad_create_role_pay_cost_3',
                'ad_create_role_pay_cost_5', 'ad_create_role_pay_cost_7'
                ]
    for col in cost_col:
        df[col] = df[col].apply(lambda x: x if x > 0 else np.inf)

    df['create_date'] = pd.to_datetime(df['create_date'])
    df['ad_im_sort_id'] = df.groupby(['ad_account_id', 'image_id'])['create_time'].rank()
    df['ad_game_sort_id'] = df.groupby(['ad_account_id', 'game_id'])['create_time'].rank()
    df['im_ad_sort_id'] = df.groupby(['image_id', 'ad_account_id'])['create_time'].rank()

    df = get_mutil_feature(df)

    cat_cols = ['ad_account_id', 'game_id', 'schedule_time', 'delivery_range', 'flow_control_mode',
                'smart_bid_type', 'hide_if_converted', 'gender', 'location_type', 'launch_price', 'retargeting_type',
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
    uri = "model/generationPlanBatchTask"
    url = open_api_url_prefix + uri
    params = {
        "secretkey": "abc2018!@**@888"
    }
    rsp = requests.post(url, json=ad_info, params=params)
    rsp_data = rsp.json()
    print('结束....')
    return rsp_data


def main_model():
    df, plan_create = get_train_df()
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
    threshold = pd.Series(y_predict).sort_values(ascending=False).reset_index(drop=True)[int(y_predict.shape[0] * 0.3)]

    plan_result = plan_create[plan_create['prob'] >= threshold]
    plan_result['rank_ad_im'] = plan_result.groupby(['ad_account_id', 'image_id'])['prob'].rank(ascending=False, method='first')
    plan_result = plan_result[plan_result['rank_ad_im'] <= 1]

    plan_create['rank_ad_im'] = plan_create.groupby(['ad_account_id', 'image_id'])['prob'].rank(ascending=False, method='first')
    plan_result_pr = plan_create[plan_create['rank_ad_im'] <= 1]

    ad_num = plan_result['ad_account_id'].value_counts()
    for ad in np.setdiff1d(plan_create['ad_account_id'].values, ad_num[ad_num > 2].index):
        add_plan = plan_result_pr[plan_result_pr['ad_account_id'] == ad].sort_values('prob', ascending=False)[0:2]
        plan_result = plan_result.append(add_plan)

    if plan_result.shape[0] > 12:
        plan_result = plan_result.sample(12)

    plan_result = plan_result.drop(['create_time', 'create_date', 'prob', 'rank_ad_im', 'label_ids'], axis=1)
    plan_result['convertIndex'] = plan_result['deep_bid_type'].apply(lambda x: 13 if x == 'BID_PER_ACTION' else 14)

    plan_result['budget'] = plan_result.apply(lambda x: x.budget if x.budget >= x.cpa_bid else x.cpa_bid, axis=1)
    plan_result['budget'] = plan_result['budget'].apply(np.ceil)
    plan_result['operation'] = 'disable'
    plan_result['web_url'] = 'https://www.chengzijianzhan.com/tetris/page/6960565870964621319/'

    plan_result.to_csv('./plan_result.csv', index=0)  # 保存创建日志

    # rsp_data = get_ad_create(plan_result)
    # print(rsp_data)


if __name__ == '__main__':
    main_model()