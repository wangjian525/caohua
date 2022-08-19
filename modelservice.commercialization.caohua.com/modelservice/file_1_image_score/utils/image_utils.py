# -*- coding:utf-8 -*-
"""
   File Name：     image_utils.py
   Description :   素材评分 - 数据采集
   Author :        royce.mao
   date：          2022/7/12 11:53
"""

import pymysql
import datetime
import pandas as pd
import multiprocessing
from multiprocessing import cpu_count

import modelservice.serv_conf as serv_conf
nacosServ = serv_conf.get_var()  # !!! 增加线上服务参数nacos配置


def getPlanData(conn, mgame_id, begin, end, mid):
    originSql = '''
        /*手动查询*/
            SELECT
                a.image_id AS 'image_id',
                a.image_name AS 'image_name',
                a.media_id,
                a.label_ids,
                c.launch_time AS 'image_launch_time',
                c.image_source_total_num AS 'image_source_total_num',
                ifnull( a.image_run_date_amount, 0 ) AS 'image_run_date_amount',
                ifnull( a.image_create_role_pay_num, 0 ) AS 'image_create_role_pay_num',
                ifnull( a.image_create_role_num, 0 ) AS 'image_create_role_num',
                ifnull( a.image_create_role_pay_sum, 0 ) AS 'image_create_role_pay_sum',
                ifnull( a.image_source_num, 0 ) AS 'image_source_num',
                (
                CASE
                        
                        WHEN ifnull( a.image_create_role_num, 0 )= 0 THEN
                        0 ELSE IFNULL( a.image_create_role_pay_num, 0 ) / ifnull( a.image_create_role_num, 0 ) 
                    END 
                    ) AS 'image_create_role_pay_rate',
                    (
                    CASE
                            
                            WHEN ifnull( a.image_create_role_num, 0 )= 0 THEN
                            0 ELSE IFNULL( a.image_run_date_amount, 0 ) / ifnull( a.image_create_role_num, 0 ) 
                        END 
                        ) AS 'image_create_role_cost',
                        (
                        CASE
                                
                                WHEN ifnull( a.image_create_role_pay_num, 0 )= 0 THEN
                                0 ELSE IFNULL( a.image_run_date_amount, 0 ) / ifnull( a.image_create_role_pay_num, 0 ) 
                            END 
                            ) AS 'image_create_role_pay_cost',
                            ifnull( b.image_valid_source_num, 0 ) AS 'image_valid_source_num',
                            (
                            CASE
                                    
                                    WHEN ifnull( a.image_source_num, 0 )= 0 THEN
                                    0 ELSE IFNULL( b.image_valid_source_num, 0 ) / ifnull( a.image_source_num, 0 ) 
                                END 
                                ) AS 'image_valid_source_rate',
                                (
                                CASE
                                        
                                        WHEN ifnull( b.image_valid_source_num, 0 )= 0 THEN
                                        0 ELSE IFNULL( a.image_create_role_pay_sum, 0 ) / ifnull( b.image_valid_source_num, 0 ) 
                                    END 
                                    ) AS 'image_pay_sum_ability',
                                    (
                                    CASE
                                            
                                            WHEN ifnull( b.image_valid_source_num, 0 )= 0 THEN
                                            0 ELSE IFNULL( a.image_create_role_pay_num, 0 ) / ifnull( b.image_valid_source_num, 0 ) 
                                        END 
                                        ) AS 'image_pay_num_ability',
                                        (
                                        CASE
                                                
                                                WHEN ifnull( a.image_run_date_amount, 0 )= 0 THEN
                                                0 ELSE IFNULL( a.image_create_role_pay_sum, 0 ) / ifnull( a.image_run_date_amount, 0 ) 
                                            END 
                                            ) AS 'image_create_role_roi' 
                                        FROM
                                            (
                                            SELECT
                                                aa.image_id AS 'image_id',
                                                aa.image_name AS 'image_name',
                                                aa.media_id AS 'media_id',
                                                aa.label_ids AS 'label_ids',
                                                sum( aa.amount ) AS 'image_run_date_amount',
                                                IFNULL( sum( aa.create_role_num ), 0 ) AS 'image_create_role_num',
                                                IFNULL( sum( bb.pay_role_user_num ), 0 ) AS 'image_create_role_pay_num',
                                                IFNULL( sum( bb.new_role_money ), 0 ) AS 'image_create_role_pay_sum',
                                                IFNULL( sum( aa.image_source_num ), 0 ) AS 'image_source_num' 
                                            FROM
                                                (
                                                SELECT
                                                    a.game_id,
                                                    a.channel_id,
                                                    a.source_id,
                                                    a.media_id,
                                                    c.image_id,
                                                    c.image_name,
                                                    c.label_ids,
                                                    IFNULL( sum( a.amount ), 0 ) AS amount,
                                                    IFNULL( sum( create_role_num ), 0 ) AS create_role_num,
                                                    count( DISTINCT b.plan_id ) AS 'image_source_num' 
                                                FROM
                                                    db_stdata.st_lauch_report a
                                                    INNER JOIN db_data_ptom.ptom_plan b ON a.game_id = b.game_id 
                                                    AND a.source_id = b.source_id 
                                                    AND a.channel_id = b.chl_user_id
                                                    LEFT JOIN db_data_ptom.ptom_image_info c ON b.image_id = c.image_id
                                                WHERE
                                                    a.tdate_type = 'day' 
                                                    AND a.tdate <= '{end}' AND a.tdate >= '{begin}' 
                                                    AND a.amount > 0 
                                                    AND a.media_id IN ( 10, 16, 32, 45 ) 
                                                    AND b.image_id IS NOT NULL 
                                                    AND b.image_id <> '' 
                                                    AND a.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = {mgame} AND dev_game_id IS NOT NULL ) 
                                                GROUP BY
                                                    a.game_id,
                                                    a.channel_id,
                                                    a.source_id
                                                HAVING
                                                    c.image_id IS NOT NULL 
                                                    AND c.image_name <> '' 
                                                ) aa
                                                LEFT JOIN (
                                                SELECT
                                                    game_id,
                                                    channel_id,
                                                    source_id,
                                                    IFNULL( sum( m.new_role_money ), 0 ) AS new_role_money,
                                                    IFNULL( sum( m.pay_role_user_num ), 0 ) AS pay_role_user_num 
                                                FROM
                                                    (
                                                    SELECT
                                                        a.game_id,
                                                        a.channel_id,
                                                        a.source_id,
                                                        IFNULL( sum( a.new_role_money ), 0 ) AS new_role_money,
                                                        IFNULL( sum( a.pay_role_user_num ), 0 ) AS pay_role_user_num 
                                                    FROM
                                                        db_stdata.st_lauch_report a
                                                        INNER JOIN db_data_ptom.ptom_plan pp ON ( a.game_id = pp.game_id AND a.channel_id = pp.chl_user_id AND a.source_id = pp.source_id ) 
                                                    WHERE
                                                        a.tdate = '{end}' 
                                                        AND a.tdate_type = 'day' 
                                                    GROUP BY
                                                        a.game_id,
                                                        a.channel_id,
                                                        a.source_id 
                                                    HAVING
                                                        ( new_role_money > 0 OR pay_role_user_num > 0 ) UNION ALL
                                                    SELECT
                                                        c.game_id,
                                                        c.channel_id,
                                                        c.source_id,
                                                        sum( c.create_role_money ) new_role_money,
                                                        IFNULL( sum( c.pay_role_user_num ), 0 ) AS pay_role_user_num 
                                                    FROM
                                                        db_stdata.st_game_days c 
                                                    WHERE
                                                        c.report_days = 3 
                                                        AND c.tdate = '{mid}' 
                                                        AND c.tdate_type = 'day' 
                                                        AND c.query_type = 13 
                                                    GROUP BY
                                                        c.game_id,
                                                        c.channel_id,
                                                        c.source_id 
                                                    HAVING
                                                    ( new_role_money > 0 OR pay_role_user_num > 0 )) m 
                                                GROUP BY
                                                    game_id,
                                                    channel_id,
                                                    source_id 
                                                ) bb ON aa.game_id = bb.game_id 
                                                AND aa.channel_id = bb.channel_id 
                                                AND aa.source_id = bb.source_id 
                                            GROUP BY
                                                aa.image_id,
                                                aa.image_name,
                                                aa.media_id 
                                            HAVING
                                                image_run_date_amount >= 100 
                                            ) a
                                            LEFT JOIN (
                                            SELECT
                                                c.image_id,
                                                c.media_id,
                                                sum( c.image_valid_source_num ) AS 'image_valid_source_num' 
                                            FROM
                                                (
                                                SELECT
                                                    b.image_id,
                                                    a.media_id,
                                                    count( DISTINCT b.plan_id ) AS 'image_valid_source_num' 
                                                FROM
                                                    db_stdata.st_lauch_report a
                                                    INNER JOIN db_data_ptom.ptom_plan b ON a.game_id = b.game_id 
                                                    AND a.source_id = b.source_id 
                                                    AND a.channel_id = b.chl_user_id 
                                                WHERE
                                                    a.tdate_type = 'day' 
                                                    AND a.tdate >= '{begin}' 
                                                    AND a.tdate <= '{end}' AND a.amount > 100 
                                                    AND a.media_id IN ( 10, 16, 32, 45 ) 
                                                    AND b.image_id IS NOT NULL 
                                                    AND b.image_id <> '' 
                                                    AND a.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = {mgame} AND dev_game_id IS NOT NULL ) 
                                                    AND a.platform = 1 
                                                GROUP BY
                                                    b.image_id,
                                                    a.media_id 
                                                HAVING
                                                    sum( a.amount ) / sum( a.pay_role_user_num )< 5000 
                                                    AND sum( a.pay_role_user_num )> 0 UNION ALL
                                                SELECT
                                                    b.image_id,
                                                    a.media_id,
                                                    count( DISTINCT b.plan_id ) AS 'image_valid_source_num' 
                                                FROM
                                                    db_stdata.st_lauch_report a
                                                    INNER JOIN db_data_ptom.ptom_plan b ON a.game_id = b.game_id 
                                                    AND a.source_id = b.source_id 
                                                    AND a.channel_id = b.chl_user_id 
                                                WHERE
                                                    a.tdate_type = 'day' 
                                                    AND a.tdate >= '{begin}' 
                                                    AND a.tdate <= '{end}' AND a.amount > 100 
                                                    AND a.media_id IN ( 10, 16, 32, 45 ) 
                                                    AND b.image_id IS NOT NULL 
                                                    AND b.image_id <> '' 
                                                    AND a.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = {mgame} AND dev_game_id IS NOT NULL ) 
                                                    AND a.platform = 2 
                                                GROUP BY
                                                    b.image_id,
                                                    a.media_id 
                                                HAVING
                                                    SUM( a.amount ) / SUM( a.pay_role_user_num ) < 5000 AND SUM( a.pay_role_user_num ) > 0 
                                                ) c 
                                            GROUP BY
                                                c.image_id,
                                                c.media_id 
                                            ) b ON a.image_id = b.image_id 
                                            AND a.media_id = b.media_id
                                            LEFT JOIN (
                                            SELECT
                                                b.image_id,
                                                min( b.launch_time ) AS launch_time,
                                                count( DISTINCT b.plan_id ) AS image_source_total_num 
                                            FROM
                                                db_stdata.st_lauch_report a
                                                INNER JOIN db_data_ptom.ptom_plan b ON a.game_id = b.game_id 
                                                AND a.source_id = b.source_id 
                                                AND a.channel_id = b.chl_user_id 
                                            WHERE
                                                a.tdate_type = 'day' 
                                                AND a.amount > 100 
                                                AND b.image_id IS NOT NULL 
                                                AND b.image_id <> '' 
                                                AND a.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = {mgame} AND dev_game_id IS NOT NULL ) 
                                            GROUP BY
                                            b.image_id 
                ) c ON a.image_id = c.image_id
    '''
    finalSql = originSql.format(mgame=mgame_id, begin=begin, end=end, mid=mid)
    result = pd.read_sql(finalSql, conn)
    return result


def getPaySum(conn, begin, end):
    originSql = '''
             /*手动查询*/
            SELECT
                p.image_id AS 'image_id',
                p.media_id as 'media_id',
                sum( a.create_role_pay_sum ) AS 'create_role_30_pay_sum' 
            FROM
                (
                SELECT
                    m.game_id,
                    m.channel_id,
                    m.source_id,
                    m.media_id,
                    IFNULL( sum( m.create_role_money_sum ), 0 ) AS create_role_pay_sum 
                FROM
                    db_stdata.st_game_retain m 
                WHERE
                    m.tdate >= '%s' 
                    AND m.tdate <= '%s' 
                    AND m.tdate_type = 'day' 
                    AND m.media_id IN (10,16,32,45) 
                    AND m.query_type = 19 
                    AND m.server_id =- 1 
                    AND m.retain_date = 30 
                GROUP BY
                    m.game_id,
                    m.channel_id,
                    m.source_id,
                    m.media_id,
                    m.retain_date 
                ) a
                INNER JOIN db_data_ptom.ptom_plan p ON p.game_id = a.game_id 
                AND p.source_id = a.source_id 
                AND p.chl_user_id = a.channel_id 
            WHERE
                p.image_id IS NOT NULL 
            GROUP BY
                p.image_id,
                p.media_id
    '''
    finalSql = originSql % (begin, end)
    result = pd.read_sql(finalSql, conn)
    return result


def getCreateRoleRetain(conn, begin, end):
    originSql = '''
             /*手动查询*/
            SELECT
            p.image_id AS 'image_id',
            p.media_id AS 'media_id',
            (
            CASE

                    WHEN ifnull( sum(a.create_role_num), 0 )= 0 THEN
                    0 ELSE ifnull( sum( a.create_role_retain_num ), 0 ) / ifnull( sum(a.create_role_num), 0 ) 
                END 
                ) AS 'image_create_role_retain_1d'
            FROM
                (
                SELECT
                    m.game_id,
                    m.channel_id,
                    m.source_id,
                    m.media_id,
                    sum( m.create_role_num ) AS 'create_role_num',
                    sum( m.create_role_retain_num ) AS 'create_role_retain_num' 
                FROM
                    db_stdata.st_game_retain m 
                WHERE
                    m.tdate >= '%s' 
                    AND m.tdate <= '%s' 
                    AND m.tdate_type = 'day'
                    AND m.media_id in (10,16,32,45) 
                    AND m.query_type = 19 
                    AND m.server_id =- 1 
                    AND m.retain_date = 2
                GROUP BY
                    m.game_id,
                    m.channel_id,
                    m.source_id,
                    m.media_id,
                    m.retain_date
                ) a
                INNER JOIN db_data_ptom.ptom_plan p ON p.game_id = a.game_id 
                AND p.source_id = a.source_id 
                AND p.chl_user_id = a.channel_id 
            WHERE
                p.image_id IS NOT NULL 
        GROUP BY
            p.image_id,
            p.media_id
    '''
    finalSql = originSql % (begin, end)
    result = pd.read_sql(finalSql, conn)
    return result


def merge(date, mgame):
        conn = pymysql.connect(host=nacosServ['DB_SLAVE_FENXI_HOST'], port=int(nacosServ['DB_SLAVE_FENXI_PORT']), user=nacosServ['DB_SLAVE_FENXI_USERNAME'],
                               passwd=nacosServ['DB_SLAVE_FENXI_PASSWORD'], db=nacosServ['DB_SLAVE_FENXI_DATABASE'])
        end = date.strftime('%Y-%m-%d')
        begin = (datetime.datetime.strptime(end, '%Y-%m-%d') - pd.Timedelta(days=3)).strftime('%Y-%m-%d')
        mid = (datetime.datetime.strptime(end, '%Y-%m-%d') - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
        planDataList = getPlanData(conn, mgame, begin, end, mid)
        roleRetainList = getCreateRoleRetain(conn, begin, end)
        PaySumList = getPaySum(conn, begin, end)

        df = pd.merge(planDataList, roleRetainList, on=['image_id', 'media_id'], how='left')
        df = pd.merge(df, PaySumList, on=['image_id', 'media_id'], how='left')
        df['model_run_datetime'] = date
        conn.close()
        return df


def etl_image_train(start, end, mgame):
    """ 训练：数据采集 """
    columns = ['image_id', 'image_name', 'media_id', 'image_launch_time',
            'image_source_total_num', 'image_run_date_amount',
            'image_create_role_pay_num', 'image_create_role_num',
            'image_create_role_pay_sum', 'image_source_num',
            'image_create_role_pay_rate', 'image_create_role_cost',
            'image_create_role_pay_cost', 'image_valid_source_num',
            'image_valid_source_rate', 'image_pay_sum_ability',
            'image_pay_num_ability', 'image_create_role_roi',
            'image_create_role_retain_1d', 'create_role_30_pay_sum', 'model_run_datetime']
   
    date_list = pd.date_range(start=start, end=end)
    
    p = multiprocessing.Pool(cpu_count()-1) 
    results = [p.apply_async(func=merge, args=(date, mgame)) for date in date_list]
    
    p.close()
    p.join()

    result_df = pd.DataFrame(columns=columns)
    for res in results:
        res = res.get()
        result_df = result_df.append(res) if len(res) > 0 else result_df
    
    result_df['data_win'] = 3

    return result_df


def etl_image_pred(mgame_id):
    """ 预测：数据采集 """
    # 链接数据库，并创建游标
    conn = pymysql.connect(host=nacosServ['DB_SLAVE_FENXI_HOST'], port=int(nacosServ['DB_SLAVE_FENXI_PORT']), user=nacosServ['DB_SLAVE_FENXI_USERNAME'],
                           passwd=nacosServ['DB_SLAVE_FENXI_PASSWORD'], db=nacosServ['DB_SLAVE_FENXI_DATABASE'])

    end = datetime.datetime.now().strftime('%Y-%m-%d')  # '2022-03-12'
    begin = (datetime.datetime.strptime(end, '%Y-%m-%d') - pd.Timedelta(days=3)).strftime('%Y-%m-%d')
    mid = (datetime.datetime.strptime(end, '%Y-%m-%d') - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
    # 获取计划数据
    planDataList = getPlanData(conn, mgame_id, begin, end, mid)
    # 获取次留数据
    roleRetainList = getCreateRoleRetain(conn, begin, end)
    # 合并数据
    result_df = pd.merge(planDataList, roleRetainList, on=['image_id', 'media_id'], how='left')
    result_df['model_run_datetime'] = end
    result_df['data_win'] = 3
    conn.close()

    return result_df


def change_woe(d, cut, woe):
    """
    将每个样本对应特征值更换为woe值
    """
    list1 = []
    i = 0
    while i < len(d):
        value = d.values[i]
        j = len(cut) - 2
        m = len(cut) - 2
        while j >= 0:
            if value >= cut[j]:
                j = -1
            else:
                j -= 1
                m -= 1
        list1.append(woe[m])
        i += 1
    return list1


if __name__ == '__main__':
    # 采集
    df_train = etl_image_train(start='2022-06-12', end='2022-07-12', mgame=1056)
    print("[INFO]:数据采集完毕！")
