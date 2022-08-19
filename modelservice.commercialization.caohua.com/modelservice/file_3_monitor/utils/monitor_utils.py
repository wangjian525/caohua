# -*- coding:utf-8 -*-

import pandas as pd
import pymysql
from impala.dbapi import connect
from impala.util import as_pandas

import modelservice.serv_conf as serv_conf
nacosServ = serv_conf.get_var()  # !!! 增加线上服务参数nacos配置


def get_an_cost(media_id, mgame_id, conf):
    conn = pymysql.connect(host=nacosServ['DB_SLAVE_FENXI_HOST'], port=int(nacosServ['DB_SLAVE_FENXI_PORT']), user=nacosServ['DB_SLAVE_FENXI_USERNAME'],
                           passwd=nacosServ['DB_SLAVE_FENXI_PASSWORD'])
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
                SELECT
                    sum( amount ) / sum( create_role_num ) AS create_cost,
                    sum( amount ) / sum( pay_role_user_num ) AS pay_cost 
                FROM
                    db_stdata.st_lauch_report 
                WHERE
                    tdate_type = 'day' 
                    AND tdate >= DATE_SUB( date( NOW()), INTERVAL 15 DAY ) 
                    AND media_id = {} 
                    AND platform = 1 
                    AND game_id IN (
                    SELECT
                        dev_game_id AS game_id 
                    FROM
                        db_data.t_game_config 
                    WHERE
                        game_id = {} 
                    AND dev_game_id IS NOT NULL 
                    )
        '''
    f_sql = sql.format(media_id, mgame_id)
    cur.execute(f_sql)
    result_df = pd.read_sql(f_sql, conn)
    cur.close()
    conn.close()
    try:
        create_cost = int(result_df['create_cost'].values)
        pay_cost = int(result_df['pay_cost'].values)
        1 / create_cost
        1 / pay_cost
    except:
        create_cost = conf['create_cost_an']
        pay_cost = conf['pay_cost_an']
    return create_cost, pay_cost


def get_ios_cost(media_id, mgame_id, conf):
    conn = pymysql.connect(host=nacosServ['DB_SLAVE_FENXI_HOST'], port=int(nacosServ['DB_SLAVE_FENXI_PORT']), user=nacosServ['DB_SLAVE_FENXI_USERNAME'],
                           passwd=nacosServ['DB_SLAVE_FENXI_PASSWORD'])
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
                SELECT
                    sum( amount ) / sum( create_role_num ) AS create_cost, 
                    sum( amount ) / sum( pay_role_user_num ) AS pay_cost
                FROM
                    db_stdata.st_lauch_report 
                WHERE
                    tdate_type = 'day' 
                    AND tdate >= DATE_SUB( date( NOW()), INTERVAL 15 DAY ) 
                    AND media_id = {} 
                    AND platform = 2 
                    AND game_id IN (
                    SELECT
                        dev_game_id AS game_id 
                    FROM
                        db_data.t_game_config 
                    WHERE
                        game_id = {} 
                    AND dev_game_id IS NOT NULL 
                    )
        '''
    f_sql = sql.format(media_id, mgame_id)
    cur.execute(f_sql)
    result_df = pd.read_sql(f_sql, conn)
    cur.close()
    conn.close()
    try:
        create_cost = int(result_df['create_cost'].values)
        pay_cost = int(result_df['pay_cost'].values)
        1 / create_cost
        1 / pay_cost
    except:
        create_cost = conf['create_cost_ios']
        pay_cost = conf['pay_cost_ios']
    return create_cost, pay_cost


def get_source_data(mgame_id, media_id):
    conn = connect(host=nacosServ['HIVE_HOST'], port=int(nacosServ['HIVE_PORT']), auth_mechanism=nacosServ['HIVE_AUTH_MECHANISM'], 
                   user=nacosServ['HIVE_USERNAME'],password=nacosServ['HIVE_PASSWORD'], database=nacosServ['HIVE_DATABASE'])
    cursor = conn.cursor()
    sql = '''
            SELECT
                channel_id,
                source_id,
                7_amount as amount_7,
                7_roi as roi_7,
                data_win 
            FROM
                tmp_data.tmp_ra_media_source 
            WHERE
                dt = `current_date` () 
                AND mgame_id = {} 
                AND media_id = {}
        '''
    f_sql = sql.format(mgame_id, media_id)
    cursor.execute(f_sql)
    result = as_pandas(cursor)

    # 关闭链接
    cursor.close()
    conn.close()
    return result
