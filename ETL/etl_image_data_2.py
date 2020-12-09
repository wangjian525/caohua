import pymysql
import time
import pandas as pd


# 获取素材报表数据
def getPlanData(conn, begin, end):
    originSql = '''
                SELECT
                a.image_id AS 'image_id',
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
                                                b.image_id,
                                                sum( a.amount ) AS 'image_run_date_amount',
                                                sum( pay_role_user_num ) AS 'image_create_role_pay_num',
                                                sum( create_role_num ) AS 'image_create_role_num',
                                                sum( new_role_money ) AS 'image_create_role_pay_sum',
                                                count( DISTINCT b.plan_id ) AS 'image_source_num' 
                                            FROM
                                                db_stdata.st_lauch_report a
                                                INNER JOIN db_data_ptom.ptom_plan b ON a.game_id = b.game_id 
                                                AND a.source_id = b.source_id 
                                                AND a.channel_id = b.chl_user_id 
                                            WHERE
                                                a.tdate_type = 'day' 
                                                AND a.tdate >= '{begin}' 
                                                AND a.tdate <= '{end}' 
                                                AND a.amount > 100                          
                                                AND b.image_id IS NOT NULL 
                                                AND b.image_id <> '' 
                                                AND a.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1056 AND dev_game_id IS NOT NULL ) 
                                            GROUP BY
                                                b.image_id 
                                            ) a
                                            LEFT JOIN (
                                            SELECT
                                                c.image_id,
                                                sum( c.image_valid_source_num ) AS 'image_valid_source_num' 
                                            FROM
                                                (
                                                SELECT
                                                    b.image_id,
                                                    count( DISTINCT b.plan_id ) AS 'image_valid_source_num' 
                                                FROM
                                                    db_stdata.st_lauch_report a
                                                    INNER JOIN db_data_ptom.ptom_plan b ON a.game_id = b.game_id 
                                                    AND a.source_id = b.source_id 
                                                    AND a.channel_id = b.chl_user_id 
                                                WHERE
                                                    a.tdate_type = 'day' 
                                                    AND a.tdate >= '{begin}' 
                                                    AND a.tdate <= '{end}' 
                                                    AND a.amount > 100                                                    
                                                    AND b.image_id IS NOT NULL 
                                                    AND b.image_id <> '' 
                                                    AND a.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1056 AND dev_game_id IS NOT NULL ) 
                                                    AND a.platform = 1 
                                                GROUP BY
                                                    b.image_id 
                                                HAVING
                                                    sum( a.amount ) / sum( a.pay_role_user_num )< 3500 
                                                    AND sum( a.pay_role_user_num )> 0 UNION ALL
                                                SELECT
                                                    b.image_id,
                                                    count( DISTINCT b.plan_id ) AS 'image_valid_source_num' 
                                                FROM
                                                    db_stdata.st_lauch_report a
                                                    INNER JOIN db_data_ptom.ptom_plan b ON a.game_id = b.game_id 
                                                    AND a.source_id = b.source_id 
                                                    AND a.channel_id = b.chl_user_id 
                                                WHERE
                                                    a.tdate_type = 'day' 
                                                    AND a.tdate >= '{begin}' 
                                                    AND a.tdate <= '{end}' 
                                                    AND a.amount > 100                                                 
                                                    AND b.image_id IS NOT NULL 
                                                    AND b.image_id <> '' 
                                                    AND a.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1056 AND dev_game_id IS NOT NULL ) 
                                                    AND a.platform = 2 
                                                GROUP BY
                                                    b.image_id 
                                                HAVING
                                                    SUM( a.amount ) / SUM( a.pay_role_user_num ) < 5000 AND SUM( a.pay_role_user_num ) > 0 
                                                ) c 
                                            GROUP BY
                                            c.image_id 
                ) b ON a.image_id = b.image_id
    '''
    finalSql = originSql.format(begin=begin, end=end)
    result = pd.read_sql(finalSql, conn)
    return result

# 获取30日支付金额
def getPaySum(conn, begin, end):
    originSql = '''
        SELECT
            p.image_id AS 'image_id',
            sum( a.create_role_pay_sum ) AS 'create_role_30_pay_sum'
        FROM
            (
            SELECT
                m.game_id,
                m.channel_id,
                m.source_id,
                IFNULL( sum( m.create_role_money_sum ), 0 ) AS create_role_pay_sum 
            FROM
                db_stdata.st_game_retain m 
            WHERE
                m.tdate >= '%s'
                AND m.tdate <= '%s' 
                AND m.tdate_type = 'day' 
                AND m.query_type = 19 
                AND m.server_id =- 1 
                AND m.retain_date = 30 
            GROUP BY
                m.game_id,
                m.channel_id,
                m.source_id,
                m.retain_date 
            ) a
            INNER JOIN db_data_ptom.ptom_plan p ON p.game_id = a.game_id 
            AND p.source_id = a.source_id 
            AND p.chl_user_id = a.channel_id 
        WHERE
            image_id IS NOT NULL 
        GROUP BY
            image_id
    '''
    finalSql = originSql % (begin, end)
    result = pd.read_sql(finalSql, conn)
    return result

# 获取次留率数据
def getCreateRoleRetain(conn, begin, end):
    originSql = '''
            SELECT
            p.image_id AS 'image_id',
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
                    sum( m.create_role_num ) AS 'create_role_num',
                    sum( m.create_role_retain_num ) AS 'create_role_retain_num' 
                FROM
                    db_stdata.st_game_retain m 
                WHERE
                    m.tdate >= '%s' 
                    AND m.tdate <= '%s' 
                    AND m.tdate_type = 'day' 
                    AND m.query_type = 19 
                    AND m.server_id =- 1 
                    AND m.retain_date = 2 
                GROUP BY
                    m.game_id,
                    m.channel_id,
                    m.source_id,
                    m.retain_date 
                ) a
                INNER JOIN db_data_ptom.ptom_plan p ON p.game_id = a.game_id 
                AND p.source_id = a.source_id 
                AND p.chl_user_id = a.channel_id 
            WHERE
                p.image_id IS NOT NULL 
        GROUP BY
            p.image_id
    '''
    finalSql = originSql % (begin, end)
    result = pd.read_sql(finalSql, conn)
    return result


def etl_image():
    # 链接数据库，并创建游标
    conn1 = pymysql.connect(host='192.168.0.79', port=3306, user='readonly',
                            passwd='Kc&r+z1ql9x8za4kzfk761weq8ozGv3ZpI;RMe,#+s%c>t', db='db_data')
    # cur1 = conn1.cursor(cursor=pymysql.cursors.DictCursor)

    start = time.time()
    columns = ['image_id', 'image_run_date_amount', 'image_create_role_pay_num',
       'image_create_role_num', 'image_create_role_pay_sum',
       'image_source_num', 'image_create_role_pay_rate',
       'image_create_role_cost', 'image_create_role_pay_cost',
       'image_valid_source_num', 'image_valid_source_rate',
       'image_pay_sum_ability', 'image_pay_num_ability',
       'image_create_role_roi', 'image_create_role_retain_1d',
       'create_role_30_pay_sum', 'model_run_datetime']
    result_df = pd.DataFrame(columns=columns)
    date_list = pd.date_range(start='2020-10-02', end='2020-11-04')
    for date in date_list:
        end = date
        begin = date - pd.Timedelta(days=3)
        end = str(end).split(' ')[0]
        begin = str(begin).split(' ')[0]
        # 获取计划数据
        planDataList = getPlanData(conn1, begin, end)
        # 获取30日回款金额
        paySumList = getPaySum(conn1, begin, end)
        # 获取次留数据
        roleRetainList = getCreateRoleRetain(conn1, begin, end)
        # 合并数据
        temp = pd.merge(planDataList, roleRetainList, on='image_id', how='inner')
        temp = pd.merge(temp, paySumList, on='image_id', how='inner')
        temp['model_run_datetime'] = date

        # 将数据添加到结果result_list
        result_df = result_df.append(temp)
    # # print(result_df.shape)
    result_df['data_win'] = 3
    result_df.to_excel('./result_df.xls', index=0)
    # cur1.close()
    conn1.close()
    end = time.time()
    print('程序共计耗时：%.2f 秒' % (end - start))


# 运行程序
if __name__ == '__main__':
    etl_image()
