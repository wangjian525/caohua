import pymysql
import time
import pandas as pd


# 获取素材报表数据
def getPlanData(cur, begin, end):
    originSql = '''
                SELECT
                a.image_id AS 'image_id',
                ifnull( a.image_run_date_amount, 0 ) AS 'image_run_date_amount',
                ifnull( a.image_create_role_pay_num, 0 ) AS 'image_create_role_pay_num',
                ifnull( a.image_create_role_num, 0 ) AS 'image_create_role_num',
                ifnull( a.image_create_role_pay_sum, 0 ) AS 'image_create_role_pay_sum',
                ifnull( a.image_source_num, 0 ) AS 'image_source_num',
                (CASE 
                WHEN ifnull(a.image_create_role_num,0)=0 THEN
                    0
                ELSE
                    IFNULL(a.image_create_role_pay_num,0) / ifnull(a.image_create_role_num ,0)
                END) as 'image_create_role_pay_rate',
                (CASE 
                WHEN ifnull(a.image_create_role_num,0)=0 THEN
                    0
                ELSE
                    IFNULL(a.image_run_date_amount,0) / ifnull(a.image_create_role_num ,0)
                END) as 'image_create_role_cost',
                (CASE 
                WHEN ifnull(a.image_create_role_pay_num,0)=0 THEN
                    0
                ELSE
                    IFNULL(a.image_run_date_amount,0) / ifnull(a.image_create_role_pay_num ,0)
                END) as 'image_create_role_pay_cost',
                ifnull( b.image_valid_source_num, 0 ) AS 'image_valid_source_num',
                (CASE 
                WHEN ifnull(a.image_source_num,0)=0 THEN
                    0
                ELSE
                    IFNULL(b.image_valid_source_num,0) / ifnull(a.image_source_num ,0)
                END) as 'image_valid_source_rate',
                (CASE 
                WHEN ifnull(b.image_valid_source_num,0)=0 THEN
                    0
                ELSE
                    IFNULL(a.image_create_role_pay_sum,0) / ifnull(b.image_valid_source_num ,0)
                END) as 'image_pay_sum_ability',
                (CASE 
                WHEN ifnull(b.image_valid_source_num,0)=0 THEN
                    0
                ELSE
                    IFNULL(a.image_create_role_pay_num,0) / ifnull(b.image_valid_source_num ,0)
                END) as 'image_pay_num_ability',
                    (CASE 
                WHEN ifnull(a.image_run_date_amount,0)=0 THEN
                    0
                ELSE
                    IFNULL(a.image_create_role_pay_sum,0) / ifnull(a.image_run_date_amount ,0)
                END) as 'image_create_role_roi'
            FROM
                (
                SELECT
                    b.image_id,
                    sum( a.amount ) AS 'image_run_date_amount',
                    sum( pay_role_user_num ) AS 'image_create_role_pay_num',
                    sum( create_role_num ) AS 'image_create_role_num',
                    sum( new_role_money ) AS 'image_create_role_pay_sum',
                    count(DISTINCT b.plan_id ) AS 'image_source_num' 
                FROM
                    db_stdata.st_lauch_report a
                    INNER JOIN db_data_ptom.ptom_plan b ON a.game_id = b.game_id 
                    AND a.source_id = b.source_id 
                    AND a.channel_id = b.chl_user_id 
                WHERE
                    a.tdate_type = 'day' 
                    AND a.tdate >= '%s' 
                    AND a.tdate <= '%s'
                    AND a.amount > 100 
                    AND a.media_id = 10 
                    AND b.image_id IS NOT NULL 
                    AND b.image_id <> '' 
                    AND a.game_id IN (
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
                        1001413 
                    ) 
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
                        AND a.tdate >= '%s' 
                        AND a.tdate <= '%s'
                        AND a.amount > 100 
                        AND a.media_id = 10 
                        AND b.image_id IS NOT NULL 
                        AND b.image_id <> '' 
                        AND a.game_id IN (
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
                            1001413 
                        ) 
                        AND a.platform=1
                    GROUP BY
                        b.image_id 
                    HAVING
                        sum( a.amount ) / sum( a.pay_role_user_num )< 3500 
                        AND sum( a.pay_role_user_num )> 0 
                    UNION ALL
                    SELECT
                        b.image_id,
                        count(DISTINCT b.plan_id ) AS 'image_valid_source_num' 
                    FROM
                        db_stdata.st_lauch_report a
                        INNER JOIN db_data_ptom.ptom_plan b ON a.game_id = b.game_id 
                        AND a.source_id = b.source_id 
                        AND a.channel_id = b.chl_user_id 
                    WHERE
                        a.tdate_type = 'day' 
                        AND a.tdate >= '%s' 
                        AND a.tdate <= '%s'
                        AND a.amount > 100 
                        AND a.media_id = 10 
                        AND b.image_id IS NOT NULL 
                        AND b.image_id <> '' 
                        AND a.game_id IN (
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
                            1001413 
                        ) 
                        AND a.platform=2
                    GROUP BY
                        b.image_id 
                    HAVING
                        sum( a.amount )/ sum( a.pay_role_user_num )< 5000 
                        AND sum( a.pay_role_user_num )> 0 
                    ) c 
                GROUP BY
                c.image_id 
                ) b ON a.image_id = b.image_id
    '''
    finalSql = originSql % (begin, end, begin, end, begin, end)
    cur.execute(finalSql)
    all_line = cur.fetchall()
    columns = ['image_id', 'image_run_date_amount', 'image_create_role_pay_num', 'image_create_role_num',
               'image_create_role_pay_sum', 'image_source_num', 'image_create_role_pay_rate', 'image_create_role_cost',
               'image_create_role_pay_cost', 'image_valid_source_num', 'image_valid_source_rate',
               'image_pay_sum_ability', 'image_pay_num_ability', 'image_create_role_roi']
    result = pd.DataFrame(data=all_line, columns=columns)
    return result


# 获取素材vedio_report报表数据
def getImageReportData(cur, begin, end):
    originSql = '''
            SELECT
            a.image_id AS 'image_id',
            c.create_time AS 'create_time',
            sum( b.total_play ) AS 'total_play',
            sum( b.valid_play ) AS 'valid_play',
            (case 
            when ifnull(b.total_play,0)=0 then 0
            else ifnull(b.valid_play,0) / ifnull(b.total_play,0)
            end) as 'valid_play_rate',
            sum( b.play_over ) AS 'play_over',
            (case 
            when ifnull(b.total_play,0)=0 then 0
            else ifnull(b.play_over,0) / ifnull(b.total_play,0)
            end) as 'play_over_rate',
            avg( b.play_duration_2s_rate ) AS 'play_duration_2s_rate',
            avg( b.play_duration_3s_rate ) AS 'play_duration_3s_rate',
            avg( b.play_duration_5s_rate ) AS 'play_duration_5s_rate',
            avg( b.play_duration_10s_rate ) AS 'play_duration_10s_rate',
            avg( b.play_25_feed_break_rate ) AS 'play_25_feed_break_rate',
            avg( b.play_50_feed_break_rate ) AS 'play_50_feed_break_rate',
            avg( b.play_75_feed_break_rate ) AS 'play_75_feed_break_rate',
            avg( b.play_100_feed_break_rate ) AS 'play_100_feed_break_rate',
            avg( b.average_play_time_per_play ) AS 'average_play_time_per_play',
            sum( b.`share` ) AS 'share',
            sum( b.`comment` ) AS 'comment',
            sum( b.`like` ) AS 'like',
            sum( b.dislike ) AS 'dislike',
            sum( b.follow ) AS 'follow',
            sum( b.message_action ) AS 'message_action',
            sum( b.report ) AS 'report' 
        FROM
            ptom_image_detail_info a
            INNER JOIN ptom_video_report b ON a.id = b.image_detail_id
            INNER JOIN ptom_image_info c ON c.image_id = a.image_id 
        WHERE
            b.media_id = 10 
            AND b.tdate >= '%s' 
            AND b.tdate <= '%s' 
        GROUP BY
            a.image_id   
    '''
    finalSql = originSql % (begin, end)
    cur.execute(finalSql)
    all_line = cur.fetchall()
    columns = ['image_id', 'create_time', 'total_play', 'valid_play', 'valid_play_rate', 'play_over', 'play_over_rate',
               'play_duration_2s_rate', 'play_duration_3s_rate', 'play_duration_5s_rate', 'play_duration_10s_rate',
               'play_25_feed_break_rate', 'play_50_feed_break_rate', 'play_75_feed_break_rate',
               'play_100_feed_break_rate',
               'average_play_time_per_play', 'share', 'comment', 'like', 'dislike', 'follow', 'message_action',
               'report']
    result = pd.DataFrame(data=all_line, columns=columns)
    return result


# 获取30日支付金额
def getPaySum(cur, begin, end):
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
    cur.execute(finalSql)
    all_line = cur.fetchall()
    columns = ['image_id', 'create_role_30_pay_sum']
    result = pd.DataFrame(data=all_line, columns=columns)
    return result


# 获取次留率数据
def getCreateRoleRetain(cur, begin, end):
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
    cur.execute(finalSql)
    all_line = cur.fetchall()
    columns = ['image_id', 'image_create_role_retain_1d']
    result = pd.DataFrame(data=all_line, columns=columns)
    return result


def etl_image():
    # 链接数据库，并创建游标
    conn1 = pymysql.connect(host='192.168.0.79', port=3306, user='readonly',
                            passwd='Kc&r+z1ql9x8za4kzfk761weq8ozGv3ZpI;RMe,#+s%c>t', db='db_data')
    cur1 = conn1.cursor(cursor=pymysql.cursors.DictCursor)
    conn2 = pymysql.connect(host='192.168.0.65', port=3306, user='readonly',
                            passwd='Kc&r+z1ql9x8za4kzfk761weq8ozGv3ZpI;RMe,#+s%c>t', db='db_ptom')
    cur2 = conn2.cursor(cursor=pymysql.cursors.DictCursor)
    start = time.time()
    columns = ['image_id', 'image_run_date_amount', 'image_create_role_pay_num',
               'image_create_role_num', 'image_create_role_pay_sum',
               'image_source_num', 'image_create_role_pay_rate',
               'image_create_role_cost', 'image_create_role_pay_cost',
               'image_valid_source_num', 'image_valid_source_rate',
               'image_pay_sum_ability', 'image_pay_num_ability',
               'image_create_role_roi', 'create_time', 'total_play', 'valid_play',
               'valid_play_rate', 'play_over', 'play_over_rate',
               'play_duration_2s_rate', 'play_duration_3s_rate',
               'play_duration_5s_rate', 'play_duration_10s_rate',
               'play_25_feed_break_rate', 'play_50_feed_break_rate',
               'play_75_feed_break_rate', 'play_100_feed_break_rate',
               'average_play_time_per_play', 'share', 'comment', 'like', 'dislike',
               'follow', 'message_action', 'report', 'image_create_role_retain_1d',
               'create_role_30_pay_sum', 'model_run_datetime']
    result_df = pd.DataFrame(columns=columns)
    date_list = pd.date_range(start='2019-12-01', end='2019-12-31')
    for date in date_list:
        end = date
        begin = date - pd.Timedelta(days=3)
        end = str(end).split(' ')[0]
        begin = str(begin).split(' ')[0]
        # 获取计划数据
        planDataList = getPlanData(cur1, begin, end)
        # 获取30日回款金额
        paySumList = getPaySum(cur1, begin, end)
        # 获取次留数据
        roleRetainList = getCreateRoleRetain(cur1, begin, end)
        # 获取素材报表数据
        imageReportDataList = getImageReportData(cur2, begin, end)
        # 合并数据
        # print(planDataList.head())
        # print(paySumList.head())
        # print(roleRetainList.head())
        # print(imageReportDataList.head())

        temp = pd.merge(planDataList, imageReportDataList, on='image_id', how='inner')
        temp = pd.merge(temp, roleRetainList, on='image_id', how='inner')
        temp = pd.merge(temp, paySumList, on='image_id', how='inner')
        temp['model_run_datetime'] = date
        # print(temp.shape)
        # print(temp.columns)
        # 将数据添加到结果result_list
        result_df = result_df.append(temp)
    # print(result_df.shape)
    result_df['media_id'] = 10
    result_df['data_win'] = 3
    result_df.to_excel('./result_df.xls', index=0)
    cur1.close()
    conn1.close()
    cur2.close()
    conn2.close()
    end = time.time()
    print('程序共计耗时：%.2f 秒' % (end-start))


# 运行程序
if __name__ == '__main__':
    etl_image()
