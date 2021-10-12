import pymysql
import datetime
import pandas as pd
import numpy as np
import joblib


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
                                                AND a.media_id = 10 
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
                                                    AND a.media_id = 10 
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
                                                    AND a.media_id = 10 
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


# 获取素材vedio_report报表数据
def getImageReportData(conn, begin, end):
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
            AND b.tdate >= '{begin}' 
            AND b.tdate <= '{end}' 
        GROUP BY
            a.image_id   
    '''
    finalSql = originSql.format(begin=begin, end=end)
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
                    m.tdate >= '{begin}' 
                    AND m.tdate <= '{end}' 
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
    finalSql = originSql.format(begin=begin, end=end)
    result = pd.read_sql(finalSql, conn)
    return result


def etl_image():
    # 链接数据库，并创建游标
    conn1 = pymysql.connect(host='192.168.0.79', port=3306, user='readonly',
                            passwd='Kc&r+z1ql9x8za4kzfk761weq8ozGv3ZpI;RMe,#+s%c>t', db='db_data')
    conn2 = pymysql.connect(host='192.168.0.65', port=3306, user='readonly',
                            passwd='Kc&r+z1ql9x8za4kzfk761weq8ozGv3ZpI;RMe,#+s%c>t', db='db_ptom')
    end = datetime.datetime.now().strftime('%Y-%m-%d')
    begin = (datetime.datetime.now() - pd.Timedelta(days=3)).strftime('%Y-%m-%d')
    # 获取计划数据
    planDataList = getPlanData(conn1, begin, end)
    # 获取次留数据
    roleRetainList = getCreateRoleRetain(conn1, begin, end)
    # 获取素材报表数据
    imageReportDataList = getImageReportData(conn2, begin, end)
    # 合并数据

    temp = pd.merge(planDataList, imageReportDataList, on='image_id', how='inner')
    result_df = pd.merge(temp, roleRetainList, on='image_id', how='inner')
    result_df['model_run_datetime'] = end
    # print(result_df.columns)
    # print(result_df.shape)
    result_df['media_id'] = 10
    result_df['data_win'] = 3
    conn1.close()
    conn2.close()
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


def Prob2Score(prob, basePoint=600, PDO=30):
    # 将概率转化成分数且为正整数
    y = np.log(prob/(1-prob))
    result = basePoint + int(PDO / np.log(2) * (y))
    return (result)


def image_sorce():
    image_info = etl_image()
    image_info = image_info[image_info['image_run_date_amount'] > 1000]
    image_info['image_create_role_cost'].replace(0, float('inf'), inplace=True)
    image_info['image_create_role_pay_cost'].replace(0, float('inf'), inplace=True)
    # 分桶定义（根据训练模型给出）
    pinf = float('inf')  # 正无穷大
    ninf = float('-inf')  # 负无穷大
    cutx1 = [ninf, 950.445, 1509.64, 2353.295, 3463.51, 5095.1, 7437.14, 11779.07, 20877.87, 45370.76, pinf]
    cutx2 = [ninf, 1, 3, 8, 12, 50, pinf]
    cutx3 = [ninf, 5.0, 12.0, 23.0, 46.0, 83.0, 143.0, 260.0, 545.0, 1461.0, pinf]
    cutx4 = [ninf, 60, 200, 700, 2000, 5000, 10000, pinf]
    cutx5 = [ninf, 3, 6, 12, 20, 50, pinf]
    cutx6 = [ninf, 0.005, 0.01, 0.015, 0.02, pinf]
    cutx7 = [ninf, 21.2804, 29.9661, 39.486, 49.7108, 63.7436, 82.9905, 111.5605, 164.255, 280.8425, pinf]
    cutx8 = [ninf, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 11000, 13000, pinf]
    cutx9 = [ninf, 0.1, 0.2, 0.3, 0.4, 0.6, pinf]
    cutx10 = [ninf, 50, 100, 200, 400, 1000, pinf]
    cutx11 = [ninf, 1, 2, 5, 10, 20, pinf]
    cutx12 = [ninf, 0.005, 0.01, 0.015, 0.02, 0.025, 0.05, pinf]
    cutx13 = [ninf, 22761.0, 46271.25, 82718.25, 145704.0, 261913.0, 481238.75, 1116520.25, pinf]
    cutx14 = [ninf, 2381.5, 5161.0, 9077.5, 15171.0, 24842.0, 40153.0, 67037.5, 118796.0, 267641.5, pinf]
    cutx16 = [ninf, 1004.0, 2291.0, 4006.0, 7064.0, 11651.0, 19039.0, 32327.0, 60041.0, 132055.0, pinf]
    cutx20 = [ninf, 0.06, 0.2, 0.5, 0.8, pinf]
    cutx27 = [ninf, 2, 8, 20, 50, 100, pinf]
    cutx28 = [ninf, 2, 6, 10, 20, 40, 60, pinf]
    cutx29 = [ninf, 4.0, 11.0, 23.0, 44.0, 79.0, 143.0, 264.0, 529.0, 1271.0, pinf]
    cutx30 = [ninf, 3, 50, 200, 500, 1000, pinf]
    cutx31 = [ninf, 1, 2, 10, 100, 200, pinf]
    cutx32 = [ninf, 0.06, 0.1, 0.13, 0.2, pinf]
    woex1 = [1.121, 0.742, 0.763, 0.624, 0.465, 0.226, 0.062, -0.487, -0.821, -1.561]
    woex2 = [0.693, -0.447, -0.962, -1.326, -1.979, -4.051]
    woex3 = [1.398, 0.994, 0.778, 0.637, 0.428, 0.244, -0.075, -0.447, -0.918, -1.65]
    woex4 = [0.746, -0.128, -0.896, -1.539, -2.373, -3.522, -4.626]
    woex5 = [0.459, -0.147, -0.461, -0.625, -1.187, -2.679]
    woex6 = [0.476, -0.908, -0.962, -0.879, -0.246]
    woex7 = [-0.865, -0.657, -0.577, -0.439, -0.197, 0.204, 0.381, 0.809, 1.247, 1.41]
    woex8 = [-1.462, -1.429, -1.269, -0.956, -0.742, -0.448, -0.08, -0.067, 0.088, -0.036, 0.105, 0.854]
    woex9 = [0.381, -0.356, -1.3, -1.253, -1.479, -1.294]
    woex10 = [0.338, -0.973, -1.761, -2.275, -2.836, -3.273]
    woex11 = [0.277, -1.526, -2.09, -2.522, -2.506, -2.547]
    woex12 = [0.798, -0.017, -0.483, -0.984, -1.124, -1.877, -2.453]
    woex13 = [0.549, 0.76, 0.744, 0.482, 0.1, -0.108, -0.507, -1.346]
    woex14 = [0.566, 0.949, 0.71, 0.496, 0.442, 0.091, -0.009, -0.298, -0.553, -1.549]
    woex16 = [0.522, 0.779, 0.842, 0.438, 0.409, 0.162, -0.105, -0.251, -0.452, -1.565]
    woex20 = [-0.825, 0.569, 0.114, 0.654, -0.407]
    woex27 = [0.359, -0.375, -0.869, -1.492, -2.487, -2.809]
    woex28 = [0.599, 0.441, 0.265, -0.12, -0.53, -0.581, -1.05]
    woex29 = [0.55, 0.702, 0.665, 0.563, 0.48, 0.396, -0.021, -0.318, -0.633, -1.568]
    woex30 = [-0.115, 0.539, -0.09, -0.769, -1.17, -1.95]
    woex31 = [0.216, 0.374, -0.114, -0.801, -1.614, -2.779]
    woex32 = [0.708, -0.293, -0.479, -0.116, 0.687]
    # 数据转化
    image_info_change = image_info.copy()
    image_info_change['image_run_date_amount'] = change_woe(image_info_change['image_run_date_amount'], cutx1, woex1)
    image_info_change['image_create_role_pay_num'] = change_woe(image_info_change['image_create_role_pay_num'], cutx2,
                                                                woex2)
    image_info_change['image_create_role_num'] = change_woe(image_info_change['image_create_role_num'], cutx3, woex3)
    image_info_change['image_create_role_pay_sum'] = change_woe(image_info_change['image_create_role_pay_sum'], cutx4,
                                                                woex4)
    image_info_change['image_source_num'] = change_woe(image_info_change['image_source_num'], cutx5, woex5)
    image_info_change['image_create_role_pay_rate'] = change_woe(image_info_change['image_create_role_pay_rate'], cutx6,
                                                                 woex6)
    image_info_change['image_create_role_cost'] = change_woe(image_info_change['image_create_role_cost'], cutx7, woex7)
    image_info_change['image_create_role_pay_cost'] = change_woe(image_info_change['image_create_role_pay_cost'], cutx8,
                                                                 woex8)
    image_info_change['image_valid_source_rate'] = change_woe(image_info_change['image_valid_source_rate'], cutx9,
                                                              woex9)
    image_info_change['image_pay_sum_ability'] = change_woe(image_info_change['image_pay_sum_ability'], cutx10, woex10)
    image_info_change['image_pay_num_ability'] = change_woe(image_info_change['image_pay_num_ability'], cutx11, woex11)
    image_info_change['image_create_role_roi'] = change_woe(image_info_change['image_create_role_roi'], cutx12, woex12)
    image_info_change['total_play'] = change_woe(image_info_change['total_play'], cutx13, woex13)
    image_info_change['valid_play'] = change_woe(image_info_change['valid_play'], cutx14, woex14)
    image_info_change['play_over'] = change_woe(image_info_change['play_over'], cutx16, woex16)
    image_info_change['play_duration_5s_rate'] = change_woe(image_info_change['play_duration_5s_rate'], cutx20, woex20)
    image_info_change['share'] = change_woe(image_info_change['share'], cutx27, woex27)
    image_info_change['comment'] = change_woe(image_info_change['comment'], cutx28, woex28)
    image_info_change['like'] = change_woe(image_info_change['like'], cutx29, woex29)
    image_info_change['dislike'] = change_woe(image_info_change['dislike'], cutx30, woex30)
    image_info_change['report'] = change_woe(image_info_change['report'], cutx31, woex31)
    image_info_change['image_create_role_retain_1d'] = change_woe(image_info_change['image_create_role_retain_1d'],
                                                                  cutx32, woex32)

    select_feature = ['image_run_date_amount', 'image_create_role_pay_num',
                      'image_create_role_num', 'image_create_role_pay_sum',
                      'image_source_num', 'image_create_role_pay_rate',
                      'image_create_role_cost', 'image_create_role_pay_cost',
                      'image_valid_source_rate',
                      'image_pay_sum_ability', 'image_pay_num_ability',
                      'image_create_role_roi', 'total_play', 'valid_play',
                      'play_over',
                      'play_duration_5s_rate', 'share', 'comment', 'like', 'dislike',
                      'report', 'image_create_role_retain_1d']
    # 模型导入
    model_path = "./"
    best_est_XGB = joblib.load(model_path + 'best_est_XGB.pkl')
    best_est_LGB = joblib.load(model_path + 'best_est_LGB.pkl')
    best_est_RF = joblib.load(model_path + 'best_est_RF.pkl')
    feature = image_info_change[select_feature]
    image_info_change['pred'] = 0.4 * best_est_XGB.predict_proba(feature)[:, 1] + 0.3 * best_est_LGB.predict_proba(
        feature)[:, 1] + 0.3 * best_est_RF.predict_proba(feature)[:, 1]
    image_info_change['score'] = image_info_change['pred'].apply(Prob2Score)
    image_info['score'] = image_info_change['score']

    image_info.to_csv('./image_info_sorce.csv')


# 运行程序
if __name__ == '__main__':
    image_sorce()
