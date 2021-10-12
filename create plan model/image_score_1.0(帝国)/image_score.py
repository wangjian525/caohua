import pymysql
import datetime
import pandas as pd
import numpy as np
import joblib
import warnings
warnings.filterwarnings('ignore')


# 获取素材报表数据
def getPlanData(conn, begin, end):
    originSql = '''
            SELECT
                a.image_id AS 'image_id',
                a.image_name AS 'image_name',
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
                                                b.image_id,
                                                c.image_name,
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
                                                LEFT JOIN db_data_ptom.ptom_image_info c ON b.image_id = c.image_id 
                                            WHERE
                                                a.tdate_type = 'day' 
                                                AND a.tdate >= '{begin}' 
                                                AND a.tdate <= '{end}' AND a.amount > 100 
                                                AND b.image_id IS NOT NULL 
                                                AND b.image_id <> '' 
                                                AND a.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1112 AND dev_game_id IS NOT NULL ) 
                                            GROUP BY
                                                b.image_id,
                                                c.image_name 
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
                                                    AND a.tdate <= '{end}' AND a.amount > 100 
                                                    AND b.image_id IS NOT NULL 
                                                    AND b.image_id <> '' 
                                                    AND a.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1112 AND dev_game_id IS NOT NULL ) 
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
                                                    AND a.tdate <= '{end}' AND a.amount > 100 
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
                                                AND a.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1056 AND dev_game_id IS NOT NULL ) 
                                            GROUP BY
                                                b.image_id 
                                            ) c ON a.image_id = c.image_id
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
    '''
    获取mysql数据
    :return:
    '''
    # 链接数据库，并创建游标
    conn1 = pymysql.connect(host='192.168.0.79', port=3306, user='readonly',
                            passwd='Kc&r+z1ql9x8za4kzfk761weq8ozGv3ZpI;RMe,#+s%c>t', db='db_data')
    # cur1 = conn1.cursor(cursor=pymysql.cursors.DictCursor)

    end = datetime.datetime.now().strftime('%Y-%m-%d')
    begin = (datetime.datetime.now() - pd.Timedelta(days=3)).strftime('%Y-%m-%d')
    # 获取计划数据
    planDataList = getPlanData(conn1, begin, end)
    # 获取次留数据
    roleRetainList = getCreateRoleRetain(conn1, begin, end)
    # 合并数据

    result_df = pd.merge(planDataList, roleRetainList, on='image_id', how='left')
    result_df['model_run_datetime'] = end
    result_df['data_win'] = 3
    conn1.close()
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
    # 将概率转化成分数且为正整数  基础分为600
    y = np.log(prob / (1 - prob))
    result = basePoint + int(PDO / np.log(2) * (y))
    return (result)


def image_score():
    # 模型导入
    model_path = "./"
    best_est_XGB = joblib.load(model_path + 'best_est_XGB.pkl')
    best_est_LGB = joblib.load(model_path + 'best_est_LGB.pkl')
    best_est_RF = joblib.load(model_path + 'best_est_RF.pkl')
    best_est_XGB_retain = joblib.load(model_path + 'best_est_XGB_retain.pkl')
    best_est_LGB_retain = joblib.load(model_path + 'best_est_LGB_retain.pkl')
    best_est_RF_retain = joblib.load(model_path + 'best_est_RF_retain.pkl')

    # 数据获取
    image_info = etl_image()
    image_info = image_info[image_info['image_run_date_amount'] > 1000]

    # 将无付费和无创角的成本由0改为无穷大
    image_info['image_create_role_cost'].replace(0, float('inf'), inplace=True)
    image_info['image_create_role_pay_cost'].replace(0, float('inf'), inplace=True)
    # 分桶定义（具体值根据训练模型给出）
    pinf = float('inf')  # 正无穷大
    ninf = float('-inf')  # 负无穷大
    woex1 = [1.332, 1.202, 0.953, 0.845, 0.503, 0.216, 0.031, -0.363, -0.871, -1.702]
    woex2 = [0.896, -0.47, -1.078, -1.616, -2.105, -4.266]
    woex3 = [1.902, 1.227, 0.873, 0.771, 0.503, 0.215, 0.022, -0.394, -0.862, -1.784]
    woex4 = [1.0, -0.098, -0.943, -1.772, -2.692, -3.759, -5.138]
    woex5 = [0.552, -0.086, -0.573, -0.865, -1.345, -3.033]
    woex6 = [0.691, -0.919, -0.999, -0.956, -0.646]
    woex7 = [-0.609, -0.703, -0.521, -0.481, -0.139, 0.059, 0.359, 0.689, 1.315, 1.777]
    woex8 = [-1.338, -1.316, -1.277, -0.962, -0.781, -0.609, -0.363, -0.261, -0.112, -0.076, 0.243, 1.219]
    woex9 = [0.513, -0.717, -1.43, -1.04, -1.413, -1.256]
    woex10 = [0.443, -0.981, -1.658, -2.221, -2.823, -3.33]
    woex11 = [0.343, -1.502, -2.005, -2.372, -2.66, -2.883]
    woex12 = [1.104, 0.044, -0.404, -0.896, -1.163, -1.872, -2.508]
    woex13 = [0.712, -0.167, -0.338, -0.162, 0.212, 0.47]
    cutx1 = [ninf, 891.102, 1409.628, 2165.43, 3167.068, 4656.48, 6886.692, 10651.84, 18862.02, 40665.782, pinf]
    cutx2 = [ninf, 1, 3, 8, 12, 50, pinf]
    cutx3 = [ninf, 6.0, 14.0, 27.0, 51.0, 85.0, 142.0, 244.0, 480.0, 1271.0, pinf]
    cutx4 = [ninf, 60, 200, 700, 2000, 5000, 10000, pinf]
    cutx5 = [ninf, 3, 6, 12, 20, 50, pinf]
    cutx6 = [ninf, 0.005, 0.01, 0.015, 0.02, pinf]
    cutx7 = [ninf, 20.073, 28.7217, 36.4717, 45.8384, 57.2259, 74.043, 98.3442, 141.3023, 254.55, pinf]
    cutx8 = [ninf, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 11000, 13000, pinf]
    cutx9 = [ninf, 0.1, 0.2, 0.3, 0.4, 0.6, pinf]
    cutx10 = [ninf, 50, 100, 200, 400, 1000, pinf]
    cutx11 = [ninf, 1, 2, 5, 10, 20, pinf]
    cutx12 = [ninf, 0.005, 0.01, 0.015, 0.02, 0.025, 0.05, pinf]
    cutx13 = [ninf, 0.06, 0.1, 0.14, 0.2, 0.3, pinf]
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

    image_info_change_na = image_info_change[image_info_change['image_create_role_retain_1d'].isna()]
    image_info_change_notna = image_info_change[image_info_change['image_create_role_retain_1d'].notna()]
    if image_info_change_notna.shape[0] != 0:
        image_info_change_notna['image_create_role_retain_1d'] = change_woe(image_info_change_notna
                                                                            ['image_create_role_retain_1d'], cutx13,
                                                                            woex13)

        select_feature_notna = ['image_run_date_amount', 'image_create_role_pay_num',
                                'image_create_role_num', 'image_create_role_pay_sum',
                                'image_source_num', 'image_create_role_pay_rate',
                                'image_create_role_cost', 'image_create_role_pay_cost',
                                'image_valid_source_rate',
                                'image_pay_sum_ability', 'image_pay_num_ability',
                                'image_create_role_roi', 'image_create_role_retain_1d']

        # 概率预测与分数计算
        feature = image_info_change_notna[select_feature_notna]
        image_info_change_notna['pred'] = 0.4 * best_est_XGB_retain.predict_proba(feature)[:, 1] + 0.3 * \
                                          best_est_LGB_retain.predict_proba(feature)[:,
                                          1] + 0.3 * best_est_RF_retain.predict_proba(feature)[:, 1]
        image_info_change_notna['score'] = image_info_change_notna['pred'].apply(Prob2Score)

    if image_info_change_na.shape[0] != 0:
        select_feature_na = ['image_run_date_amount', 'image_create_role_pay_num',
                             'image_create_role_num', 'image_create_role_pay_sum',
                             'image_source_num', 'image_create_role_pay_rate',
                             'image_create_role_cost', 'image_create_role_pay_cost',
                             'image_valid_source_rate',
                             'image_pay_sum_ability', 'image_pay_num_ability',
                             'image_create_role_roi']

        # 概率预测与分数计算
        feature = image_info_change_na[select_feature_na]
        image_info_change_na['pred'] = 0.4 * best_est_XGB.predict_proba(feature)[:, 1] + 0.3 * \
                                       best_est_LGB.predict_proba(feature)[:,
                                       1] + 0.3 * best_est_RF.predict_proba(feature)[:, 1]
        image_info_change_na['score'] = image_info_change_na['pred'].apply(Prob2Score)

    image_info_change = pd.concat([image_info_change_notna, image_info_change_na], axis=0)
    temp = image_info_change[['image_id', 'score']]
    image_info = pd.merge(image_info, temp, on='image_id', how='left')
    image_info = image_info[['image_id', 'image_name', 'image_run_date_amount', 'image_create_role_pay_num',
                               'image_create_role_num', 'image_create_role_pay_sum',
                               'image_source_num', 'image_create_role_pay_rate',
                               'image_create_role_cost', 'image_create_role_pay_cost',
                               'image_valid_source_num', 'image_valid_source_rate',
                               'image_pay_sum_ability', 'image_pay_num_ability',
                               'image_create_role_roi', 'image_create_role_retain_1d',
                               'model_run_datetime', 'data_win', 'score', 'image_launch_time', 'image_source_total_num']]
    # 数据导出
    image_info.to_csv('./image_info_sorce.csv', index=0, encoding='utf_8_sig')


# 运行程序
if __name__ == '__main__':
    image_score()
