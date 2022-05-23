import pymysql
import datetime
import pandas as pd
import numpy as np
import joblib
import os
import logging
import time
import json
import warnings
warnings.filterwarnings('ignore')

logger = logging.getLogger('ImageScoreTk')

from modelservice.__myconf__ import get_var
dicParam = get_var()

#
# 打包接口
#
class ImageScoreTk:
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
        image_score()
        load_to_hive()
        ret = json.dumps({"code": 200, "msg": "success!", "data": "hive load success123"})
        return ret


# 获取素材报表数据
def getPlanData(conn):
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
                                                AND a.tdate <= date( NOW()) AND a.tdate >= DATE_SUB( date( NOW()), INTERVAL 3 DAY ) 
                                                AND a.platform = 1
                                                AND a.amount > 0 
                                                AND a.media_id IN ( 10, 16 ,32, 45) 
                                                AND b.image_id IS NOT NULL 
                                                AND b.image_id <> '' 
                                                AND a.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1043 AND dev_game_id IS NOT NULL ) 
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
                                                    a.tdate = date(NOW())
                                                    AND a.platform = 1
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
                                                    AND c.platform = 1	
                                                    AND c.tdate = date( NOW() - INTERVAL 24 HOUR ) 
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
                                                AND a.tdate >= DATE_SUB( date( NOW()), INTERVAL 3 DAY ) 
                                                AND a.tdate <= date( NOW()) AND a.amount > 100 
                                                AND a.media_id IN ( 10, 16, 32, 45) 
                                                AND b.image_id IS NOT NULL 
                                                AND b.image_id <> '' 
                                                AND a.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1043 AND dev_game_id IS NOT NULL ) 
                                                AND a.platform = 1 
                                            GROUP BY
                                                b.image_id,
                                                a.media_id 
                                            HAVING
                                                sum( a.amount ) / sum( a.pay_role_user_num )< 2500 
                                                AND sum( a.pay_role_user_num )> 0 
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
                                            AND a.platform = 1	
                                            AND b.image_id IS NOT NULL 
                                            AND b.image_id <> '' 
                                            AND a.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1043 AND dev_game_id IS NOT NULL ) 
                                        GROUP BY
                                        b.image_id 
            ) c ON a.image_id = c.image_id
    '''
    # finalSql = originSql.format(begin=begin, end=end)
    result = pd.read_sql(originSql, conn)
    return result


# 获取次留率数据
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
                    AND m.media_id in (10,16, 32, 45) 
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


def etl_image():
    '''
    获取mysql数据
    :return:
    '''
    # 链接数据库，并创建游标
    conn1 = pymysql.connect(host=dicParam['DB_SLAVE_FENXI_HOST'], port=int(dicParam['DB_SLAVE_FENXI_PORT']), user=dicParam['DB_SLAVE_FENXI_USERNAME'],
                           passwd=dicParam['DB_SLAVE_FENXI_PASSWORD'], db=dicParam['DB_SLAVE_FENXI_DATABASE'])

    # cur1 = conn1.cursor(cursor=pymysql.cursors.DictCursor)

    end = datetime.datetime.now().strftime('%Y-%m-%d')
    begin = (datetime.datetime.now() - pd.Timedelta(days=3)).strftime('%Y-%m-%d')
    # 获取计划数据
    planDataList = getPlanData(conn1)
    # 获取次留数据
    roleRetainList = getCreateRoleRetain(conn1, begin, end)
    # 合并数据

    result_df = pd.merge(planDataList, roleRetainList, on=['image_id', 'media_id'], how='left')
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

def load_to_hive():
    # 将生成的csv文件加载到hive中
    run_dt = datetime.datetime.now().strftime('%Y-%m-%d')
    run_hour = datetime.datetime.now().strftime('%H')
    # os.system("hadoop fs -rm -r /warehouse/tablespace/managed/hive/dws.db/dws_image_score_d/dt="+run_dt+"/hr="+run_hour)
    # os.system("hadoop fs -mkdir /warehouse/tablespace/managed/hive/dws.db/dws_image_score_d/dt=" + run_dt)
    # os.system("hadoop fs -mkdir /warehouse/tablespace/managed/hive/dws.db/dws_image_score_d/dt="+run_dt+"/hr="+run_hour)
    os.system("hadoop fs -put image_info_sorce_tk.csv /warehouse/tablespace/managed/hive/dws.db/dws_image_score_d/dt="+run_dt+"/hr="+run_hour)
    os.system("beeline -u \"jdbc:hive2://bigdata-zk01.ch:2181,bigdata-zk02.ch:2181,bigdata-zk03.ch:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2\" -nhive -phive -e \"set hive.msck.path.validation=ignore;msck repair table dws.dws_image_score_d;\"")

def image_score():
    # 模型导入
    model_path = "./aimodel/"
    best_est_XGB_tk = joblib.load(model_path + 'best_est_XGB_tk.pkl')
    best_est_LGB_tk = joblib.load(model_path + 'best_est_LGB_tk.pkl')
    best_est_RF_tk = joblib.load(model_path + 'best_est_RF_tk.pkl')

    # 数据获取
    image_info = etl_image()
    image_info = image_info[image_info['image_run_date_amount'] > 1000]

    # 将无付费和无创角的成本由0改为无穷大
    image_info['image_create_role_cost'].replace(0, float('inf'), inplace=True)
    image_info['image_create_role_pay_cost'].replace(0, float('inf'), inplace=True)

    # 分桶定义（具体值根据训练模型给出）
    pinf = float('inf')  # 正无穷大
    ninf = float('-inf')  # 负无穷大
    woex1 = [1.047, 1.047, 1.278, 0.463, 0.497, -0.01, -0.005, -0.585, -0.624, -1.426]
    woex2 = [0.707, -0.28, -0.791, -0.801, -1.795, -1.803]
    woex3 = [1.109, 1.129, 0.611, 0.424, 0.646, 0.225, -0.409, -0.496, -0.497, -1.409]
    woex4 = [1.093, 0.152, -0.263, -0.909, -1.932, -2.258, -2.723]
    woex5 = [0.243, 0.007, -0.284, -0.837, -0.699, -0.686]
    woex6 = [1.182, 0.749, -0.185, -0.568, -0.571]
    woex7 = [-0.128, -0.334, -0.312, -0.441, 0.021, -0.158, 0.302, 0.128, 0.569, 0.723]
    woex8 = [-0.757, -0.214, -0.022, -0.429, 0.595, -0.974, -0.686, 1.191]
    woex9 = [0.816, -0.707, -0.703, -0.735]
    woex10 = [0.641, 0.135, -0.372, -0.775, -1.375, -2.061]
    woex11 = [0.442, -0.849, -0.645, -1.304, -1.618, -1.934]
    woex12 = [1.02, 0.381, 0.846, -0.241, 0.173, -0.639, -1.992]
    woex13 = [1.717, 0.662, -0.528, -0.338, -0.344, -0.319]

    cutx1 = [ninf, 626.314, 818.768, 1129.172, 1494.544, 2125.82, 3057.808, 4600.364, 8104.098, 15747.532, ninf]
    cutx2 = [ninf, 1, 3, 8, 12, 50, ninf]
    cutx3 = [ninf, 3.0, 5.0, 7.0, 11.0, 16.0, 25.0, 40.0, 75.0, 176.0, ninf]
    cutx4 = [ninf, 12, 60, 200, 700, 2000, 5000, ninf]
    cutx5 = [ninf, 1, 2, 3, 5, 8, ninf]
    cutx6 = [ninf, 0.005, 0.01, 0.015, 0.02, ninf]
    cutx7 = [ninf, 67.057, 83.3311, 97.2183, 114.7394, 133.37, 155.4077, 185.0767, 236.536, 343.5266, ninf]
    cutx8 = [ninf, 2000, 3000, 4000, 5000, 6000, 7000, 8000, ninf]
    cutx9 = [ninf, 0.2, 0.6, 0.9, ninf]
    cutx10 = [ninf, 20, 40, 100, 200, 400, ninf]
    cutx11 = [ninf, 1, 2, 5, 10, 20, ninf]
    cutx12 = [ninf, 0.005, 0.01, 0.015, 0.02, 0.025, 0.05, ninf]
    cutx13 = [ninf, 0.06, 0.1, 0.14, 0.2, 0.3, ninf]
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

    select_feature = ['image_run_date_amount', 'image_create_role_pay_num',
                            'image_create_role_num', 'image_create_role_pay_sum',
                            'image_source_num', 'image_create_role_pay_rate',
                            'image_create_role_cost', 'image_create_role_pay_cost',
                            'image_valid_source_rate',
                            'image_pay_sum_ability', 'image_pay_num_ability',
                            'image_create_role_roi']

    # 概率预测与分数计算
    feature = image_info_change[select_feature]
    image_info_change['pred'] = 0.4 * best_est_XGB_tk.predict_proba(feature)[:, 1] + 0.3 * \
                                best_est_LGB_tk.predict_proba(feature)[:,
                                1] + 0.3 * best_est_RF_tk.predict_proba(feature)[:, 1]
    image_info_change['score'] = image_info_change['pred'].apply(Prob2Score)

    temp = image_info_change[['image_id', 'media_id', 'score']]
    image_info = pd.merge(image_info, temp, on=['image_id', 'media_id'], how='left')
    image_info = image_info[['image_id', 'image_name', 'image_run_date_amount', 'image_create_role_pay_num',
                               'image_create_role_num', 'image_create_role_pay_sum',
                               'image_source_num', 'image_create_role_pay_rate',
                               'image_create_role_cost', 'image_create_role_pay_cost',
                               'image_valid_source_num', 'image_valid_source_rate',
                               'image_pay_sum_ability', 'image_pay_num_ability',
                               'image_create_role_roi', 'image_create_role_retain_1d', 'model_run_datetime',
                               'data_win', 'score', 'image_launch_time', 'image_source_total_num', 'media_id',
                               'label_ids']]

    image_info['label_ids'] = image_info['label_ids'].str.replace(',', ';')
    # 数据导出
    image_info.to_csv('./image_info_sorce_tk.csv', index=0, encoding='utf_8_sig', header=None)


