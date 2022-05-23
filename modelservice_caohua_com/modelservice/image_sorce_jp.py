import pymysql
import datetime
import pandas as pd
import numpy as np
import joblib
import os
import logging
import json
import warnings
warnings.filterwarnings('ignore')

logger = logging.getLogger('ImageScoreJp')

from modelservice.__myconf__ import get_var
dicParam = get_var()

#
# 打包接口
#
class ImageScoreJp:
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
                                                    AND a.amount > 0 
                                                    AND a.media_id IN ( 10, 16 ) 
                                                    AND b.image_id IS NOT NULL 
                                                    AND b.image_id <> '' 
                                                    AND a.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1051 AND dev_game_id IS NOT NULL ) 
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
                                                    AND a.media_id IN ( 10, 16 ) 
                                                    AND b.image_id IS NOT NULL 
                                                    AND b.image_id <> '' 
                                                    AND a.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1051 AND dev_game_id IS NOT NULL ) 
                                                    AND a.platform = 1 
                                                GROUP BY
                                                    b.image_id,
                                                    a.media_id 
                                                HAVING
                                                    sum( a.amount ) / sum( a.pay_role_user_num )< 3500
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
                                                    AND a.tdate >= DATE_SUB( date( NOW()), INTERVAL 3 DAY ) 
                                                    AND a.tdate <= date( NOW()) AND a.amount > 100 
                                                    AND a.media_id IN ( 10, 16 ) 
                                                    AND b.image_id IS NOT NULL 
                                                    AND b.image_id <> '' 
                                                    AND a.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1051 AND dev_game_id IS NOT NULL ) 
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
                                                AND a.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1051 AND dev_game_id IS NOT NULL ) 
                                            GROUP BY
                                            b.image_id 
                ) c ON a.image_id = c.image_id
    '''
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
                    AND m.media_id in (10,16) 
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
    # os.system("hadoop fs -rm -r hdfs://masters/user/hive/warehouse/dws.db/dws_image_score_d/dt="+run_dt+"/hr="+run_hour)
    # os.system("hadoop fs -mkdir hdfs://masters/user/hive/warehouse/dws.db/dws_image_score_d/dt=" + run_dt)
    # os.system("hadoop fs -mkdir hdfs://masters/user/hive/warehouse/dws.db/dws_image_score_d/dt="+run_dt+"/hr="+run_hour)
    os.system("hadoop fs -put image_info_sorce_jp.csv /warehouse/tablespace/managed/hive/dws.db/dws_image_score_d/dt="+run_dt+"/hr="+run_hour)
    os.system("beeline -u \"jdbc:hive2://bigdata-zk01.ch:2181,bigdata-zk02.ch:2181,bigdata-zk03.ch:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2\" -nhive -phive -e \"set hive.msck.path.validation=ignore;msck repair table dws.dws_image_score_d;\"")

def image_score():
    # 模型导入
    model_path = "./aimodel/"
    # best_est_XGB = joblib.load(model_path + 'best_est_XGB_jp.pkl')  ## todo:选择创角成本低于800的素材
    # best_est_LGB = joblib.load(model_path + 'best_est_LGB_jp.pkl')
    # best_est_RF = joblib.load(model_path + 'best_est_RF_jp.pkl')
    best_est_XGB = joblib.load(model_path + 'best_est_XGB_jp_up1000.pkl')  ## todo:选择创角成本低于800且消耗高于1000的素材
    best_est_LGB = joblib.load(model_path + 'best_est_LGB_jp_up1000.pkl')
    best_est_RF = joblib.load(model_path + 'best_est_RF_jp_up1000.pkl')

    # 数据获取
    image_info = etl_image()
    image_info = image_info[image_info['image_run_date_amount'] > 1000]

    # 将无付费和无创角的成本由0改为无穷大
    image_info['image_create_role_cost'].replace(0, float('inf'), inplace=True)
    image_info['image_create_role_pay_cost'].replace(0, float('inf'), inplace=True)
    # 分桶定义（具体值根据训练模型给出）
    pinf = float('inf')  # 正无穷大
    ninf = float('-inf')  # 负无穷大
    # woex1 = [1.04, 0.841, 0.651, 0.629, 0.469, 0.322, 0.129, -0.276, -0.852, -1.894]
    # woex2 = [1.186, 0.214, -0.117, -0.513, -1.203, -2.699]
    # woex3 = [1.491, 0.822, 0.704, 0.546, 0.4, 0.253, -0.033, -0.294, -0.776, -1.918]
    # woex4 = [1.281, 0.131, -0.685, -1.689, -2.761, -3.769, -6.92]
    # woex5 = [0.485, 0.078, -0.255, -0.618, -1.317, -2.605]
    # woex6 = [1.658, 0.843, 1.011, 1.193, -0.3]
    # woex7 = [-0.301, -0.555, -0.383, -0.292, -0.184, 0.002, 0.145, 0.361, 0.616, 1.123]
    # woex8 = [-0.41, 0.694, 0.806, 1.214, 1.28, 1.263, 1.291, 1.391, 1.491, 1.591, 1.658, 1.691]
    # woex9 = [1.816, 0.111, 0.105, 0.091, -0.007, -0.453]
    # woex10 = [0.922, -0.5, -1.229, -2.086, -2.975, -4.565]
    # woex11 = [0.716, -0.351, -0.834, -1.298, -1.772, -2.883]
    # woex12 = [1.566, 1.135, 0.666, 0.544, 0.224, -0.274, -2.027]
    # woex13 = [3.704, 1.259, 0.361, 0.086, -0.352, -0.075]
    woex1 = [0.869, 0.803, 0.681, 0.62, 0.501, 0.368, 0.012, -0.312, -0.876, -1.911]
    woex2 = [1.318, 0.425, 0.041, -0.357, -1.047, -2.543]
    woex3 = [1.181, 0.818, 0.725, 0.523, 0.455, 0.185, -0.02, -0.342, -0.826, -1.904]
    woex4 = [1.427, 0.347, -0.485, -1.522, -2.599, -3.611, -6.764]
    woex5 = [0.474, 0.131, -0.156, -0.492, -1.168, -2.449]
    woex6 = [1.868, 0.999, 1.152, 1.421, -0.193]
    woex7 = [-0.288, -0.55, -0.378, -0.278, -0.177, -0.044, 0.18, 0.391, 0.493, 1.074]
    woex8 = [-0.41, 0.694, 0.806, 1.214, 1.28, 1.263, 1.291, 1.391, 1.491, 1.591, 1.658, 1.691]
    woex9 = [2.006, 0.275, 0.232, 0.184, 0.119, -0.345]
    woex10 = [0.996, -0.341, -1.049, -1.912, -2.799, -4.387]
    woex11 = [0.747, -0.211, -0.686, -1.144, -1.616, -2.727]
    woex12 = [1.703, 1.265, 0.805, 0.712, 0.339, -0.147, -1.915]
    woex13 = [3.145, 1.331, 0.431, 0.135, -0.278, -0.028]
    # cutx1 = [ninf, 758.972, 1101.794, 1547.41, 2128.316, 2990.66, 4236.064, 6311.489, 10646.002, 23479.832, pinf]
    # cutx2 = [ninf, 1, 3, 8, 12, 50, pinf]
    # cutx3 = [ninf, 4.0, 8.0, 12.0, 19.0, 27.0, 42.0, 67.0, 122.0, 309.0, pinf]
    # cutx4 = [ninf, 60, 200, 700, 2000, 5000, 10000, pinf]
    # cutx5 = [ninf, 3, 6, 12, 20, 50, pinf]
    # cutx6 = [ninf, 0.005, 0.01, 0.015, 0.02, pinf]
    # cutx7 = [ninf, 48.028, 63.8648, 78.0665, 92.6571, 109.8316, 131.176, 159.4305, 203.7499, 296.2053, pinf]
    # cutx8 = [ninf, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 11000, 13000, pinf]
    # cutx9 = [ninf, 0.1, 0.2, 0.3, 0.4, 0.6, pinf]
    # cutx10 = [ninf, 50, 100, 200, 400, 1000, pinf]
    # cutx11 = [ninf, 1, 2, 5, 10, 20, pinf]
    # cutx12 = [ninf, 0.005, 0.01, 0.015, 0.02, 0.025, 0.05, pinf]
    # cutx13 = [ninf, 0.06, 0.1, 0.14, 0.2, 0.3, pinf]
    cutx1 = [ninf, 1372.244, 1785.55, 2292.154, 3030.192, 4061.34, 5544.584, 8137.632, 13236.814, 28521.23, pinf]
    cutx2 = [ninf, 1, 3, 8, 12, 50, pinf]
    cutx3 = [ninf, 8.0, 13.0, 19.0, 27.0, 39.0, 58.0, 90.0, 162.0, 381.0, pinf]
    cutx4 = [ninf, 60, 200, 700, 2000, 5000, 10000, pinf]
    cutx5 = [ninf, 3, 6, 12, 20, 50, pinf]
    cutx6 = [ninf, 0.005, 0.01, 0.015, 0.02, pinf]
    cutx7 = [ninf, 47.7098, 63.0009, 76.7518, 90.849, 107.08, 126.5819, 152.7279, 192.3649, 267.68, pinf]
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

    select_feature = ['image_run_date_amount', 'image_create_role_pay_num',
                            'image_create_role_num', 'image_create_role_pay_sum',
                            'image_source_num', 'image_create_role_pay_rate',
                            'image_create_role_cost', 'image_create_role_pay_cost',
                            'image_valid_source_rate',
                            'image_pay_sum_ability', 'image_pay_num_ability',
                            'image_create_role_roi']

    # 概率预测与分数计算
    feature = image_info_change[select_feature]
    image_info_change['pred'] = 0.4 * best_est_XGB.predict_proba(feature)[:, 1] + 0.3 * \
                                best_est_LGB.predict_proba(feature)[:,
                                1] + 0.3 * best_est_RF.predict_proba(feature)[:, 1]
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
    image_info.to_csv('./image_info_sorce_jp.csv', index=0, encoding='utf_8_sig', header=None)  ## , header=None
