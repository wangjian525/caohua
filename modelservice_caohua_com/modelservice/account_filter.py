import pymysql
import datetime
import pandas as pd
import numpy as np

import os
import logging
import json
import warnings
warnings.filterwarnings('ignore')
logger = logging.getLogger('AccountFilter')

from modelservice.__myconf__ import get_var
dicParam = get_var()


#
# 打包接口
#
class AccountFilter:
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
        main()
        load_to_hive()
        ret = json.dumps({"code": 200, "msg": "success!", "data": "hive load success123"})
        return ret


def get_account_info():
    conn = pymysql.connect(host=dicParam['DB_SLAVE_FENXI_HOST'], port=int(dicParam['DB_SLAVE_FENXI_PORT']), user=dicParam['DB_SLAVE_FENXI_USERNAME'],
                           passwd=dicParam['DB_SLAVE_FENXI_PASSWORD'])
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = '''
        /*手动查询*/ 
        SELECT
            aa.ad_account_id AS 'ad_account_id',
            aa.media_id AS 'media_id',
            aa.game_id as 'game_id',
            sum( aa.amount ) AS 'amount',
            IFNULL( sum( aa.create_role_num ), 0 ) AS 'create_role_num',
            IFNULL( sum( bb.pay_role_user_num ), 0 ) AS 'pay_num',
            IFNULL( sum( bb.new_role_money ), 0 ) AS 'pay_sum',
            (
         CASE
                    WHEN ifnull( sum( aa.amount ), 0 )= 0 THEN
                            0 ELSE IFNULL( sum( bb.new_role_money ), 0 ) / ifnull( sum( aa.amount ), 0 )
                                                    END 
                                                    ) AS 'create_role_roi' 
        FROM
            (
            SELECT
                a.game_id,
                a.channel_id,
                a.source_id,
                b.ad_account_id,
                a.media_id,
                IFNULL( sum( a.amount ), 0 ) AS amount,
                IFNULL( sum( create_role_num ), 0 ) AS create_role_num
            FROM
                db_stdata.st_lauch_report a
                INNER JOIN db_data_ptom.ptom_plan b ON a.game_id = b.game_id 
                AND a.source_id = b.source_id 
                AND a.channel_id = b.chl_user_id
            WHERE
                a.tdate_type = 'day' 
                AND a.tdate <= DATE_SUB( date( NOW()), INTERVAL 1 DAY )  AND a.tdate >= DATE_SUB( date( NOW()), INTERVAL 15 DAY ) 
                AND a.amount > 0 
                AND a.media_id in (10,16)
                AND b.ad_account_id IS NOT NULL 
                AND b.ad_account_id <> '' 
                AND a.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1056 AND dev_game_id IS NOT NULL ) 
                AND b.ad_account_id in (7982, 8037, 8082, 8080, 8078, 8076, 8075, 7981, 7984, 8035, 8036, 8038, 8039, 8077, 8073,
                8814, 8815, 8816, 8817, 8818, 8819, 8820, 8821, 8822, 8823, 8824, 8825, 8826, 8827, 8828,
                8829, 8830, 8831, 8832, 8833, 8834, 8835, 8836, 8837, 8838, 8839, 8840, 8841, 8842, 8843,
                8844, 8845, 8846, 8847, 8848, 8854, 8855, 8856, 8857, 8858, 8859, 8860, 8743, 8742, 8741,
                8740, 8739, 8738, 8737, 8736, 8735, 8734, 8733, 8732, 8731, 8730, 8729, 8728, 8727, 8726,
                8725, 8724, 
                8499, 8500, 8501, 8502, 8503, 8504, 8505, 8506, 8507, 8508,
                8518, 8517, 8516, 8515, 8514, 8513, 8512, 8511, 8510, 8509,
                7770, 7771, 7772, 7773, 7856)
            GROUP BY
                a.game_id,
                a.channel_id,
                a.source_id  
            ) aa
            LEFT JOIN (
                SELECT
                    c.game_id,
                    c.channel_id,
                    c.source_id,
                    sum( c.create_role_money ) new_role_money,
                    IFNULL( sum( c.pay_role_user_num ), 0 ) AS pay_role_user_num 
                FROM
                    db_stdata.st_game_days c 
                WHERE
                    c.report_days = 15 
                    AND c.tdate = date( NOW() - INTERVAL 24 HOUR ) 
                    AND c.tdate_type = 'day' 
                    AND c.query_type = 13 
                GROUP BY
                    c.game_id,
                    c.channel_id,
                    c.source_id 
                HAVING
                ( new_role_money > 0 OR pay_role_user_num > 0 )
            ) bb ON aa.game_id = bb.game_id 
            AND aa.channel_id = bb.channel_id 
            AND aa.source_id = bb.source_id 
        GROUP BY
            aa.ad_account_id,
            aa.game_id
    '''
    result_df = pd.read_sql(sql, conn)
    cur.close()
    conn.close()
    return result_df


def load_to_hive():
    # 将生成的csv文件加载到hive中
    run_dt = datetime.datetime.now().strftime('%Y-%m-%d')
    os.system("hadoop fs -rm -r /warehouse/tablespace/managed/hive/dws.db/account_info/dt="+run_dt)
    os.system("hadoop fs -mkdir /warehouse/tablespace/managed/hive/dws.db/account_info/dt=" + run_dt)
    os.system("hadoop fs -put account_info.csv /warehouse/tablespace/managed/hive/dws.db/account_info/dt="+run_dt)
    os.system("beeline -u \"jdbc:hive2://bigdata-zk01.ch:2181,bigdata-zk02.ch:2181,bigdata-zk03.ch:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2\" -nhive -phive -e \"msck repair table dws.account_info\"")


def main():
    account_info = get_account_info()
    account_info['account_num'] = account_info.groupby(['media_id', 'game_id'])['ad_account_id'].transform('count')
    account_info['mean_roi'] = account_info.groupby(['media_id', 'game_id'])['pay_sum'].transform('sum') / \
                               account_info.groupby(['media_id', 'game_id'])['amount'].transform('sum')
    account_info['rank_roi'] = account_info.groupby(['media_id', 'game_id'])['create_role_roi'].rank(
        method='min').astype(int)
    account_info['label'] = account_info.apply(
        lambda x: 1 if (x.create_role_roi < x.mean_roi * 0.8) & (x.rank_roi <= np.ceil(x.account_num * 0.3)) & (
                    x.amount >= 40000) else 0, axis=1)

    # 数据导出
    account_info.to_csv('./account_info.csv', index=0, encoding='utf_8_sig', header=None)
