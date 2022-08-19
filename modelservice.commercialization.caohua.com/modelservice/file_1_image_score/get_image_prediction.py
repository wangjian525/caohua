# -*- coding:utf-8 -*-
import yaml
yaml.warnings({'YAMLLoadWarning': False})
import pymysql
import datetime
import traceback
import pandas as pd
import numpy as np
import joblib
import os
import web
import logging
import json

from modelservice.file_1_image_score.utils.image_utils import *
import warnings
warnings.filterwarnings('ignore', category=FutureWarning)
logger = logging.getLogger('ImageScorePred')


class ImageScorer:
    """ 离线预测 """
    def POST(self, ):
        ret = self.Process()
        return ret

    def __init__(self, ):
        pass

    # 任务处理函数
    def Process(self, ):
        try:
            mgame_id_json = web.data()
            mgame_id_json = json.loads(mgame_id_json)
            self.mgame_id = mgame_id_json['mgame_id']
            print("[INFO-PREDING]:评分预测中...")
            image_score(self.mgame_id)
            print("[INFO-PREDING]:评分预测完毕！")
            print("[INFO-PREDING]:写入SQL中...")
            load_to_hive()
            print("[INFO-PREDING]:写入SQL完毕！")
        except Exception as e:
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logging.error("image score pred error! at {}".format(timestamp))
            logging.error(e)
            logging.error("\n" + traceback.format_exc())
            return json.dumps({"code": 500, "msg": str(e), "datetime": timestamp})

        ret = json.dumps({"code": 200, "msg": "success!", "data": "image score pred success!"})
        return ret


def Prob2Score(prob, basePoint=600, PDO=30):
    # 将概率转化成分数且为正整数  基础分为600
    y = np.log(prob / (1 - prob))
    result = basePoint + int(PDO / np.log(2) * (y))
    return (result)


def load_to_hive():
    # 将生成的csv文件加载到hive中
    run_dt = datetime.datetime.now().strftime('%Y-%m-%d')
    run_hour = datetime.datetime.now().strftime('%H')
    os.system("hadoop fs -rm -r /warehouse/tablespace/managed/hive/dws.db/dws_image_score_d/dt="+run_dt+"/hr="+run_hour)
    os.system("hadoop fs -mkdir /warehouse/tablespace/managed/hive/dws.db/dws_image_score_d/dt=" + run_dt)
    os.system("hadoop fs -mkdir /warehouse/tablespace/managed/hive/dws.db/dws_image_score_d/dt="+run_dt+"/hr="+run_hour)
    os.system("hadoop fs -put image_info_sorce.csv /warehouse/tablespace/managed/hive/dws.db/dws_image_score_d/dt="+run_dt+"/hr="+run_hour)
    os.system("beeline -u \"jdbc:hive2://bigdata-zk01.ch:2181,bigdata-zk02.ch:2181,bigdata-zk03.ch:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2\" -nhive -phive -e \"set hive.msck.path.validation=ignore;msck repair table dws.dws_image_score_d;\"")

def image_score(mgame_id):
    # 模型加载
    model_path = "./aimodel/"
    best_est_XGB = joblib.load(model_path + 'best_est_XGB_{}.pkl'.format(mgame_id))
    best_est_LGB = joblib.load(model_path + 'best_est_LGB_{}.pkl'.format(mgame_id))
    best_est_RF = joblib.load(model_path + 'best_est_RF_{}.pkl'.format(mgame_id))

    # 参数加载
    with open("./aimodel/cutwoe_{}.json".format(mgame_id), mode="r") as cw_f:
        cw = json.load(cw_f)

    # 数据获取
    image_info = etl_image_pred(mgame_id)
    image_info = image_info[image_info['image_run_date_amount'] > 1000]

    # 将无付费和无创角的成本由0改为无穷大
    # image_info['image_create_role_cost'].replace(0, float('inf'), inplace=True)
    # image_info['image_create_role_pay_cost'].replace(0, float('inf'), inplace=True)

    # 分箱
    select_feature = ['image_run_date_amount', 'image_create_role_pay_num',
                      'image_create_role_num', 'image_create_role_pay_sum',
                      'image_source_num', 'image_create_role_pay_rate',
                      'image_create_role_cost', 'image_create_role_pay_cost',
                      'image_valid_source_rate',
                      'image_pay_sum_ability', 'image_pay_num_ability',
                      'image_create_role_roi']
    # 数据转化
    image_info_change = image_info.copy()
    for fea in select_feature:
        if fea in cw.keys():
            image_info_change[fea] = change_woe(image_info_change[fea], cw[fea][0], cw[fea][1])

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
    # 合并媒体（广点通+头条）
    image_info['label_ids'] = image_info['label_ids'].str.replace(',', ';')
    # 数据导出
    image_info.to_csv('./image_info_sorce.csv', index=0, encoding='utf_8_sig', header=None)


if __name__ == '__main__':
    # 测试
    scorer_image = ImageScorer(1056)  # !!! 输入形式
    scorer_image.Process()
