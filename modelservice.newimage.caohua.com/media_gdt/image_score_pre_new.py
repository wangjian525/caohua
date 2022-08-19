# -*- coding:utf-8 -*-
"""
   File Name：     image_pre_score_new.py
   Description :   新素材评分 - 事前
   Author :        royce.mao
   date：          2021/7/5 20:34
"""

from impala.dbapi import connect
from impala.util import as_pandas
import pandas as pd
import numpy as np
import joblib
import os
import logging
import json
import os
# os.chdir(os.path.dirname(__file__))

import warnings
warnings.filterwarnings('ignore')
logger = logging.getLogger('ImageScore')

from config import cur_config as cfg

#
# 打包接口
#
class GDTImageScore:
    def __init__(self, ):
        self.new_image_score = NewImageScore()

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
        self.new_image_score()
        self.new_image_score.load_to_hive()
        ret = json.dumps({"code": 200, "msg": "success!", "data": "hive load success123"})
        return ret


class NewImageScore(object):
    """ 新素材评分类 """  ## 老素材评分也可用，但是老素材有上线指标参考，采用老素材评分方案更准确
    def __init__(self, ):
        pass

    @staticmethod
    def get_new_images(mode):
        """ image新素材提取 """
        # score为null时label一定是0，但label为0时score不一定为null；
        conn = connect(host='192.168.0.97', port=10000, auth_mechanism='PLAIN', user='hadoop', database='default')
        cursor = conn.cursor()
        sql_engine = 'set hive.execution.engine=tez'
        if mode == '末日':
            sql = 'select image_id,image_name,label_ids,game_ids,score,label from test.gdt_new_image_test'  ## 未测过新的素材查询
        if mode == '帝国':
            sql = 'select image_id,image_name,label_ids,game_ids,score,label,tdate from test.gdt_new_image_test_dg'  ## 未测过新的素材查询
        cursor.execute(sql_engine)
        cursor.execute(sql)
        result = as_pandas(cursor)

        # 关闭链接
        cursor.close()
        conn.close()
        
        return result

    @staticmethod
    def quantization(df_, mapping):
        """ 根据mapping编码量化2字段 """
        # 具体数学细节与实现见Image.ipynb
        for col, map in zip(['label_ids', 'game_ids'], mapping):
            param = 0
            for id in df_[col].split(','):
                if id != 'null' and id in map.keys():
                    param += map[id]
                else:
                    param += 0
            df_[col + '_value'] = param
        return df_

    def __call__(self, mode):
        """[类评分函数]

        Args:
            ckpt ([score评分的xgb模型地址]): [str]
            new_image_info ([dataframe]): [字段详情为：['image_id', 'label_ids', 'game_ids']]
        """
        # 新素材加载
        # new_image_info = pd.read_csv(new_image_path)
        new_image_info = self.get_new_images(mode)

        # 模型加载
        best_score_xgb = joblib.load(cfg.XGB_PRE_SCORE_PATH)

        # 多标签统一逗号分隔
        new_image_info['label_ids'] = new_image_info['label_ids'].str.replace('\"', '')
        new_image_info['label_ids'] = new_image_info['label_ids'].str.replace(';', ',')
        
        new_image_info['game_ids'] = new_image_info['game_ids'].str.replace('\"', '')
        new_image_info['game_ids'] = new_image_info['game_ids'].str.replace(';', ',')

        # mapping
        label_ids_mapping = cfg.LABEL_IDS_MAPPING
        game_ids_mapping = cfg.GAME_IDS_MAPPING

        # 'label_ids'与'game_ids'的量化
        new_image_info = new_image_info.apply(lambda x: self.quantization(x, (label_ids_mapping, game_ids_mapping)), axis=1)
        a = new_image_info[['label_ids_value', 'game_ids_value']]
        a.columns = ['label_ids', 'game_ids']
        # 预测
        score = best_score_xgb.predict(a)
        score = np.expand_dims(score, axis=-1)

        new_image_info['score'] = score
        new_image_info = new_image_info[['image_id', 'image_name', 'label_ids', 'game_ids', 'score', 'label']]

        # 新素材评分导出 [['image_id', 'label_ids', 'game_ids', 'score']]
        # new_image_info.to_csv('./new_image.csv', index=0, encoding='utf_8_sig',header=None)  ## label暂未实时修正的结果

        return new_image_info


if __name__ == "__main__":
    # todo:脚本测试通过!
    new_image_score = NewImageScore()  ## 实例
    new_image = new_image_score()  ## label==0未测新的新素材事前评分（可能滚动覆盖多次）
    new_image_score.load_to_hive()  ## 加载至测新表test.new_image_test
