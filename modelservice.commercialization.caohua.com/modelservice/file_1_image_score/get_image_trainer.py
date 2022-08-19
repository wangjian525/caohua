# -*- coding:utf-8 -*-
import os
import web
import json
import time
import joblib
import logging
import datetime
import traceback
import pandas as pd
import numpy as np
from scipy import stats
import yaml
yaml.warnings({'YAMLLoadWarning': False})
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

from xgboost import XGBClassifier
from lightgbm import LGBMClassifier
from sklearn.model_selection import train_test_split, RandomizedSearchCV
from sklearn.ensemble import RandomForestClassifier

from modelservice.file_1_image_score.utils.image_utils import *
# import sys
# sys.path.append("..") 
import modelservice.yaml_conf as yaml_conf
logger = logging.getLogger('ImageScoreTrain')

confs = yaml_conf.get_config()  # !!! 一对一计划参数YAML全局配置

class ImageTrainer:
    """ 离线训练 """
    def POST(self, ):
        ret = self.main()
        return ret

    def __init__(self, ):
        pass
    
    def dataing(self, data_sta, data_end):
        """ 数据采集 """
        # self.data_sta = input('start:')
        # self.data_end = input('end:')
        self.data_sta = data_sta  # !!! 输入形式1
        self.data_end = data_end  # !!! 输入形式2

        try: time.strptime(self.data_sta, "%Y-%m-%d")
        except:print("训练数据起始日，日期格式不对！")
        try: time.strptime(self.data_end, "%Y-%m-%d")
        except:print("训练数据终止日，日期格式不对！")

        train_data = etl_image_train(start=self.data_sta, end=self.data_end, mgame=self.mgame_id)

        return train_data

    @staticmethod
    def processing_wash(data, conf):
        """ 预处理：清洗 """
        data = data[data['image_run_date_amount']>=500]
        data = data[data['image_create_role_cost']<=1000]
        data = data[~data['create_role_30_pay_sum'].isnull()]
        data['image_30_roi'] = data['create_role_30_pay_sum'] / data['image_run_date_amount']
        data['label'] = data['image_30_roi'].apply(lambda x:1 if x>=conf['upper'] else(0 if x<conf['lower'] else 2))  # !!! 打标签
        data = data[data['label']!=2]

        # data['image_create_role_cost'].replace(0, float('inf'), inplace=True)
        # data['image_create_role_pay_cost'].replace(0, float('inf'), inplace=True)

        return data
    
    @staticmethod
    def processing_bin(data, mgame_id):
        """ 预处理：分箱 """
        select_feature = ['image_run_date_amount', 'image_create_role_pay_num',
                          'image_create_role_num', 'image_create_role_pay_sum',
                          'image_source_num', 'image_create_role_pay_rate',
                          'image_create_role_cost', 'image_create_role_pay_cost',
                          'image_valid_source_rate','image_pay_sum_ability',
                          'image_pay_num_ability','image_create_role_roi']
        def mono_bin(Y, X, n = 20):
            ''' 函数：自动分箱 '''
            r = 0
            good=Y.sum()
            bad=Y.count() - good
            n = pd.value_counts(pd.qcut(X, n, duplicates='drop')).shape[0]
            while np.abs(r) < 1:
                d1 = pd.DataFrame({"X": X, "Y": Y, "Bucket": pd.qcut(X, n, duplicates='raise')})
                d2 = d1.groupby('Bucket', as_index = True)
                r, p = stats.spearmanr(d2.mean().X, d2.mean().Y)
                n = n - 1
            d3 = pd.DataFrame(d2.min().X, columns = ['min_' + X.name])
            d3['min_' + X.name] = d2.min().X
            d3['max_' + X.name] = d2.max().X
            d3[Y.name] = d2.sum().Y
            d3['total'] = d2.count().Y
            d3['badattr']=d3[Y.name]/bad
            d3['goodattr']=(d3['total']-d3[Y.name])/good
            d3['woe'] = np.log(d3['goodattr']/d3['badattr'])
            iv = ((d3['goodattr']-d3['badattr'])*d3['woe']).sum()
            d4 = (d3.sort_values(by = 'min_' + X.name)).reset_index(drop = True)

            cut = []
            cut.append(float('-inf'))
            for i in range(1, n+1):
                qua = X.quantile(i / (n+1))
                cut.append(round(qua, 4))
            cut.append(float('inf'))
            woe = list(d4['woe'].round(3))
            return cut, woe

        def replace_woe(d, cut, woe):
            ''' 函数：woe转换 '''
            list1=[]
            i=0
            while i < len(d):
                value = d.values[i]
                j = len(cut) - 2
                m = len(cut) - 2
                while j >= 0:
                    if value >= cut[j]:
                        j =- 1
                    else:
                        j -= 1
                        m -= 1
                list1.append(woe[m])
                i += 1
            return list1
        
        dict_save = {}
        for fea in select_feature:
            # 转化
            try:
                cutx, woex = mono_bin(data['label'], data[fea], n=10)
                if len(woex) == 1:  # !!! 只分1个箱：抛出异常
                    raise Exception("One Bin Pass...!")
                data[fea] = replace_woe(data[fea], cutx, woex)
            except Exception:
                continue  # !!! 异常处理（所有）：该特征不分箱
            dict_save[fea] = [cutx, woex]
        # 存储
        save_cutxwoex(dict_save, mgame_id)

        return data[select_feature+['label']]

    @staticmethod
    def training(data, mgame_id):
        """ 训练 """
        x_train, x_test, y_train, y_test = train_test_split(data.drop('label', axis=1), data['label'], test_size=0.01, random_state=42)
        # 模型1：随机森林
        RF = RandomForestClassifier()
        param_grid = {"n_estimators" : [9,18,27,36,100,150],
                      "max_depth" : [2,3,5,7,9],
                      "min_samples_leaf" : [2,4,6,8]}
        RF_random = RandomizedSearchCV(RF, param_distributions=param_grid, cv=5)
        RF_random.fit(x_train, y_train)
        best_est_RF = RF_random.best_estimator_

        # 模型2：XGB模型
        XGB = XGBClassifier(n_jobs=-1)
        param_grid = {'n_estimators' :[100,150,200,250,300],
                      "learning_rate" : [0.001,0.01,0.0001,0.05, 0.10 ],
                      "gamma"            : [ 0.0,0.1,0.2,0.3 ],
                      "colsample_bytree" : [0.5,0.7],
                      'max_depth': [3,4,6,8,10,12,15,20,25,30]}
        XGB_random = RandomizedSearchCV(XGB, param_distributions=param_grid, cv=5)
        XGB_random.fit(x_train,y_train)
        best_est_XGB = XGB_random.best_estimator_

        # 模型3：LGBM模型
        LGB = LGBMClassifier(boosting_type='gbdt', objective='binary', metric='auc', n_jobs=-1)
        param_grid = {'max_depth': [15, 20, 25, 30, 35],
                      'learning_rate': [0.01, 0.02, 0.05, 0.1, 0.15],
                      'feature_fraction': [0.6, 0.7, 0.8, 0.9, 0.95],
                      'bagging_fraction': [0.6, 0.7, 0.8, 0.9, 0.95],
                      'bagging_freq': [2, 4, 5, 6, 8],
                      'lambda_l1': [0, 0.1, 0.4, 0.5, 0.6],
                      'lambda_l2': [0, 10, 15, 35, 40],
                      'cat_smooth': [1, 10, 15, 20, 35]}
        LGB_random = RandomizedSearchCV(LGB, param_distributions=param_grid, cv=5)
        LGB_random.fit(x_train,y_train)
        best_est_LGB = LGB_random.best_estimator_

        joblib.dump(best_est_RF, './aimodel/best_est_RF_{}.pkl'.format(mgame_id))
        joblib.dump(best_est_XGB, './aimodel/best_est_XGB_{}.pkl'.format(mgame_id))
        joblib.dump(best_est_LGB, './aimodel/best_est_LGB_{}.pkl'.format(mgame_id))


    def main(self, ):
        try:
            mgame_id_json = web.data()
            mgame_id_json = json.loads(mgame_id_json)
            self.mgame_id = mgame_id_json['mgame_id']
            # 采集
            print("[INFO-IMAGING]:数据采集中...")
            self.train_data = self.dataing((datetime.datetime.now()+datetime.timedelta(days=-180)).strftime("%Y-%m-%d"), 
                                            datetime.datetime.now().strftime("%Y-%m-%d"))
            print("[INFO-IMAGING]:数据采集完毕！")
            # 清洗/标注/预处理
            print("[INFO-PROCESSING]:预处理中...")
            self.train_data = self.processing_wash(self.train_data, confs[str(self.mgame_id) + ','])
            self.train_data = self.processing_bin(self.train_data, self.mgame_id)
            print("[INFO-PROCESSING]:预处理完毕！")
            # 训练
            print("[INFO-TRAINING]:训练中...")
            self.training(self.train_data,self.mgame_id)
            print("[INFO-TRAINING]:训练完毕！")
        except Exception as e:
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logging.error("image score train error! at {}".format(timestamp))
            logging.error(e)
            logging.error("\n" + traceback.format_exc())
            return json.dumps({"code": 500, "msg": str(e), "datetime": timestamp})
        
        ret = json.dumps({"code": 200, "msg": "success!", "data": "image score train success!"})
        return ret


def save_cutxwoex(cw, mgame_id):
    """ 参数存储 """
    dir_param = "./aimodel/cutwoe_{}.json".format(mgame_id)
    if not os.path.exists(dir_param):
        os.mknod(dir_param)
    else:
        pass 
    cw_f = open(dir_param, mode="w")  # 可覆盖
    json.dump(cw, cw_f)


if __name__ == '__main__':
    # 测试
    trainer_image = ImageTrainer(1056)  # !!! 输入形式3
    trainer_image.main()
