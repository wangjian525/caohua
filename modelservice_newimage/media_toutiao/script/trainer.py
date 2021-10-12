# -*- coding:utf-8 -*-
"""
   File Name：     trainer.py
   Description :   脚本：计划好坏的训练（头条）
   Author :        royce.mao
   date：          2021/6/15 10:42
"""

import pandas as pd
import numpy as np
import joblib
pd.set_option("display.max_columns", None)
from impala.dbapi import connect
from impala.util import as_pandas
from sklearn.preprocessing import LabelEncoder, StandardScaler
from tqdm.auto import tqdm
tqdm.pandas(desc="Processing!")

from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier  # XGB树模型集成
from collections import Counter


def get_mutil_feature(data, cols):
    """ hotting处理 """
    for col in cols:
        data[col] = data[col].apply(lambda x: x if x == x else np.nan)
        data = data.join(data[col].str.join('|').str.get_dummies().add_prefix(col + '_').reset_index(drop=True))
        data.drop(col, axis=1, inplace=True)
    # gc.collect()
    return data

def _score(x, d):
    """ 为历史计划分配image_id相等且时间上最近的素材评分score """
    d = d[d['image_id'] == x['image_id']]
    if not len(d):
        return np.nan
    else:
        seri = (x['create_time'] - d['create_time']).abs()
        return d[seri == seri.min()]['image_score'].mean()


def train(df, image, plan, cfg):
    """[训练]

    Args:
        df ([dataframe]): [采集数据：realtime_df返回的第一个dataframe]
        image ([dataframe]): [采集数据：realtime_df返回的第二个dataframe]
        plan ([dataframe]): [组合计划：FeaAssoc()类函数的返回]
        cfg ([配置类实例]): []
    """
    # df = df[plan.columns]
    # assert df.columns == plan.columns, "Columns Mismatch!"
    # df与plan两张表的字段差集
    df['image_score'] = 0  ## 初始化
    plan['label'] = 0  ## 初始化
    cols = cfg.ENCODER_COLS + cfg.HOTTING_COLS + cfg.CONT_COLS + ['create_time', 'label']  ## 初始化

    df_mesh = plan[cols].append(df[cols])  ## 合并做特征工程

    df_mesh = get_mutil_feature(df_mesh, cfg.HOTTING_COLS)
    for col in list(set(cfg.ENCODER_COLS) - set(['image_id'])):
        le = LabelEncoder()
        df_mesh[col] = df_mesh[col].astype(str)
        df_mesh[col] = le.fit_transform(df_mesh[col])
        
    df = df_mesh.iloc[len(plan):]  ## 切分
    plan_after = df_mesh.iloc[:len(plan)].drop(['image_id', 'create_time', 'label'], axis=1)  ## 切分
    plan_after['image_score'] = plan_after['image_score'].astype(float)

    df.drop_duplicates(inplace=True)  # keep=first
    df.dropna(how='all', inplace=True)

    # 为计划分配score
    df['create_time'] = df['create_time'].apply(lambda x: x.date())
    df['image_score'] = df.progress_apply(lambda x: _score(x, image), axis=1)

    df = df.drop(['image_id', 'create_time'], axis=1)
    df.drop_duplicates(inplace=True)  # keep=first
    df.dropna(how='any', inplace=True)

    df.reset_index(drop=True, inplace=True)
    
    # 跑训练
    X = df.drop(['label'], axis=1)
    Y = df['label']
    X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size = 0.2, random_state = 2021, shuffle=True)

    xgb_model = XGBClassifier()
    xgb_model.fit(X_train, Y_train)

    plan_prob = xgb_model.predict_proba(plan_after)  ## 概率预测

    joblib.dump(xgb_model, cfg.DTC_PLAN_PATH)

    return plan_prob
