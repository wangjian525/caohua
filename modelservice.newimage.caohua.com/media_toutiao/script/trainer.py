# -*- coding:utf-8 -*-
"""
   File Name：     trainer.py
   Description :   脚本：计划好坏的训练（头条）
   Author :        royce.mao
   date：          2021/6/15 10:42
"""

import pandas as pd
import gc
import lightgbm as lgb
pd.set_option("display.max_columns", None)
from sklearn.preprocessing import LabelEncoder
from tqdm.auto import tqdm
tqdm.pandas(desc="Processing!")
from sklearn.model_selection import train_test_split

from itertools import combinations
from tqdm import tqdm_notebook


def get_mutil_feature(data):
    cols = ['inventory_type', 'age', 'city', 'retargeting_tags_include', 'retargeting_tags_exclude', 'ac',
            'interest_categories',
            'action_scene', 'action_categories']
    for col in cols:
        if col in ['inventory_type', 'age']:
            data[col] = data[col].apply(lambda x: x if x == x else [])
            data = data.join(data[col].str.join('|').str.get_dummies().add_prefix(col + '_').reset_index(drop=True))
            data.drop(col, axis=1, inplace=True)

        else:
            data[col] = data[col].apply(lambda x: x if x == x else [])
            data[col] = data[col].apply(lambda x: [str(i) for i in x])
            data[col] = data[col].astype(str)
            le = LabelEncoder()
            data[col] = le.fit_transform(data[col])

    gc.collect()
    return data


def train(image_info, df, plan_create):
    """[训练]

    Args:
        df ([dataframe]): [采集数据：realtime_df返回的第一个dataframe]
        image ([dataframe]): [采集数据：realtime_df返回的第二个dataframe]
        plan ([dataframe]): [组合计划：FeaAssoc()类函数的返回]
        cfg ([配置类实例]): []
    """
    image_info.dropna(subset=['image_id'], inplace=True)
    image_info['image_id'] = image_info['image_id'].astype(int)
    plan_create = pd.merge(plan_create, image_info[['image_id', 'label_ids']].drop_duplicates(), on='image_id',
                        how='left')

    plan_create_train = plan_create.drop(
        ['budget', 'cpa_bid', 'ad_keywords', 'title_list', 'third_industry_id'],
        axis=1)
    plan_create_train['platform'] = plan_create_train['platform'].map({'[ANDROID]': 1, '[IOS]': 2})

    df['train_label'] = 1
    df = df[df['label'] != -1]

    plan_create_train['train_label'] = 0
    plan_create_train['label'] = -1
    df = df[df['create_time'] >= pd.datetime.now() - pd.DateOffset(180)]
    # print('df', df.shape)
    # print('plan_create_train', plan_create_train.shape)
    df = df.append(plan_create_train)

    df['create_date'] = pd.to_datetime(df['create_date'])
    df['ad_im_sort_id'] = df.groupby(['ad_account_id', 'image_id'])['create_time'].rank()
    df['ad_game_sort_id'] = df.groupby(['ad_account_id', 'game_id'])['create_time'].rank()
    df['im_ad_sort_id'] = df.groupby(['image_id', 'ad_account_id'])['create_time'].rank()

    df = get_mutil_feature(df)

    cat_cols = ['ad_account_id', 'game_id', 'schedule_time', 'delivery_range', 'flow_control_mode',
                'smart_bid_type', 'hide_if_converted', 'gender', 'location_type', 'launch_price',
                'android_osv', 'ios_osv', 'interest_action_mode', 'action_days', 'image_id', 'label_ids',
                'deep_bid_type']

    cat_cross = []
    for col in tqdm_notebook(combinations(cat_cols, 2)):
        df[str(col[0]) + '_' + str(col[1])] = df[col[0]].map(str) + '_' + df[col[1]].map(str)
        cat_cross.append(str(col[0]) + '_' + str(col[1]))

    for col in cat_cols + cat_cross:
        df[col] = df[col].astype(str)
        le = LabelEncoder()
        df[col] = le.fit_transform(df[col])

    # 训练
    train_data = df[df['train_label'] == 1]
    test_data = df[df['train_label'] == 0]

    train_data = train_data.drop(['train_label', 'create_time', 'create_date'], axis=1)
    test_data = test_data.drop(['train_label', 'create_time', 'create_date'], axis=1)
    target = train_data['label']
    features = train_data.drop(['label'], axis=1)

    X_val, x_test, Y_val, y_test = train_test_split(features, target, test_size=0.3)

    params = {
        "objective": "binary",
        "boosting_type": "gbdt",
        "learning_rate": 0.01,
        "max_depth": 8,
        "num_leaves": 55,
        "max_bin": 255,
        "min_data_in_leaf": 101,
        "min_child_samples": 15,
        "feature_fraction": 0.5,
        "bagging_fraction": 0.6,
        "bagging_freq": 20,
        "lambda_l1": 1e-05,
        "lambda_l2": 0,
        "min_split_gain": 0.0,
        "metric": "auc",
        "is_unbalance": True
    }

    train_data = lgb.Dataset(X_val, label=Y_val)
    val_data = lgb.Dataset(x_test, label=y_test, reference=train_data)
    model = lgb.train(params, train_data, num_boost_round=8000, early_stopping_rounds=100,
                    valid_sets=[train_data, val_data])

    
    # 预测
    features_test = test_data.drop(['label'], axis=1)
    plan_prob = model.predict(features_test)
    plan_create['prob'] = plan_prob

    return plan_create, plan_prob
