# -*- coding:utf-8 -*-
"""
   File Name：     weight.py
   Description :   新素材评分 - ID的离散序列字段集，先embedding后矩阵相乘权值向量，目标为已知的score向量。矩阵推算权值向量编码的过程。
   Author :        royce.mao
   date：          2021/6/4
"""

from tqdm import tqdm
import pandas as pd
import numpy as np


def weights_calcul(df, LABEL='game_ids'):
    """[根据game_ids与score的单因素关系，分配权重值]

    Args:
        df ([dataframe]): [拼接后的DF表]
    Return: 
        mapping ([dict]): [{game_id:weight}的权重映射]
    """
    # game_id取值的embedding矩阵
    uniq_game_ids = ','.join(df[LABEL].tolist())
    uniq_ids_list = uniq_game_ids.split(',')
    game_ids_unique = np.unique(uniq_ids_list).tolist()  # game_id去重

    cols = len(game_ids_unique)
    rows = len(df)

    game_ids_matrix = np.zeros((rows, cols))  # multi-hotting（embedding）矩阵

    # embedding矩阵赋值
    for game_ids_sam, i in zip(df[LABEL], range(rows)):
        # 每行的game_ids，可能有1个或多个
        for game_id in game_ids_sam.split(','):
            # 赋值
            row_ind = i
            col_ind = game_ids_unique.index(game_id)
            game_ids_matrix[row_ind][col_ind] = 1

    # 矩阵运算求解权重向量
    score = np.transpose(df['score'].values)
    weights = np.linalg.lstsq(game_ids_matrix, score, rcond=-1)[0].tolist()  # 超定线性方程组AX=b的最小二乘法求解X，以最小化2范数\ b - A x \^2

    # 权值映射mapping
    assert len(weights) == cols, "game_ids种类数量与权值向量长度不匹配！"
    mapping = dict(zip(game_ids_unique, weights))

    return mapping


def weights_assign(df, mapping, LABEL='game_ids'):
    """[根据上面获取的权重向量，为指定列赋权值]

    Args:
        df ([dataframe]): [拼接后的DF表]
        mapping ([dict]): [{game_id:weight}的权重映射]
    """
    for i, row_goods in enumerate(tqdm(df[LABEL])):
        param = 0
        for row_good in row_goods.split(','):
            if row_good != 'null':
                param += mapping[row_good]

        df.loc[i, [LABEL]] = [param]

    return df