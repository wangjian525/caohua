import pandas as pd
import numpy as np
import joblib
import warnings
warnings.filterwarnings('ignore')


def mr_model_1d():
    def active_time_transform(df):
        df['role_created_active_time'].fillna('"0-8,0","8-12,0","12-14,0","14-18,0","18-24,0"', inplace=True)
        df['role_created_active_time'] = df['role_created_active_time'].apply(lambda x: str(x)[1:-1])
        temp = df['role_created_active_time'].str.split(',', expand=True).iloc[:, [1, 3, 5, 7, 9]].rename(
            columns={1: 'active_0-8',
                     3: 'active_8-12', 5: 'active_12-14', 7: 'active_14-18', 9: 'active_18-24'})
        for i in range(len(temp.columns)):
            temp.iloc[:, i] = temp.iloc[:, i].apply(lambda x: str(x)[:-1])
        df = df.join(temp).drop(['role_created_active_time'], axis=1)
        return df

    def pay_grade_transform(df):
        df['pay_grade'].fillna('[0,0,0,0,0,0,0]', inplace=True)
        df['pay_grade'] = df['pay_grade'].apply(lambda x: str(x)[1:-1])
        temp = df['pay_grade'].str.split(',', expand=True).rename(columns={0: 'pay_grade_1',
                                                                           1: 'pay_grade_2', 2: 'pay_grade_3',
                                                                           3: 'pay_grade_4', 4: 'pay_grade_5',
                                                                           5: 'pay_grade_6', 6: 'pay_grade_7'})
        df = df.join(temp).drop(['pay_grade'], axis=1)
        return df

    # 数据加载
    role_info = pd.read_csv('./mr_role_sample_1.csv')
    # 数据清洗
    role_info = active_time_transform(role_info)
    role_info = pay_grade_transform(role_info)
    role_info['pay_num'].fillna(0, inplace=True)
    role_info['pay_sum'].fillna(0, inplace=True)
    role_info['role_created_active'] = role_info['role_created_active'].clip(0, 1)
    role_info['pay_rate'] = role_info['pay_num'] / (role_info['role_created_active'] + 1e-4)
    role_info['pay_avg'] = role_info['pay_sum'] / (role_info['pay_num'] + 1e-4)
    # 特征选择
    select_features = ['user_id', 'cp_server_no', 'cp_role_id', 'role_created_login_num',
                       'role_created_active', 'role_created_online',
                       'max_role_level', 'ip_num',
                       'pay_num', 'pay_sum', 'active_0-8', 'active_8-12', 'active_12-14',
                       'active_14-18', 'active_18-24', 'pay_grade_1', 'pay_grade_2',
                       'pay_grade_3', 'pay_grade_4', 'pay_grade_5', 'pay_grade_6',
                       'pay_rate', 'pay_avg']
    role_info = role_info[select_features]
    col_list = ['active_0-8', 'active_8-12', 'active_12-14', 'active_14-18', 'active_18-24', 'pay_grade_1',
                'pay_grade_2',
                'pay_grade_3', 'pay_grade_4', 'pay_grade_5', 'pay_grade_6']
    for col in col_list:
        role_info[col] = pd.to_numeric(role_info[col], errors='coerce')

    # 根据角色行为分类
    role_info_pay = role_info[role_info['pay_sum'] != 0]
    role_info_nopay = role_info[role_info['pay_sum'] == 0]
    role_info_nopay_online_n = role_info_nopay[
        (role_info_nopay['role_created_online'] <= 300) | (role_info_nopay['role_created_login_num'] < 3) | (
                    role_info_nopay['max_role_level'] <= 1)]
    role_info_nopay_online_y = role_info_nopay.loc[np.setdiff1d(role_info_nopay.index, role_info_nopay_online_n.index),
                               :]

    # 标记角户质量不付费不在线0;不付费在线1;付费2
    role_info_nopay_online_n['role_type'] = 0
    role_info_nopay_online_y['role_type'] = 1
    role_info_pay['role_type'] = 2

    # 模型加载
    lgb_r_1d = joblib.load('./lgb_r_1d.pkl')
    # 付费金额预测
    role_info_nopay_online_n['predict_30_pay'] = 0
    role_info_nopay_online_y['predict_30_pay'] = 0
    role_info_pay['predict_30_pay'] = np.expm1(
        lgb_r_1d.predict(role_info_pay.drop(['user_id', 'cp_server_no', 'role_type',
                                             'cp_role_id'], axis=1))) * 2.1
    # 数据输出
    role_info_predict = pd.concat([role_info_pay, role_info_nopay_online_n, role_info_nopay_online_y], axis=0)
    role_info_predict.to_csv('./role_info_predict.csv')


if __name__ == '__main__':
    mr_model_1d()


