# -*- coding:utf-8 -*-
"""
   File Name：     reforcast.py
   Description :   函数：回款预测实现（模型 - 30日、60日、870日、900日）
   Author :        royce.mao
   date：          2022/4/14 10:59 
"""
import pandas as pd
import numpy as np
from math import e
import matplotlib.pyplot as plt
import os
import web
import time
import json
import random
import logging
import datetime
np.set_printoptions(suppress=True)
from scipy.optimize import curve_fit


mapp_hist = {'retain1':1, 'retain2':2, 'retain3':3, 'retain4':4, 'retain5':5, 'retain6':6, 'retain7':7, 'retain8':8, 'retain9':9, 'retain10':10, 
            'retain11':11, 'retain12':12, 'retain13':13, 'retain14':14, 'retain15':15, 'retain16':16, 'retain17':17, 'retain18':18, 'retain19':19, 'retain20':20, 
            'retain21':21, 'retain22':22, 'retain23':23, 'retain24':24, 'retain25':25, 'retain26':26, 'retain27':27, 'retain28':28, 'retain29':29, 'retain30':30, 
            'retain31':60, 'retain32':90, 'retain33':120, 'retain34':150, 'retain35':180, 'retain36':210, 'retain37':240, 'retain38':270, 'retain39':300, 'retain40':330}

class ReturnForcast():
    """ 回款预测类 """
    def __init__(self, ) -> str:
        pass

    def GET(self):
        # 处理GET请求
        logging.info("do LifeReturnForcast Get service...")
        timestamp = "%d" % int(round(time.time() * 1000))
        return json.dumps({"data":{}, "code": 500, "msg": "Not allow GET request!", "timestamp": timestamp})

    def POST(self):
        # 处理POST请求
        logging.info("Doing LifeReturnForcast Post Service...")
        try:
            ret = self.Process()
            logging.info(ret)
            return ret
        except Exception as e:
            logging.error(e)
            timestamp = "%d" % int(round(time.time() * 1000))
            ret = json.dumps({"data":{}, "code": 500, "msg": str(e), "timestamp": timestamp})
            return ret

    def Process(self, ):
        # 读取
        df_dict_json = web.data()
        df_dict_json = json.loads(df_dict_json)

        # 数据
        dtype = df_dict_json['dateType']  # int (1：自然日 2：标准日)
        data1 = df_dict_json['data']  # list of dict (维度回款数据retain 按日或月)
        mapp = df_dict_json['index'] # dict (retainN与横坐标X的映射)
        data2 = df_dict_json['predictAuxiliaryData'] # list of dict (历史回款数据retain 按月)
        group = df_dict_json['groupNames'] # !!! list (游戏组名)

        # 清洗（字段）
        data1 = pd.DataFrame(data1)
        data1 = data1.drop(['peopleNum', 'cost'], axis=1)  # Drop列
        data1.dropna(axis=1, inplace=True)
        data2 = pd.DataFrame(data2)
        data2 = data2.drop(['dt', 'amount', 'peopleNum', 'cost'], axis=1)  # Drop列
        data2.dropna(axis=1, inplace=True)

        # 清洗（文件）
        imgs = os.listdir("./img")
        for img in imgs:
            if img.endswith('.jpg'):
                os.remove(os.path.join("./img", img))

        # 算法
        print("算法...")
        rule = 20
        out = self.main(data1, data2, mapp, rule, len(data1.columns)-1, group, dtype)  # rule_len自己定义、pred_len等于data列长
        ret = json.dumps({"data":out, "code": 200, "msg": "LifeReturnForcast is success!"})
        print("[INFO] Finished!")
        return ret

    @staticmethod
    def data_prepare(data1, data2, mapp):
        '''
        静态: 数据处理（计算衰减率）
        Args:
            data ([dataframe]): [N x [amount,rate*26]]

        '''
        data1.columns = [mapp[a] if a in mapp.keys() else a for a in data1.columns]  # 列名转数字映射
        data2.columns = [mapp_hist[a] for a in data2.columns]  # 列名转数字映射
        # TODO：嵌套json值过滤
        data1 = data1.applymap(lambda x:x['retain'] if isinstance(x, dict) else x)  # 理想是递增
        data2 = data2.applymap(lambda x:x['increase'] if isinstance(x, dict) else x) # 理想是非负
        # TODO：data1非递增直接置0（数据没跑完）
        data_arm = data1.copy()
        for index, row in data1.iterrows():
            try:
                # TODO:规则纪录点（月）
                # if len(row['dt'].split(" ")) == 1:
                #     ind = np.argwhere(row[2:].values - row[1:-1].values < 0).min()+2  # ind代表递减开始的索引
                #     if row[ind] != 0:
                #         row[ind:] = 0
                #     elif row[ind-1] - row[ind-2] < row[ind-2] * 0.2:  # 当期未跑完（0.2增量的容忍度）
                #         row[ind-1:] = 0
                # TODO:精确纪录点（日）
                # else:
                ind = position(row)
                ind = 5 if ind < 5 else ind  # 保证至少3条纪录用于拟合
                row[ind:] = 0
            except Exception as e:
                # print(e)
                pass  # 数据完整
            finally:
                data1.iloc[index] = row
        data1 = data1[data1.columns[1:]]   # 过滤dt列
        # TODO：data2负数直接置0（数据没跑完）
        data2[data2 < 0] = 0

        data1.replace(0, np.nan, inplace=True)
        data2.replace(0, np.nan, inplace=True)
        data2.dropna(axis=1, how='all', inplace=True) 
        data1_ = data1.copy()

        for col in data1_.columns[2:]:  # index_0:amount、index_1:首月回款roi
            data1_[col] = data1_[col] / data1_[mapp['retain1']]
        return data1, data1_, data2, data_arm

    @staticmethod
    def func_now(data_real, data_hist, rule_len):
        '''
        静态: 数据拟合（幂函数拟合+规则确定a、b、c值）
        Args:
            data ([dataframe]): [N x [amount,rate*26], 其中data计算过衰减率]

        '''
        def power_batch(series, popt_hist, rule_len):
            ''' 幂函数（拟合） '''
            def power(x, a, b):
                ''' 幂函数 '''
                # return a * np.exp(b*np.log(x) + c)
                # return a * np.exp(b * x) + c
                # return a * x**b
                # return a * x**b + c
                return a * x**b
                
            series.dropna(inplace=True)
            ydata = series.tolist()
            xdata = series.index.tolist()
            xdata = list(map(lambda x:int(x), xdata))
            # xdata = list(map(lambda x:int(''.join(list(filter(str.isdigit, mapp[x])))), xdata))  # Retain1、Retain2、Retain360转整型的1、2、360
            assert len(xdata) == len(ydata), "拟合数据对不齐！"
            # TODO：问题1：分段
            d_value = [a - b for a ,b in zip(xdata[1:], xdata[:-1])]
            if len(set(d_value)) == 1:
                xdata_ = xdata.copy()
                ydata_ = ydata.copy()
            else:
                ind = next(i for i,x in enumerate(d_value) if x > 1)
                xdata_ = xdata[ind:]
                ydata_ = ydata[ind:]
            # TODO：选择1：直接拟合（标记0）
            if len(xdata_) > rule_len:
                popt, _ = curve_fit(power, xdata_, ydata_, maxfev=50000)
                sign = 0
                a, b = popt[0], popt[1] 
            # TODO：选择2：增速拟合（标记1）
            else:
                sign = 1
                a, b = (popt_hist[0][0], popt_hist[1][0]), (popt_hist[0][1], popt_hist[1][1])

            return pd.Series({'sign': sign, 'a': a, 'b': b})
        
        def fit(dat, hist, rule_len):
            ''' 拟合 '''
            def power(x, a, b):
                ''' 幂函数 '''
                # return a * np.exp(b*np.log(x) + c)
                # return a * np.exp(b * x) + c
                # return a * x**b
                # return a * x**b + c
                return a * x**b
            part1 = dat[dat.columns[:2].tolist()]  # 消耗 + 首日/月ROI
            part2 = dat[dat.columns[2:].tolist()]  # 每日/月ROI的衰减因子
            part1.columns = ['amount', 'roi_1st']
            # part1['std'] = part2.std(axis=1)
            # TODO:一次性拟合历史roi增速（分段）
            xdata = np.asarray(hist.columns).astype(int)
            ydata = np.mean(hist, axis=0).values
            popt1, _ = curve_fit(power, xdata[:30], ydata[:30], maxfev=50000)
            popt2, _ = curve_fit(power, xdata[30:], ydata[30:], maxfev=50000)
            plot_model(power, popt1, xdata[:30], ydata[:30])
            plot_model(power, popt2, xdata[30:], ydata[30:])
            # TODO:个性化拟合已跑roi数据
            part1 = part1.join(part2.apply(lambda x: power_batch(x, [popt1, popt2], rule_len), axis=1))
            # part1['adj'] = part1['a'] / part1['roi_1st']
            # part1.reset_index(drop=True, inplace=True)
            return part1[['sign','a','b']].values
        
        # 调整值、首月、均方差（标准差）
        assert rule_len > 3, '已跑历史序列规则值太小'
        not_nan_count = data_real.count(axis=1)  # 每行非NaN数量
        data_real = data_real.iloc[:, :max(not_nan_count)]  # 批次真实的最大长度
        labc = fit(data_real, data_hist, rule_len)  # ['amount','roi_1st','sign','a','b','c','adj'] [消耗、首日回款率、后续回款率衰减因子的标准差、拟合a值、拟合b值、拟合c值、调整系数]
        # abc = []
        # for index, row in data.iterrows():
        #     row.dropna(inplace=True)
        #     roi_1st = row.iloc[1]
        #     row = row.iloc[2:]
        #     # TODO:注释-已跑时长10个时间节点及以上
        #     if len(row) >= rule_len-1:
        #         abc += [data_attr.loc[index, ['a','b','c']].values.tolist()]
        #     # TODO:注释-已跑时长2-10个时间节点（方案二：历史调整系数）
        #     elif len(row) >= 1:
        #         adj, b, c = data_attr[data_attr['std'] == min(data_attr['std'], key=lambda x:abs(x-row.std()))][['adj','b','c']].values[0]
        #         abc += [[adj * roi_1st, b, c]]
        #     # TODO:注释-已跑时长只有1个时间节点，且最近邻阈值条件满足（方案二：历史调整系数）
        #     elif min(data_attr['roi_1st'], key=lambda x:abs(x-roi_1st)) <= 0.05:
        #         adj, b, c = data_attr[data_attr['roi_1st'] == min(data_attr['roi_1st'], key=lambda x:abs(x-roi_1st))][['adj','b','c']].values[0]
        #         abc += [[adj * roi_1st, b, c]]
        #     # TODO:注释-已跑时长只有1个时间节点，且最近邻阈值条件不满足（方案二：历史调整系数）
        #     else:
        #         adj, b, c = data_attr['adj'].mode(), data_attr['b'].mode(), data_attr['c'].mode()
        #         abc += [[adj * roi_1st, b, c]]

        return labc

    @staticmethod
    def inference(lab, data, data_, group, pred_len, data_arm):
        '''
        静态: 推理预测
        Args:
            labc ([list]): [[标记, a值, b值, c值] 拟合标记加拟合的a、b、c系数]
            data ([dataframe]): [N x [amount,rate*30], 原始数据表]
            data_ ([dataframe]): [N x [amount,rate*30], 衰减数据表]
        '''
        def power(x, a, b):
            ''' 幂函数 '''
            return a * x**b
            # return a * x**b + c
            # return a * np.exp(b*np.log(x) + c)
        def match(group):
            ''' 匹配 '''
            string = ','.join(group)
            games = list(filter(lambda x: x in string, ['末日','帝国','坦克','金牌','东家']))  # 游戏组名：游戏关键词
            plats = list(filter(lambda x: x in string, ['安卓','IOS','ios']))  # 游戏组名：平台关键词
            return games, plats
        
        game, plat = match(group)  # !!! 区分游戏、平台

        values = []
        for (l, a, b), (index, row), (index_, row_), (index_arm, row_arm) in zip(lab, data.iterrows(), data_.iterrows(), data_arm.iterrows()):
            row.dropna(inplace=True)
            row_.dropna(inplace=True)
            prc = int(row.index[-1]) # 最近列索引
            pre = row.iloc[-1].item() if prc != 45 else row.iloc[-2].item()  # 最近值
            value = []
            pre_45 = 0  # 870日模型初参
            if l == 0:  # TODO:0标记直接衰减值 * 首日roi
                for x in range(len(row_), pred_len):
                    x = int(data_.columns[x])
                    # x = int(''.join(list(filter(str.isdigit, mapp[data.columns[x]]))))
                    # x = np.log(x)  # 对数平滑
                    # value += [(e ** power(x, a, b, c)) * row.iloc[1]]  # 指数还原
                    value += [power(x, a, b) * row_.iloc[1]]
            elif l == 1:  # TODO:1标记递归增速 + 上节点roi
                for x in range(len(row), pred_len):
                    x = int(data.columns[x])
                    v_now = np.float64(row_arm[x])
                    if x < 60:
                        # TODO:30天内日拟合累计计算
                        for i in range(prc, x):
                            pre += power(i, a[0], b[0])
                        pre = v_now if pre < v_now else pre  # 已有真实比对
                        value += [pre]
                        prc = x
                        if x == 30:
                            pre_30 = pre.copy()
                        if x == 45:
                            pre_45 = pre.copy()
                            pre = pre_30 if int(data.columns[pred_len-1]) != 60 else pre  # 870日模型特例处理
                    else:
                        # TODO:30天外30天间隔拟合赋值
                        day_ = 360 if '安卓' not in plat else 180  # 静态节点
                        day = int(data.columns[len(row)+1]) if (len(list(set(game) & set(['末日','帝国'])))==0 or ('末日' in game and 'ios' in plat)) else day_  # 动态节点
                        if x >= day:
                            rev = revise_decay(power(x, a[1], b[1]), x, game, plat)
                        else:
                            rev = power(x, a[1], b[1])
                        pre = pre + rev if pre + rev >= pre_45 else (pre_30 + pre_45)/2 + rev  # 870日模型特例处理
                        value += [pre]
                        prc = x
            values += [value]
            
        return values

    def main(self, df1, df2, mapp, rule_len, pred_len, group, dtype):
        """ 
        主函数：需要区分平台、游戏、小组，采集回款数据作为输入
        Args:
             df1 ([dataframe]): [N x [amount, rate*N], 待预测回款数据表]
             df2 ([dataframe]): [N x [amount, rate*40], 历史的回款数据表]
        """
        rate = {1:1, 2:0.6}  # 系数：标准日/自然日
        # assert rule_len + 1 <= len(df.columns), "规则长度小于最长已跑天数/月数"
        assert pred_len >= len(df1.columns) - 1, "预测长度大于最长已跑天数/月数"
        print("衰减...")
        df1, df1_, df2_, df_arm = self.data_prepare(df1, df2, mapp)  # df1计算衰减率（原表/衰减表）、df2做基本处理
        print("拟合...")
        lab = self.func_now(df1_, df2_, rule_len)  # 规则+拟合：完整的a、b值
        print("推理...")
        vals = self.inference(lab, df1, df1_, group, pred_len, df_arm)  # 推理：预测值（计算区分自然/标准）
        print("补全...")
        # 预测值补全
        for (index, row), val in zip(df1.iterrows(), vals):
            row.dropna(inplace=True)
            a = row.tolist() + val
            df1.iloc[index] = a
        # 自然/标准换算
        df1[df1.columns[1:]] = df1[df1.columns[1:]] / rate[dtype]

        # TODO：计算汇总
        print("汇总...")
        # df.to_csv('./data_pred.csv', index=0)  # 保存本地
        mapp = dict(zip(mapp.values(), mapp.keys()))
        out = {}
        for col in df1.columns[1:]:
            out[mapp[col]] = (df1[col] * df1['amount']).sum() / df1['amount'].sum()

        # 保存：本地csv    
        # out = pd.DataFrame.from_dict(out, orient='index').T
        # print(out)
        return out


def position(row):
    ''' 定位已跑完索引 '''
    dicDate = {1:"31", 2:"28", 3:"31", 4:"30", 5:"31",
               6:"30", 7:"31", 8:"31", 9:"30", 10:"31",
               11:"30", 12:"31"}
    
    date_now = datetime.datetime.strptime(datetime.date.today().strftime("%Y-%m-%d"), '%Y-%m-%d')  # 当前日期
    try:
        date_start = datetime.datetime.strptime(row['dt'].split(" ")[0], '%Y-%m-%d')  # 日下钻
    except Exception as e:
        month = int(row['dt'].split("-")[-1])
        date_start = datetime.datetime.strptime(row['dt'] + '-' + dicDate[month], '%Y-%m-%d')  # 月下钻

    lands = list(filter(lambda x: x >= (date_now - date_start).days, row.index[2:]))
    if len(lands) > 0:
        return row.index.tolist().index(min(lands))
    else:
        return len(row)


# !!! 衰减个性化（安卓180天、IOS360天开始）
def revise_decay(base, x, game, plat):
    ''' 施加增速衰减 '''
    if len(game) == 1 and game[0] == '帝国':
        return base * np.e ** ((-300) / (800-x))  # 自然指数衰减
    elif len(game) == 1 and plat[0] != '安卓' and game[0] == '末日':
        return base * np.e ** ((-500) / (800-x))  # 自然指数衰减
    else:
        return base * np.e ** ((-300) / (1200-x))  # 自然指数衰减

# !!! 回款计算器（随时更新）
def revise_paystd():
    ''' 施加标准框定 '''
    # TODO:待定
    return 0


def cal_R_square(func, popt, xdata, ydata):
    """ 拟合度R^2的计算评估 """
    calc_ydata = [func(i, popt[0], popt[1]) for i in xdata]
    res_ydata  = np.array(ydata) - np.array(calc_ydata)
    ss_res     = np.sum(res_ydata**2)
    ss_tot     = np.sum((ydata - np.mean(ydata))**2)
    r_squared  = 1 - (ss_res / ss_tot)
    return r_squared


def plot_model(func, popt, xdata, ydata):
    """ 可视化 """
    calc_ydata = [func(i, popt[0], popt[1]) for i in xdata]
    plt.figure()
    plt.plot(xdata, ydata,'ko',label='Original Data')
    plt.plot(xdata, calc_ydata,'r',label='Fitted Curve')
    plt.legend()
    plt.savefig('./img/{}.jpg'.format(random.randint(1, 31)))
    plt.close()


if __name__ == "__main__":
    # 调用流程关系
    # from pycallgraph import PyCallGraph
    # from pycallgraph.output import GraphvizOutput
    # with PyCallGraph(output=GraphvizOutput()):
    # 读取
    with open("./data.json", 'r') as load_f:
        df_dict = json.load(load_f)
    
    # 数据
    dtype = df_dict['dateType']  # int (1：自然日 2：标准日)
    data1 = df_dict['data']  # list of dict (维度回款数据retain 按日或月)
    mapp = df_dict['index'] # dict (retainN与横坐标X的映射)
    data2 = df_dict['predictAuxiliaryData'] # list of dict (历史回款数据retain 按月)
    group = df_dict['groupNames'] # !!! list (游戏组名)

    # 清洗（字段）
    data1 = pd.DataFrame(data1)
    data1 = data1.drop(['peopleNum', 'cost'], axis=1)  # Drop列
    data1.dropna(axis=1, inplace=True)
    data2 = pd.DataFrame(data2)
    data2 = data2.drop(['dt', 'amount', 'peopleNum', 'cost'], axis=1)  # Drop列
    data2.dropna(axis=1, inplace=True)

    # 清洗（文件）
    imgs = os.listdir("./img")
    for img in imgs:
        if img.endswith('.jpg'):
            os.remove(os.path.join("./img", img))

    # 算法
    RF = ReturnForcast()
    rule = 20
    RF.main(data1, data2, mapp, rule, len(data1.columns)-1, group, dtype)  # rule_len自己定义、pred_len等于data列长
