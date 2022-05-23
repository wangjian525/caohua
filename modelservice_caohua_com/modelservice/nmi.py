import web
import time
import logging
import json
import pandas as pd
import numpy as np
import math
import datetime
from sklearn import metrics

logger = logging.getLogger('CalculateNmi')


#
# 打包接口
#
class CalculateNmi:
    def __init__(self, ):
        self.start_time = datetime.date.today()  ## 服务启动时间
        # self.df = self.GetData()  ## 提取一次数据

    def GET(self):
        # 处理POST请求
        logging.info("do CalculateNmi service")
        timestamp = "%d" % int(round(time.time() * 1000))
        return json.dumps({"code": 500, "msg": "Not allow GET request!", "timestamp": timestamp, "data": ""})

    def POST(self):
        # 处理POST请求
        logging.info("do service")
        try:
            ret = self.Process()
            logging.info(ret)
            return ret
        except Exception as e:
            logging.error(e)
            timestamp = "%d" % int(round(time.time() * 1000))
            ret = json.dumps({"code": 500, "msg": str(e), "timestamp": timestamp})
            return ret

    # 任务处理函数
    def Process(self):
        data = web.data()
        logging.info("Pack post==> %s" % data)

        json_packet = json.loads(data)
        fea_columns = json_packet['feature']
        label = json_packet['label'][0]

        json_data = calculate_nmi(json_packet, fea_columns, label)
        timestamp = "%d" % int(round(time.time() * 1000))
        ret = json.dumps({"code": 200, "msg": "success!", "timestamp": timestamp, "data": json_data})
        return ret


def NMI(A, B):
    # 样本点数
    total = len(A)
    A_ids = set(A)
    B_ids = set(B)
    # 互信息计算
    MI = 0
    eps = 1.4e-45
    for idA in A_ids:
        for idB in B_ids:
            idAOccur = np.where(A == idA)
            idBOccur = np.where(B == idB)
            idABOccur = np.intersect1d(idAOccur, idBOccur)
            px = 1.0 * len(idAOccur[0]) / total
            py = 1.0 * len(idBOccur[0]) / total
            pxy = 1.0 * len(idABOccur) / total
            MI = MI + pxy * math.log(pxy / (px * py) + eps, 2)
    # 标准化互信息
    Hx = 0
    for idA in A_ids:
        idAOccurCount = 1.0 * len(np.where(A == idA)[0])
        Hx = Hx - (idAOccurCount / total) * math.log(idAOccurCount / total + eps, 2)
    Hy = 0
    for idB in B_ids:
        idBOccurCount = 1.0 * len(np.where(B == idB)[0])
        Hy = Hy - (idBOccurCount / total) * math.log(idBOccurCount / total + eps, 2)
    MIhat = 2.0 * MI / (Hx + Hy)
    return MIhat


def calculate_nmi(jsondata, fea_columns, label):
    df = pd.DataFrame()
    for i in range(len(jsondata['data']['tableData'])):
        df = df.append(pd.DataFrame([jsondata['data']['tableData'][i]]))

    rusult = []
    for col in fea_columns:
        nmi = {}
        nmi['featureName'] = col
        if df[[col, label]].corr().shape[0] == 2:
            if round(df[[col, label]].corr().iloc[0, 1], 4) == round(df[[col, label]].corr().iloc[0, 1], 4):
                nmi['relative_value'] = round(df[[col, label]].corr().iloc[0, 1], 4)    # 相关系数
            else:
                nmi['relative_value'] = 0
        else:
            nmi['relative_value'] = round(metrics.normalized_mutual_info_score(df[col], df[label]), 4)   #  互信息
        rusult.append(nmi)
    rusult = sorted(rusult, key=lambda i: i['relative_value'], reverse=True)
    return rusult


