import web
import time
import logging
import json
import numpy as np
from scipy.optimize import curve_fit

logger = logging.getLogger('CurveFit')


#
# 打包接口
#
class CurveFit:
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
        x = json_packet['X']
        y = json_packet['Y']
        x_, y_, param, deviation = mycurvefit(x, y)
        json_data = {"X": x_, "Y": y_, "param": param, "deviation": deviation}
        timestamp = "%d" % int(round(time.time() * 1000))
        ret = json.dumps({"code": 200, "msg": "success!", "timestamp": timestamp, "data": json_data})
        return ret


def getError(y_predict, y_data):
    y_predict = np.array(y_predict)
    y_data = np.array(y_data)
    n = y_data.size
    # SSE为和方差
    SSE = ((y_data - y_predict) ** 2).sum()
    # MSE为均方差
    MSE = SSE / n
    # RMSE为均方根,越接近0，拟合效果越好
    RMSE = np.sqrt(MSE)
    # 求R方，0<=R<=1，越靠近1,拟合效果越好
    u = y_data.mean()
    SST = ((y_data - u) ** 2).sum()
    SSR = SST - SSE
    R_square = SSR / SST
    return SSE, MSE, RMSE, R_square


def func1(x, a, b):
    return a*x**b


def func2(x, y30, y60):
    return y30 - (x / 30.0 - 1) * (y30 - y60)


def func3(x, yy60):
    return yy60 * (0.99**(x - 60))


# 拟合曲线
def mycurvefit(xdata, ydata):
    if len(xdata) != len(ydata):
        raise Exception("传入的数据个数不对！")
    popt, pcov = curve_fit(func1, xdata, ydata, maxfev=5000)
    y30 = func1(30, popt[0], popt[1])
    y60 = func1(60, popt[0], popt[1])
    for i in range(len(xdata)):
        if xdata[i] == 30:
            y30 = ydata[i]
        elif xdata[i] == 00:
            y60 = ydata[i]
    yy60 = func2(60, y30, y60)
    x_ = list(range(2, 360 + 1))
    y_ = [0 for _ in range(len(x_))]
    for i in range(len(x_)):
        if x_[i] <= 30:
            y_[i] = func1(x_[i], popt[0], popt[1])
        elif (x_[i] > 30) and (x_[i] <= 60):
            y_[i] = func2(x_[i], y30, y60)
        elif (x_[i] > 60) and (x_[i] <= 90):
            y_[i] = func3(x_[i], yy60)
        elif x_[i] > 90:
            y_[i] = func3(x_[i], yy60)
    sse, mse, rmse, rsquare = getError([y_[i - 2] for i in xdata], ydata)
    return x_, y_, {"a": popt[0], "b": popt[1]}, {"SSE": sse, "MSE": mse, "RMSE": rmse, "RSquare": rsquare}