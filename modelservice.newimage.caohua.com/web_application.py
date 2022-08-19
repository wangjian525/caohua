# -*- coding:utf-8 -*-
"""
   File Name：     web_application.py
   Description :   生产环境 - Web服务（8182端口）
   Author :        royce.mao
   date：          2021/7/1 9:26
"""

import web
import logging
import logging.config
import yaml

from config import *
from media_toutiao import TTCreatePlan  # 末日
from media_toutiao import TTCreatePlanDg  # 帝国
from media_toutiao import TTCreatePlanDdj  # 大东家
from media_gdt import GDTCreatePlan  # 末日
from media_gdt import GDTCreatePlanDg  # 帝国
from media_gdt import GDTCreatePlanSite  # 末日版位

from ptom_model import PtomPredictNew


#
# 初始化全局日志配置
#


def init_logging():
    with open('./logs/logging.yml', 'r') as conf:
        logging_conf = yaml.load(conf, Loader=yaml.FullLoader)
    logging.config.dictConfig(config=logging_conf)


#
# 获取应用程序配置
#
def get_applicaion_configure():
    with open('./logs/application.yml', 'r') as conf:
        application_conf = yaml.load(conf, Loader=yaml.FullLoader)
    return application_conf


#
# 首页
#
class Index:
    def GET(self):
        return 'Hello world!'


#
# 网站路由
#
urls = (
    '/', Index,
    '/PtomPredictNew', PtomPredictNew,
    '/TTCreatePlanNew', TTCreatePlan,
    '/TTCreatePlanNewDg', TTCreatePlanDg,
    '/TTCreatePlanNewDdj', TTCreatePlanDdj,
    '/GDTCreatePlanNew', GDTCreatePlan,
    '/GDTCreatePlanNewDg', GDTCreatePlanDg,
    '/GDTCreatePlanNewSite', GDTCreatePlanSite,
)

#
# 封闭web，指定地址和端口
#
class MyApplication(web.application):
    def run(self, port=8182, *middleware):  ## 默认8182端口
        func = self.wsgifunc(*middleware)
        return web.httpserver.runsimple(func, ('0.0.0.0', port))


init_logging()
logger = logging.getLogger('web_application')
app_config = get_applicaion_configure()


if __name__ == '__main__':
    # 
    logger.info('ModelServiceNewImage web server start!')
    # 启动web server
    port = app_config['server']['port']
    app = MyApplication(urls, globals())
    app.run(port=port)
