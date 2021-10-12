# -*- coding:utf-8 -*-
"""
   File Name：     web_application.py
   Description :   生产环境 - Web服务（只8182一个端口）
   Author :        royce.mao
   date：          2021/7/1 9:26
"""

import web
import logging
import logging.config
import yaml
from config import cur_config as cfg

if not len(cfg.MEDIA):
    raise ValueError("MEDIA字段取值异常")
if '头条' in cfg.MEDIA:
    from media_toutiao.image_score_pre_new import TTImageScore
    if '末日' in cfg.GAME:
        from media_toutiao.create_plan_with_train import TTCreatePlan
    if '帝国' in cfg.GAME:
        from media_toutiao.create_plan_with_train_dg import TTCreatePlanDg
if '广点通' in cfg.MEDIA:
    from media_gdt.image_score_pre_new import GDTImageScore
    if '末日' in cfg.GAME:
        from media_gdt.create_plan_with_train import GDTCreatePlan
    if '帝国' in cfg.GAME:
        from media_gdt.create_plan_with_train_dg_tmp import GDTCreatePlanDg
    if '末日版位' in cfg.GAME:
        from media_gdt.create_plan_with_train_site_tmp import GDTCreatePlanSite

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
    '/TTImageScoreNew', TTImageScore,
    '/TTCreatePlanNew', TTCreatePlan,
    '/TTCreatePlanNewDg', TTCreatePlanDg,
    '/GDTImageScoreNew', GDTImageScore,
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
