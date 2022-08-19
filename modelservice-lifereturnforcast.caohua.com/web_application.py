# -*- coding:utf-8 -*-
"""
   File Name：     web_application.py
   Description :   接口服务
   Author :        royce.mao
   date：          2022/4/22 11:36 
"""
import web
import logging
import logging.config
import yaml
from reforcast import ReturnForcast
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
    '/ReturnForcast', ReturnForcast
)

#
# 封闭web，指定地址和端口
#
class MyApplication(web.application):
    def run(self, port=8080, *middleware):
        func = self.wsgifunc(*middleware)
        return web.httpserver.runsimple(func, ('0.0.0.0', port))

init_logging()
logger = logging.getLogger('web_application')
app_config = get_applicaion_configure()

if __name__ == '__main__':
    logger.info('LifeReturnForcast web server start!')
    # 启动web server
    port = app_config['server']['port']
    app = MyApplication(urls, globals())
    app.run(port=port)
