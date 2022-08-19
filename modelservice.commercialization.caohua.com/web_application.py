# -*- coding:utf-8 -*-

import web
import logging
import logging.config
import yaml
from modelservice import ImageTrainer
from modelservice import ImageScorer
from modelservice import PlanCreate
from modelservice import PlanMonitor


#
# 初始化全局日志配置
#
def init_logging():
    with open('./config/logging.yml', 'r') as conf:
        logging_conf = yaml.load(conf, Loader=yaml.FullLoader)
    logging.config.dictConfig(config=logging_conf)


#
# 获取应用程序配置
#
def get_applicaion_configure():
    with open('./config/application.yml', 'r') as conf:
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
    '/ImageScoreTrain', ImageTrainer,
    '/ImageScorePred', ImageScorer,
    '/PlanCreate', PlanCreate,
    '/PlanMonitor', PlanMonitor
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


def main():
    """ 主函数  """
    logger.info('ModelService web server start!')
    # 启动web server
    port = app_config['server']['port']
    app = MyApplication(urls, globals())
    app.run(port=port)


if __name__ == '__main__':
    main()
