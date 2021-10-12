import web
import logging
import logging.config
import yaml
from modelservice import PtomPredict
from modelservice import CurveFit
from modelservice import ImageScore
from modelservice import ImageScoreDg
from modelservice import CreatePlan
from modelservice import CreatePlan2
from modelservice import CreatePlanDg
from modelservice import CreatePlanDggz
from modelservice import CreatePlangdt
from modelservice import CreatePlangdtdg
from modelservice import ImageScoreJp
from modelservice import CreatePlangdtjp
from modelservice import AccountFilter
from modelservice import CreatePlangdtMrIos
from modelservice import CreatePlanttdgIOS
from modelservice import CreatePlangdtdgIOS
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
    '/curvefit', CurveFit,
    '/PtomPredict', PtomPredict,
    '/ImageScore', ImageScore,
    '/ImageScoreDg', ImageScoreDg,
    '/ImageScoreJp', ImageScoreJp,
    '/CreatePlan', CreatePlan,
    '/CreatePlan2', CreatePlan2,
    '/CreatePlanDggz', CreatePlanDggz,
    '/CreatePlanDg', CreatePlanDg,
    '/CreatePlangdt', CreatePlangdt,
    '/CreatePlangdtdg', CreatePlangdtdg,
    '/CreatePlangdtjp', CreatePlangdtjp,
    '/AccountFilter', AccountFilter,
    '/CreatePlangdtMrIos', CreatePlangdtMrIos,
    '/CreatePlanttdgIOS', CreatePlanttdgIOS,
    '/CreatePlangdtdgIOS', CreatePlangdtdgIOS
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
    logger.info('ModelService web server start!')
    # 启动web server
    port = app_config['server']['port']
    app = MyApplication(urls, globals())
    app.run(port=port)
