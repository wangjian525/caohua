import nacos

# 测试Nacos
SERVER_ADDRESSES = "172.16.167.37:8848"
NAMESPACE = "dev"
USERNAME = "nacos"
PASSWORD = "nacos"
DATAID = "newair-cas-manger.properties"
GROUP = "DEFAULT_GROUP"

# 线上Nacos
# SERVER_ADDRESSES = "nacos.caohua.ch:8848"
# NAMESPACE = "prd"
# USERNAME = "prd"
# PASSWORD = "nacos"
# DATAID = "model-service"
# GROUP = "DEFAULT_GROUP"


class global_var:
    """ 全局参数配置 """
    # HIVE地址
    HIVE_HOST=""
    HIVE_PORT=""
    HIVE_DATABASE=""
    HIVE_USERNAME=""
    HIVE_PASSWORD=""
    HIVE_AUTH_MECHANISM=""

    # db_slave_fenxi数据库参数
    DB_SLAVE_FENXI_HOST=""
    DB_SLAVE_FENXI_PORT=""
    DB_SLAVE_FENXI_DATABASE=""
    DB_SLAVE_FENXI_USERNAME=""
    DB_SLAVE_FENXI_PASSWORD=""

    # db_slave_toufang数据库参数
    DB_SLAVE_TOUFANG_HOST=""
    DB_SLAVE_TOUFANG_PORT=""
    DB_SLAVE_TOUFANG_DATABASE=""
    DB_SLAVE_TOUFANG_USERNAME=""
    DB_SLAVE_TOUFANG_PASSWORD=""

def set_var():
    client = nacos.NacosClient(SERVER_ADDRESSES, namespace=NAMESPACE, username=USERNAME, password=PASSWORD)
    # 读取
    try:
        config = client.get_config(DATAID, GROUP)
    except Exception as e:
        # print(e)
        raise ValueError("线上配置读取失败!")
    if config is None:
        raise ValueError("线上配置取值失败!")
    # 解析
    config = config.split("\n")
    dicStr = {}
    for i in config:
        dic = i.split("=")
        if len(dic) > 1:
            dicStr[dic[0].strip()] = dic[1].strip()
    # 赋值
    global_var.HIVE_HOST = dicStr['HIVE_HOST']
    global_var.HIVE_PORT = dicStr['HIVE_PORT']
    global_var.HIVE_DATABASE = dicStr['HIVE_DATABASE']
    global_var.HIVE_USERNAME = dicStr['HIVE_USERNAME']
    global_var.HIVE_PASSWORD = dicStr['HIVE_PASSWORD']
    global_var.HIVE_AUTH_MECHANISM = dicStr['HIVE_AUTH_MECHANISM']

    global_var.DB_SLAVE_FENXI_HOST = dicStr['DB_SLAVE_FENXI_HOST']
    global_var.DB_SLAVE_FENXI_PORT = dicStr['DB_SLAVE_FENXI_PORT']
    global_var.DB_SLAVE_FENXI_DATABASE = dicStr['DB_SLAVE_FENXI_DATABASE']
    global_var.DB_SLAVE_FENXI_USERNAME = dicStr['DB_SLAVE_FENXI_USERNAME']
    global_var.DB_SLAVE_FENXI_PASSWORD = dicStr['DB_SLAVE_FENXI_PASSWORD']

    global_var.DB_SLAVE_TOUFANG_HOST = dicStr['DB_SLAVE_TOUFANG_HOST']
    global_var.DB_SLAVE_TOUFANG_PORT = dicStr['DB_SLAVE_TOUFANG_PORT']
    global_var.DB_SLAVE_TOUFANG_DATABASE = dicStr['DB_SLAVE_TOUFANG_DATABASE']
    global_var.DB_SLAVE_TOUFANG_USERNAME = dicStr['DB_SLAVE_TOUFANG_USERNAME']
    global_var.DB_SLAVE_TOUFANG_PASSWORD = dicStr['DB_SLAVE_TOUFANG_PASSWORD']


def get_var():
    dicStr = {}
    dicStr['HIVE_HOST'] = global_var.HIVE_HOST
    dicStr['HIVE_PORT'] = global_var.HIVE_PORT
    dicStr['HIVE_DATABASE'] = global_var.HIVE_DATABASE
    dicStr['HIVE_USERNAME'] = global_var.HIVE_USERNAME
    dicStr['HIVE_PASSWORD'] = global_var.HIVE_PASSWORD
    dicStr['HIVE_AUTH_MECHANISM'] = global_var.HIVE_AUTH_MECHANISM

    dicStr['DB_SLAVE_FENXI_HOST'] = global_var.DB_SLAVE_FENXI_HOST
    dicStr['DB_SLAVE_FENXI_PORT'] = global_var.DB_SLAVE_FENXI_PORT
    dicStr['DB_SLAVE_FENXI_DATABASE'] = global_var.DB_SLAVE_FENXI_DATABASE
    dicStr['DB_SLAVE_FENXI_USERNAME'] = global_var.DB_SLAVE_FENXI_USERNAME
    dicStr['DB_SLAVE_FENXI_PASSWORD'] = global_var.DB_SLAVE_FENXI_PASSWORD

    dicStr['DB_SLAVE_TOUFANG_HOST'] = global_var.DB_SLAVE_TOUFANG_HOST
    dicStr['DB_SLAVE_TOUFANG_PORT'] = global_var.DB_SLAVE_TOUFANG_PORT
    dicStr['DB_SLAVE_TOUFANG_DATABASE'] = global_var.DB_SLAVE_TOUFANG_DATABASE
    dicStr['DB_SLAVE_TOUFANG_USERNAME'] = global_var.DB_SLAVE_TOUFANG_USERNAME
    dicStr['DB_SLAVE_TOUFANG_PASSWORD'] = global_var.DB_SLAVE_TOUFANG_PASSWORD

    return dicStr


set_var()
