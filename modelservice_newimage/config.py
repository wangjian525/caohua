# -*- coding:utf-8 -*-
"""
   File Name：     config.py
   Description :   应用参数配置（不同于logging的服务参数，这里直接写成config脚本）
   Author :        royce.mao
   date：          2021/6/8 9:12
"""


class Config(object):
    """[应用参数配置]

    Args:
        object ([type]): [description]
    """
    MEDIA = ['头条', '广点通'] ## 例如：['头条', '广点通']
    GAME = ['末日', '帝国', '末日版位'] ## 例如：['末日', '帝国']

    MR_GAME_ID = 1056  ## 末日的中间件ID
    DG_GAME_ID = 1112  ## 帝国的中间件ID

    # =============================== 参数情况：头条
    MR_GAME = 1001703
    DG_GAME = 1001788
    AD_ACCOUNT_ID_GROUP_MR = [7794, 7852, 7853, 7854, 7855]
    AD_ACCOUNT_ID_GROUP_DG = [7759, 7756, 7550, 6837]
    DTC_PLAN_PATH = "./aimodel/xgb_plan.pt"  ## NewCreatePlan类：新计划好坏分类模型

    # =============================== 参数情况：广点通
    GDT_MR_GAME = 1001379
    GDT_DG_GAME = 1001832 ## TODO:老包游戏ID为1001545
    GDT_AD_ACCOUNT_ID_GROUP_MR = [7983, 7985, 8074, 8079, 8081]  ## 20786176,20786174,20948065,20948059,20948057
    GDT_AD_ACCOUNT_ID_GROUP_DG = [8849, 8850, 8851, 8852, 8853]  ## TODO:原金牌账户
    GDT_SITE_SET = ['SITE_SET_WECHAT','SITE_SET_MOMENTS','SITE_SET_TENCENT_VIDEO','SITE_SET_QQ_MUSIC_GAME','SITE_SET_KANDIAN','SITE_SET_TENCENT_NEWS','SITE_SET_MOBILE_UNION']
    GDT_DTC_PLAN_PATH = "./aimodel/xgb_plan_gdt.pt"  ## NewCreatePlan类：新计划好坏分类模型
    # =============================== 分割线

    MR_LABEL_ID = 20

    SCORE_OLD = 560  ## NewCreatePlan类：旧素材score阈值
    SCORE_NEW = 0  ## NewCreatePlan类：新素材score阈值（目前全测）

    ROI = 0.02  ## DataRealTime类：界定计划好坏的标准
    PLAN_OUT = 100  ## 

    XGB_PRE_SCORE_PATH = "./aimodel/xgb_score_pre.pt"   ## NewImageScore类：新素材评分模型（事前）

    GBDT_DATAWIN1_PATH = "./aimodel/gbdt_m2_win1.pkl"
    GBDT_DATAWIN2_PATH = "./aimodel/gbdt_m2_win2.pkl"
    GBDT_DATAWIN3_PATH = "./aimodel/gbdt_m2_win3.pkl"
    AD_ACCOUNT_CAPACITY = 10000

    # =============================== 字段情况：头条
    # 数字编码字段
    ENCODER_COLS = ['ad_account_id', 'image_id', 'game_id', 'ac', 'launch_price', 'gender', 'delivery_range', 'hide_if_converted', 'hide_if_exists', 'auto_extend_enabled', 'flow_control_mode', 'location_type', 'inventory_type', 'schedule_time', 'city']
    # 连续值字段
    CONT_COLS = ['image_score', 'budget', 'cpa_bid']
    # 其他定向字段
    ORIENT_COLS_GROUP = ['retargeting_type', 'retargeting_tags_include', 'retargeting_tags_exclude']
    ORIENT_COLS_ACTION = ['action_days', 'action_categories', 'action_scene', 'interest_categories', 'interest_action_mode']
    ORIENT_COLS_BID = ['adjust_cpa', 'smart_bid_type', 'deep_bid_type', 'roi_goal', 'cpa_bid']
    NEW_COLS = ['ad_keywords', 'title_list', 'third_industry_id']

    # =============================== 字段情况：广点通
    # 数字编码字段
    GDT_ENCODER_COLS = ['ad_account_id', 'image_id', 'game_id', 'optimization_goal', 'time_series', 'bid_strategy', 'expand_enabled',
                        'expand_targeting', 'device_price', 'app_install_status', 'gender', 'game_consumption_level', 'age', 'network_type',
                        'deep_conversion_type', 'behavior_scene', 'behavior_time_window', 'conversion_behavior_list', 'excluded_dimension',
                        'location_types', 'regions', 'adcreative_template_id', 'page_type', 'site_set', 'label', 'link_name_type', 'label_ids']
    # hotting编码字段（勾选分组）
    GDT_HOTTING_COLS = ['behavior_category_id_list', 'behavior_intensity', 'behavior_keyword_list',
                        'intention_targeting_tags', 'interest_category_id_list', 'interest_keyword_list']
    # 连续值字段
    GDT_CONT_COLS = ['image_score']

    # 字段汇总：计划特征组合
    GDT_TIME_SERIES_COLS = ['time_series']
    GDT_ORIENT_COLS_BID = ['optimization_goal', 'bid_strategy', 'bid_amount', 'deep_conversion_type', 'deep_conversion_behavior_spec', 'deep_conversion_worth_spec']
    GDT_ORIENT_COLS_EXPAND = ['expand_enabled', 'expand_targeting']
    GDT_ORIENT_COLS_TARGETING = ['device_price', 'app_install_status', 'gender', 'game_consumption_level', 'age', 'network_type',
                                 'conversion_behavior_list', 'excluded_dimension', 'location_types', 'regions',
                                 'intention_targeting_tags', 'interest_category_id_list', 'interest_keyword_list',
                                 'behavior_category_id_list', 'behavior_intensity',
                                 'behavior_keyword_list', 'behavior_scene', 'behavior_time_window']
    GDT_NEW_COLS = ['site_set', 'deep_link_url', 'adcreative_template_id', 'page_spec', 'page_type', 'link_page_spec',
                    'link_name_type', 'link_page_type', 'promoted_object_id', 'profile_id', 'promoted_object_type',
                    'automatic_site_enabled', 'label', 'adcreative_elements']
    # =============================== 分割线

    # 新素材评分模型，'label_ids'字段的量化mapping
    LABEL_IDS_MAPPING = {'103': 517.0140259298205,
            '111': 324.6908398661126,
            '112': 487.2804878048776,
            '125': 467.42105263158004,
            '126': 507.4460784313726,
            '127': 541.2041288485502,
            '13': 479.4999999999983,
            '145': 502.3554817275753,
            '149': 143.64133014382395,
            '158': 506.2857142857141,
            '19': 511.85228334482116,
            '20': 530.6185059443285,
            '21': 479.60869565217416,
            '22': 533.7288267288284,
            '25': 493.0805973413028,
            '26': 461.5833333333337,
            '27': 490.34566713942127,
            '56': 485.05,
            '67': 513.1116504854368,
            '81': 494.5555555555555,
            '82': 522.8695652173913,
            '83': 357.5538857545059,
            '96': 54.55267938237962,
            '98': 478.03405994550417}
    # 新素材评分模型，'game_ids'字段的量化mapping
    GAME_IDS_MAPPING =  {'1000840': 6.934062279468404,
            '1000954': 27.875692630631335,
            '1000960': -93.5216409072253,
            '1000985': 3.5328198687400594,
            '1000992': -15.893936699616596,
            '1000993': 88.90600676969183,
            '1000994': -33.73246661694313,
            '1001049': 24.66829761876672,
            '1001155': -22.572413238529542,
            '1001193': -0.6601238953378137,
            '1001194': 30.789853268225496,
            '1001257': 11.264476387041572,
            '1001258': 37.306044734959556,
            '1001259': -50.037921257298514,
            '1001294': 4.387220564973811,
            '1001295': -15.30096191182825,
            '1001379': 467.12194034214104,
            '1001400': 24.1244114298255,
            '1001401': -19.123515838478838,
            '1001402': -54.50551003610357,
            '1001413': -32.83671835518011,
            '1001420': 25.53592181141895,
            '1001425': 2.885885885886214,
            '1001426': 499.89189189189153,
            '1001439': -23.974470048988135,
            '1001440': -1.7985353124910044,
            '1001441': -13.237733423575262,
            '1001444': -1.1825758210341168,
            '1001447': 1.036978302535827,
            '1001449': -45.65965886479905,
            '1001457': -9.36896767812626,
            '1001460': -20.400797640787935,
            '1001484': -18.952427530997355,
            '1001540': 68.02937659618891,
            '1001541': -16.335525461916703,
            '1001610': -2.078718141909352,
            '1001643': 56.227814664390905,
            '1001703': 36.45956746455081}
    

cur_config = Config()
