# -*- coding:utf-8 -*-
"""
   File Name：     create_plan_new.py
   Description :   新建计划：测新计划自动化组合（帝国广点通）（拆版位且不入库）
   Author :        royce.mao
   date：          2021/9/16 15:27
"""

import pandas as pd
import numpy as np
import json
import os
import ast
import time
import datetime
# os.chdir(os.path.dirname(__file__))
import pymysql
from impala.dbapi import connect
from impala.util import as_pandas
import random
import logging

import warnings
warnings.filterwarnings('ignore')
logger = logging.getLogger('CreatePlan')

from config import cur_config as cfg
from media_gdt.script.trainer import train
from media_gdt.script.printout import get_plan_online  ## 格式化输出
from media_gdt.script.realtime_df_dg_site import DataRealTime, get_game_id, image_elements, location_type  ## 采集实时数据类
from media_gdt.image_score_pre_new import NewImageScore  ## 新素材事前评分类

#
# 打包接口
#
class GDTCreatePlanDg:
    def __init__(self,): 
        logging.info("collect data")
        self.new_create_plan = NewCreatePlan()

    def POST(self):
        # 处理POST请求
        logging.info("do service")
        try:
            ret = self.Process()
            logging.info(ret)
            return ret
        except Exception as e:
            logging.error(e)
            ret = json.dumps({"code": 500, "msg": str(e)})
            return ret

    # 任务处理函数
    def Process(self):
        self.new_create_plan()
        ret = json.dumps({"code": 200, "msg": "success!", "data": "create plan is success"})
        return ret


class NewCreatePlan(object):
    """ 新计划创建类 """
    def __init__(self, ):
        self.image_score_pre = NewImageScore()
        self.fea_assoc = FeaAssoc()

    @staticmethod
    def find_new_images(dg_game, true_game_ids, is_back=True):
        """[新素材筛选 + 实时入库]

        Returns:
        """
        # 88数据库 游标1
        conn1 = connect(host='192.168.0.97', port=10000, auth_mechanism='PLAIN', user='hadoop',
                    password='Ycjh8FxiaoMtShZRd3-97%3hCEL0CK4ns1w', database='default')
        cur1 = conn1.cursor()
        sql_engine = 'set hive.execution.engine=tez'
        sql = 'select image_id,label_ids,score from dws.dws_image_score_d where media_id=16 and dt>=\'2021-01-01\''
        cur1.execute(sql_engine)
        cur1.execute(sql)
        score_df = as_pandas(cur1)

        # 65数据库 游标2
        conn2 = pymysql.connect(host='db-slave-modeltoufang-001.ch', port=3306, user='model_read',
                                passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8')
        cur2 = conn2.cursor(cursor=pymysql.cursors.DictCursor)
        sql = '''
                /*手动查询*/ 
                SELECT
                    a.image_id,
                    a.image_name,
                    a.label_ids,
                    a.game_ids
                FROM
                    db_ptom.ptom_image_info a
                WHERE
                    a.create_time >= date(NOW() - INTERVAL 72 HOUR)  ## 最近3天
            '''
        cur2.execute(sql)
        image_df = pd.read_sql(sql, conn2)
        image_df['label_ids'] = image_df['label_ids'].str.replace( ',', ';')  ## 避免分隔符冲突
        image_df['game_ids'] = image_df['game_ids'].str.replace( ',', ';')  ## 避免分隔符冲突

        # 79数据库 游标3
        game_id = list(map(lambda x: x['game_id'], true_game_ids))
        game_id = [str(i) for i in game_id]
        game_id = ','.join(game_id)
        conn3 = pymysql.connect(host='db-slave-modelfenxi-001.ch', port=3306, user='model_read',
                               passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8')
        cur3 = conn3.cursor(cursor=pymysql.cursors.DictCursor)
        sql = '''
            /*手动查询*/
            SELECT
                a.chl_user_id AS channel_id,
                a.source_id AS source_id,
                a.image_id,
                b.tdate,
                b.amount
            FROM
                db_data_ptom.ptom_plan a
                LEFT JOIN db_stdata.st_lauch_report b ON a.chl_user_id = b.channel_id 
                AND a.source_id = b.source_id
            WHERE
                a.create_time >= date(NOW() - INTERVAL 72 HOUR)  ## 最近3天
                AND b.tdate >= date(NOW() - INTERVAL 72 HOUR)  ## 最近3天
                AND b.tdate_type = 'day' 
                AND b.media_id = 16
                AND b.game_id IN ({})
        '''
        finalSql = sql.format(game_id)
        cur3.execute(finalSql)
        plan_df = pd.read_sql(finalSql, conn3)
        plan_df['tdate'] = pd.to_datetime(plan_df['tdate'])
        plan_df = plan_df.sort_values('tdate')
        plan_df = plan_df.drop_duplicates(['channel_id', 'source_id', 'tdate'], keep='first')

        # 关闭链接
        cur1.close(), conn1.close()
        cur2.close(), conn2.close()
        cur3.close(), conn3.close()

        # 新素材的label_ids非空
        image_df = image_df.loc[image_df.apply(lambda x:True if not pd.isnull(x['label_ids']) and not x['label_ids']=='' else False, axis=1)]
        # 新素材的game_ids对齐
        image_df = image_df.loc[image_df.apply(lambda x:True if not pd.isnull(x['game_ids']) and str(dg_game) in set(x['game_ids'].split(';')) else False, axis=1)]  ## TODO:有该id的素材
        # 新素材的image_game有TT标识
        image_df = image_df.loc[image_df['image_name'].apply(lambda x:True if 'TT' in x else False)]

        if not is_back:
            # 过滤新素材方案一：直接匹配没有评分的素材 +  有评分但总消耗在1000以上6000以内的素材
            image_df_new = image_df.loc[image_df.apply(lambda x:True if not any(score_df['image_id'] == x.image_id) else False, axis=1)]
            print("no score new num:", len(image_df_new))  ## 未评分新素材数量
            print("no score new id:", image_df_new['image_id'].values.tolist())
            plan_df = plan_df.groupby('image_id')['amount'].sum()
            plan_df = pd.DataFrame({'image_id':plan_df.index,'amount':plan_df.values})
            plan_df = plan_df.loc[(plan_df['amount'] >= 1000) & (plan_df['amount'] <= 3000)]  # todo:上下限调整
            image_df_old = image_df.loc[image_df['image_id'].isin(np.intersect1d(plan_df['image_id'].values, image_df['image_id'].values))]
            print("back new num:", len(image_df_old))  ## 返新素材数量
            print("back new id:", image_df_old['image_id'].values.tolist())
            # image_df = image_df_new
            image_df = pd.concat([image_df_new, image_df_old], ignore_index=True, join='inner')
        else:
            # 过滤新素材方案二：匹配没有评分且没有建过计划的素材 + 有评分但总消耗在1000以上6000以内的素材
            image_df_new = image_df.loc[image_df.apply(lambda x:True if not any(plan_df['image_id'] == x.image_id) else False, axis=1)]
            print("pure new num:", len(image_df_new))  ## 纯新素材数量
            plan_df = plan_df.groupby('image_id')['amount'].sum()
            plan_df = pd.DataFrame({'image_id':plan_df.index,'amount':plan_df.values})
            plan_df = plan_df.loc[(plan_df['amount'] >= 1000) & (plan_df['amount'] <= 3000)]  # todo:上下限调整
            image_df_old = image_df.loc[image_df['image_id'].isin(np.intersect1d(plan_df['image_id'].values, image_df['image_id'].values))]
            print("back new num:", len(image_df_old))  ## 返新素材数量
            image_df = pd.concat([image_df_new, image_df_old], ignore_index=True, join='inner')

        image_df['score'] = np.nan
        image_df['label'] = 0
        image_df['tdate'] = datetime.date.today()
        
        # 实时入库 第1次
        csv_path = 'new_image_{}.csv'.format(time.strftime('%Y%m%d%H%M%S', time.localtime(int(time.time()))))
        image_df.to_csv(csv_path, index=0, encoding='utf_8_sig',header=None)  ## label暂未实时修正的结果，且score也是初始化的NaN
        while not os.path.exists(csv_path):
            time.sleep(1)
        
        # os.system("hadoop fs -rm -r hdfs://masters/user/hive/warehouse/test.db/gdt_new_image_test")
        # os.system("hadoop fs -mkdir hdfs://masters/user/hive/warehouse/test.db/gdt_new_image_test")
        # os.system("hadoop fs -put {} hdfs://masters/user/hive/warehouse/test.db/gdt_new_image_test_dg".format(csv_path))
        # os.system("hive -e \"msck repair table test.gdt_new_image_test_dg\"")
        os.remove(csv_path)

        return image_df
        
    @staticmethod
    def plan_attr_assoc(score_image_group, ad_account_id_group, capacity):  # 具体建模过程见Plan.ipynb
        """ [计划资源分配]

        Args:
            score_image_group ([dataframe]): [新素材]
            ad_account_id_group ([list]): [账号集]
            columns ([list]): [新建计划涉及的字段组合]
            capacity ([int]): [账号的容积计划数量限制]
        """
        # score_image_group = score_image_group.loc[score_image_group.apply(lambda x:True if int(x['image_id']) >= 35400 else False, axis=1)]  ## todo:临时
        
        dist_image_ids = score_image_group['image_id'].value_counts().index
        plan = pd.DataFrame(columns=['image_id','label_ids','image_score'])

        for image_id in sorted(dist_image_ids, key=lambda x: random.random()):  ## 按异质id标签遍历
            # 素材下新建计划
            sample = random.choice(score_image_group[score_image_group['image_id'] == image_id][['label_ids', 'score']].values)
            label = sample[0]
            score = sample[1]
            plan_part = pd.DataFrame({'image_id': [image_id], 'label_ids': [label], 'image_score': [score]})
            plan = plan.append(plan_part)
        # 复制3条
        plan = pd.DataFrame(np.repeat(plan.values, 3, axis=0), columns=['image_id','label_ids','image_score'])
        plan['ad_account_id'] = np.nan
        
        # 分配账号
        ad_counts = np.array([0] * len(ad_account_id_group), dtype=int)
        ad_account_id_group = np.array(ad_account_id_group, dtype=int)

        for image_id in dist_image_ids:
            # index = ad_counts.argsort()[:3]
            # ad_asso = ad_account_id_group[index[ad_counts[index] < capacity]]
            ad_asso = ad_account_id_group[ad_counts.argsort()[:3]]
            plan['ad_account_id'].loc[plan['image_id'] == image_id] = ad_asso
            ad_counts[ad_counts.argsort()[:3]] += 1
        # 容积过滤
        ad_caps = ad_account_id_group[ad_counts > capacity]
        if not len(ad_caps):
            plan['label_ids'] = plan['label_ids'].apply(lambda x:x.split(','))
            return plan.reset_index(drop=True)
        else:
            for ad_cap in ad_caps:
                num_del = ad_counts[ad_account_id_group == ad_cap] - capacity
                dat_del = plan.loc[plan['ad_account_id'] == ad_cap].sample(num_del)
                plan = pd.merge(plan, dat_del, how='left', indicator=True).query("_merge=='left_only'").drop('_merge', 1)
            plan['label_ids'] = plan['label_ids'].apply(lambda x:x.split(','))
            return plan.reset_index(drop=True)

    @staticmethod
    def plan_dtc_screen(plan_prob, plan, plan_out_num, is_test=True):
        """[候选计划筛选]

        Args:
            dtc ([series]): [组合输出的候选计划 - 得分]
            plan_associate ([dataframe]]): [组合输出的候选计划 - 原计划]
            prob_thresh (float, optional): [概率阈值]. Defaults to 0.2.
            is_test (bool, optional): [是否测试场景]. Defaults to True.
        """
        if not is_test:  ## 非测试场景：需要过滤步骤
            # 70分位数的概率阈值
            prob_thresh = np.percentile(plan_prob, 70)
            
            # 分组排名prob概率最大值过滤
            plan['prob'] = plan_prob
            plan['rank_ad_im'] = plan.groupby(['ad_account_id', 'image_id'])['prob'].rank(ascending=False, method='first')
            plan_result = plan[plan['rank_ad_im'] <= 1]
            
            # 概率阈值过滤
            plan_result = plan_result[plan_result['prob'] >= prob_thresh]
            
            # 指定计划数量的加权过滤，以账户下的素材丰富度为权重
            plan_result['weight'] = plan_result.groupby(['ad_account_id'])['game_id'].transform('count')
            if plan_result.shape[0] > plan_out_num:
                plan_result = plan_result.sample(plan_out_num, weights=plan_result['weight'])

            # 前几步过滤后，各流失账户的计划找回（找一条）
            ad_account_num = plan_result['ad_account_id'].value_counts()
            for ad in np.setdiff1d(plan['ad_account_id'].values, ad_account_num.index):
                add_plan = plan[plan['ad_account_id'] == ad].sort_values('prob', ascending=False)[:1]
                plan_result = plan_result.append(add_plan)
            plan_result = plan_result.drop(['create_time', 'create_date', 'label_ids', 'prob', 'rank_ad_im', 'label', 'image_score', 'weight'], axis=1)
        else:  ## 测试场景：不需要过滤
            plan_result = plan
            plan_result = plan_result.drop(['create_time', 'create_date', 'label_ids', 'label', 'image_score'], axis=1)

        # 输出
        plan_result['op_id'] = 13678
        plan_result['flag'] = 'CESHI_GDT'
        # plan_result['operation'] = 'disable'
        plan_result['game_name'] = '亚洲王朝' ## TODO:老包1001545
        plan_result['platform'] = 1 ## TODO:新字段
        plan_result['budget_mode'] = 0  # 不限预算
        plan_result['daily_budget'] = 0  # 不限预算
        plan_result['promoted_object_id'] = '2000001805' ## TODO:老包'1111430997'（待修改）
        plan_result['ad_account_id'] = plan_result['ad_account_id'].apply(lambda x:str(int(x)))
        # plan_result['platform'] = plan_result['platform'].map({1: ['ANDROID'], 2: ['IOS']})
        ## [SITE_SET_WECHAT]公众号和取值范围在1480、560、720、721、1064五种中adcreative_template_id小程序
        # plan_result['site_set'] = plan_result['site_set'].map(str)
        plan_result = plan_result[~((plan_result['site_set'] == "['SITE_SET_WECHAT']") & \
                                    (~plan_result['adcreative_template_id'].isin([1480, 560, 720, 721, 1064])))]
        ## 落地页ID（暂时没跑）
        plan_result['page_spec'] = plan_result.apply(
        lambda x: {'override_canvas_head_option': 'OPTION_CREATIVE_OVERRIDE_CANVAS',
                   'page_id': '2268166449'} if x.site_set == "['SITE_SET_MOMENTS']"
        else ({'override_canvas_head_option': 'OPTION_CREATIVE_OVERRIDE_CANVAS',
               'page_id': '2268167873'} if x.site_set == "['SITE_SET_WECHAT']" else np.nan), axis=1)
        plan_result['link_page_spec'] = plan_result.apply(
        lambda x: {'page_id': '2268166449'} if x.site_set == "['SITE_SET_MOMENTS']"
        else ({'page_id': '2268167873'} if x.site_set == "['SITE_SET_WECHAT']" else np.nan), axis=1)

        ## 朋友圈头像ID（暂时没跑）
        plan_result_1 = plan_result[plan_result['site_set'] == "['SITE_SET_MOMENTS']"]
        plan_result_2 = plan_result[plan_result['site_set'] != "['SITE_SET_MOMENTS']"]
        profile_id_dict = {'8849': '551982', '8850': '552008', '8851': '552025', '8852': '552038', '8853': '552046'}  ## TODO:测新账号
        plan_result_1['profile_id'] = plan_result_1['ad_account_id'].map(profile_id_dict)
        plan_result = plan_result_1.append(plan_result_2)
        plan_result.reset_index(drop=True, inplace=True)
        ## 目标定向字段映射
        plan_result['intention'] = plan_result['intention_targeting_tags'].apply(lambda x: {'targeting_tags': x})
        plan_result.drop(['intention_targeting_tags'], axis=1, inplace=True)
        ## 兴趣定向组合json
        plan_result['interest'] = plan_result.apply(lambda x:json.loads(
            x[['interest_category_id_list','interest_keyword_list']].rename(index={
               'interest_category_id_list': 'category_id_list',
               'interest_keyword_list': 'keyword_list'
               }).to_json()), axis=1)
        plan_result.drop(['interest_category_id_list', 'interest_keyword_list'], axis=1, inplace=True)
        ## 行为定向组合json
        plan_result['behavior'] = plan_result.apply(lambda x:json.loads(
            x[['behavior_category_id_list','behavior_intensity','behavior_keyword_list','behavior_scene','behavior_time_window']].rename(index={
               'behavior_category_id_list': 'category_id_list',
               'behavior_intensity': 'intensity',
               'behavior_keyword_list': 'keyword_list',
               'behavior_scene': 'scene',
               'behavior_time_window': 'time_window'
               }).to_json()), axis=1)
        plan_result.drop(['behavior_category_id_list', 'behavior_intensity', 'behavior_keyword_list', 'behavior_scene',
                          'behavior_time_window'], axis=1, inplace=True)
        plan_result['behavior'] = plan_result['behavior'].apply(lambda x: [x])
        ## 排除定向组合json
        plan_result['excluded_converted_audience'] = plan_result.apply(lambda x:json.loads(
            x[['conversion_behavior_list', 'excluded_dimension']].to_json()), axis=1)
        plan_result.drop(['conversion_behavior_list', 'excluded_dimension'], axis=1, inplace=True)
        ## 地域定向组合json
        plan_result['regions'] = plan_result['regions'].apply(
            lambda x: x if x == x else [710000, 540000, 630000, 510000, 450000, 320000, 220000, 370000, 340000, 150000,
                                        140000, 420700,
                                        422800, 421100, 420200, 420800, 421000, 429005, 429021, 420300, 421300, 429006,
                                        429004, 421200,
                                        420600, 420900, 420500, 130000, 360000, 310000, 330000, 820000, 650000, 350000,
                                        120000, 110000,
                                        640000, 530000, 210000, 610000, 520000, 810000, 230000, 460000, 440000, 500000,
                                        410000, 620000, 430000])
        plan_result['geo_location'] = plan_result.apply(lambda x:json.loads(
            x[['regions', 'location_types']].to_json()), axis=1)
        plan_result.drop(['regions', 'location_types'], axis=1, inplace=True)
        ## 其他定向组合json
        plan_result['targeting'] = plan_result.apply(lambda x:json.loads(
            x[['device_price', 'app_install_status', 'gender', 'game_consumption_level', 'age', 'network_type', 'excluded_converted_audience', 'geo_location', 'intention', 'interest', 'behavior']].to_json()), axis=1)
        plan_result.drop(['device_price', 'app_install_status', 'gender', 'game_consumption_level', 'age', 'network_type', 'excluded_converted_audience', 'geo_location', 'intention', 'interest', 'behavior'], axis=1, inplace=True)
        plan_result['targeting'] = plan_result['targeting'].apply(lambda x:location_type(x))

        plan_result['ad_account_id'] = plan_result['ad_account_id'].astype(int)
        plan_result['site_set'] = plan_result['site_set'].apply(ast.literal_eval)
        ## 时段周三周四更新凌晨不跑计划
        plan_result['time_series'] = plan_result['time_series'].apply(
            lambda x: x[0:96] + '1111111111000000000011' + x[118:144] + '1111111111000000000011' + x[166:])
        
        ## 子素材以版位尺寸匹配结果做新素材的替换
        plan_result['adcreative_elements'] = plan_result.apply(lambda x:image_elements(x), axis=1)
        # print("re plan num:", len(plan_result))  ## 前置计划数量
        plan_result.dropna(subset=['adcreative_elements'], inplace=True)  ## 如果没有对应尺寸的新素材直接pass
        # print("af plan num:", len(plan_result))  ## 后置计划数量

        # 过滤组合版位的‘优量汇’广告位
        def is_in_list(n):
            return n != 'SITE_SET_MOBILE_UNION'

        plan_result['site_set'] = plan_result['site_set'].apply(lambda x: list(filter(is_in_list, x)) if len(x) > 1 else x)

        plan_result.drop('page_type', axis=1, inplace=True)
        plan_result.to_csv('./plan_result.csv', index=0)  ## 保存创建日志

        return plan_result


    def __call__(self, ):
        """ 创建类函数 """
        # 新素材
        # self.find_new_images(cfg.GDT_DG_GAME, get_game_id(cfg.DG_GAME_ID), False)  ## 该方法实时更新测新库test.new_image_test
        # score_images = self.image_score_pre('帝国')  ## 该方法实时提取测新库中的新素材
        score_images = self.find_new_images(cfg.GDT_DG_GAME, get_game_id(cfg.DG_GAME_ID), False)
        print("[INFO]：新素材提取成功！")  ## 测试用日志
        # todo: keep='first'代表保留入库记录不再重复测新（未评分的素材不再参与），keep='last'代表重建入库记录重新测新（未评分的素材再次参与）
        score_images.drop_duplicates(subset=['image_id'], keep='last', inplace=True)  ## 建计划之前去重：避免只有1次入库异常素材的重复建计划测新
        
        score_images['label_ids'] = score_images['label_ids'].astype(str)
        score_images_associate = score_images[score_images['label'] == 0]
        # score_images_associate = score_images_associate.iloc[-1:]

        # 计划：创建 + 建模
        plan_associate = self.plan_attr_assoc(score_images_associate, cfg.GDT_AD_ACCOUNT_ID_GROUP_DG, cfg.AD_ACCOUNT_CAPACITY)
        plan_associate, plan_prob = self.fea_assoc(plan_associate)
        print("[INFO]: 候选计划组合完毕！")  ## 测试用日志
        
        # 计划：过滤
        plan_result = self.plan_dtc_screen(plan_prob[:, 1], plan_associate, cfg.PLAN_OUT, True)

        # 入库：label调整
        image_mixed = np.intersect1d(plan_result['image_id'].values, score_images['image_id'].values)
        score_images['label'].loc[score_images['image_id'].isin(image_mixed)] = 1  ## 已建计划的素材label标记为1，不再属于测新范畴
        
        score_images['label_ids'] = score_images['label_ids'].str.replace( ',', ';')  ## 避免分隔符冲突
        score_images['game_ids'] = score_images['game_ids'].str.replace( ',', ';')  ## 避免分隔符冲突
        
        score_images.drop_duplicates(keep='first', inplace=True)  ## 建计划之后去重：避免计划消耗异常新素材下次测新时的重复入库
        
        # label为1的images是真正参与计划创建了的images，其他要么初步score得分太低，要么计划prob概率太低，被过滤掉了
        # 实时入库 第2次
        csv_path = 'new_image_{}.csv'.format(time.strftime('%Y%m%d%H%M%S', time.localtime(int(time.time()))))
        score_images.to_csv(csv_path, index=0, encoding='utf_8_sig',header=None)  ## label实时修正后的结果，score有初步的评分
        # os.system("hadoop fs -rm -r hdfs://masters/user/hive/warehouse/test.db/gdt_new_image_test_dg")
        # os.system("hadoop fs -mkdir hdfs://masters/user/hive/warehouse/test.db/gdt_new_image_test_dg")
        # os.system("hadoop fs -put {} hdfs://masters/user/hive/warehouse/test.db/gdt_new_image_test_dg".format(csv_path))
        # os.system("hive -e \"msck repair table test.gdt_new_image_test_dg\"")
        os.remove(csv_path)

        # 格式化输出
        rsp = get_plan_online(plan_result)  ## todo
        print("[INFO]: 新建计划创建完毕！")  ## 测试用日志
        return plan_result
        

class FeaAssoc(object):
    """ 计划特征组合类 """
    def __init__(self, ):
        self.gdt_time_series_cols = cfg.GDT_TIME_SERIES_COLS  ## 定时
        self.gdt_orient_cols_bid = cfg.GDT_ORIENT_COLS_BID  ## 出价方式
        self.gdt_orient_cols_expand = cfg.GDT_ORIENT_COLS_EXPAND  ## 扩展定向
        self.gdt_orient_cols_targeting = cfg.GDT_ORIENT_COLS_TARGETING  ## 其他定向
        
        self.gdt_new_cols = cfg.GDT_NEW_COLS  ## 创意

        self.df_, self.df_create, self.df_image = DataRealTime()(cfg)
        self.df_create = self.df_create[self.df_create['game_id'].isin([1001832, 1001545, 1001465])]
        print("[INFO]: 实时数据提取完毕！")  ## 测试用日志

    @staticmethod
    def site_given(df, plans):
        """[函数：版位下优化目标依赖]

        Args:
            df ([dataframe]]): [按image_id分组的计划]
            plans ([dataframe]]): [候选计划，用于提取字段要素]
        """
        plans['site_set'] = plans['site_set'].map(str)
        assert len(df) == 3, "复制量不等于3"
        df['site_set'] = df['site_set'].map(str)

        # TODO:版位改变（朋友圈+优量汇+1种随机普通版位的组合！）
        def req_site(data, plan_sets):
            """ 计划版位分配的闭包处理 """
            score = data['image_score'].iloc[0]
            
            moments = data[data['site_set'] == "['SITE_SET_MOMENTS']"].sample(n=1).reset_index(drop=True) if len(data[data['site_set'] == "['SITE_SET_MOMENTS']"]) > 0 else \
                      plan_sets[plan_sets['site_set'] == "['SITE_SET_MOMENTS']"].sample(n=1).reset_index(drop=True)
            moments['image_score'], moments['platform'] = score, 1
            moments = moments[data.columns]

            union =   data[data['site_set'].str.contains('SITE_SET_MOBILE_UNION')].sample(n=1).reset_index(drop=True) if len(data[data['site_set'] == "['SITE_SET_MOBILE_UNION']"]) > 0 else \
                      plan_sets[plan_sets['site_set'].str.contains('SITE_SET_MOBILE_UNION')].sample(n=1).reset_index(drop=True)
            union['image_score'], union['platform'] = score, 1
            union = union[data.columns]
            union['site_set'] = "['SITE_SET_MOBILE_UNION']"  ## TODO:优量汇单版

            grades =  plan_sets[plan_sets['site_set'].apply(lambda x:True if "," in x else False)].sample(n=1).reset_index(drop=True)
            grades['image_score'], grades['platform'] = score, 1
            grades = grades[data.columns]
            
            plan = pd.concat([moments, union, grades], axis=0).reset_index(drop=True)  ## TODO:3条计划堆叠拼接
            # plan = pd.concat([moments, grades], axis=0).reset_index(drop=True)  ## TODO:2条计划堆叠拼接（不要优量汇）
            plan.loc[:, ['image_id','label_ids','image_score','ad_account_id']] = data[['image_id','label_ids','image_score','ad_account_id']].reset_index(drop=True)

            return plan

        df_ = req_site(df, plans)

        return df_
        

    def __call__(self, plan):
        """ 组合类函数 """
        # 默认值
        plan['game_id'] = cfg.GDT_DG_GAME
        plan['platform'] = 1
        
        self.df = self.df_[self.df_['label'] == 1]  ## 所有好计划做为有效采样集

        # TimeSeries字段 - 概率采样 self.df_create
        for col in self.gdt_time_series_cols:
            count_df = pd.DataFrame(data=self.df_create[col].value_counts()).reset_index()
            count_df.columns = ['col', 'counts']
            count_df['pct'] = count_df['counts'] / count_df['counts'].sum()
            plan[col] = plan.apply(lambda x: np.random.choice(count_df['col'].values, 1, p=count_df['pct'].values)[0], axis=1)
        
        # 出价相关字段 - 随机采样 self.df_create
        orient_df = self.df_create[self.gdt_orient_cols_bid].sample(n=plan.shape[0], replace=True).reset_index(drop=True)
        plan = pd.concat([plan, orient_df], axis=1)

        # 扩展定向字段 - 随机采样 self.df_create
        orient_df = self.df_create[self.gdt_orient_cols_expand].sample(n=plan.shape[0], replace=True).reset_index(drop=True)
        plan = pd.concat([plan, orient_df], axis=1)

        # 其他定向字段 - 随机采样 self.df_create
        orient_df = self.df_create[self.gdt_orient_cols_targeting].sample(n=plan.shape[0], replace=True).reset_index(drop=True)
        plan = pd.concat([plan, orient_df], axis=1)

        # 创意字段 - 随机采样 self.df_create
        orient_df = self.df_create[self.gdt_new_cols].sample(n=plan.shape[0], replace=True).reset_index(drop=True)  ## todo:调整weights参数增加朋友圈的采样比重
        plan = pd.concat([plan, orient_df], axis=1)

        ## TODO:附加：优化目标依赖
        plan = plan.groupby(['image_id']).apply(lambda x:self.site_given(x, self.df_create.copy()))

        #  self.df_create
        plan['create_time'] = pd.to_datetime(pd.datetime.now())
        plan['create_date'] = pd.to_datetime(pd.datetime.now().date())
        plan.reset_index(drop=True, inplace=True)
        
        # 实时数据训练
        plan_prob = train(self.df_, self.df_image, plan, cfg)
        print("[INFO]: 实时计划训练完毕！")  ## 测试用日志

        return plan, plan_prob


if __name__ == "__main__":
    # todo:脚本测试通过！
    plan_creator = NewCreatePlan()
    plan = plan_creator()
