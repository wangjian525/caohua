# -*- coding:utf-8 -*-
"""
   File Name：     create_plan_dt_mr.py
   Description :   新建计划：动态计划 - 素材&计划配比（广点通末日IOS）
   Author :        royce.mao
   date：          2021/10/11 20:25
"""

import pandas as pd
import numpy as np
import json
import ast
# os.chdir(os.path.dirname(__file__))
from impala.dbapi import connect
from impala.util import as_pandas
import random
import logging

import warnings
warnings.filterwarnings('ignore')
logger = logging.getLogger('CreatePlanGdtDtIOS')

from utils.get_data_mr_gdt_ios import *

#
# 打包接口
#
class GDTCreatePlanDtIOS:
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
        self.new_create_plan.plan_create()
        ret = json.dumps({"code": 200, "msg": "success!", "data": "create plan is success"})
        return ret


class NewCreatePlan(object):
    """ 步骤3：新计划创建类 """
    def __init__(self, ):
        self.ad_account_id_gdt_mr = [9218, 9219, 9220, 9221, 9224, 9225, 9226, 9227]
        self.amount_thresh = 50000  ## TODO:大盘日消耗阈值
        # self.amount_thresh_my = 10000  ## TODO:个人盘日消耗阈值(没数据)
        self.plan_assoc = FeaAssoc()

    def base_market(self,):
        """[逻辑：大盘决定 基准配比值 & 素材量]

        Returns:
            [float]: [基准配比值，素材量]
        """
        for day in range(3, 31):
            sum_amount, sum_roi, sum_cost = get_roi(0, day, day) 
            sum_roi = sum_roi * 100
            if sum_amount > self.amount_thresh*day or day >= 30:  ## 大盘达标 or 近30日大盘
                break
        base_prop, base_num = 3, 10
        # 规则1：大盘 决定 配比基准值 & 素材基准量
        if sum_cost != 0:
            if (sum_roi <= 0.5) & (sum_cost >= 600):
                base_prop, base_num =2, 5 
            elif ((sum_roi <= 0.5) & (sum_cost < 600)) or ((sum_roi <=0.8) & (sum_cost >= 400)):
                base_prop, base_num =3, 10
            elif ((sum_roi <= 0.8) & (sum_cost < 400)) or ((sum_roi <=1.0) & (sum_cost >= 300)):
                base_prop, base_num =4, 15
            elif ((sum_roi <= 1.0) & (sum_cost < 300)) or ((sum_roi <= 1.5) & (sum_cost >= 200)):
                base_prop, base_num =5, 20
            elif ((sum_roi <= 1.5) & (sum_cost < 200)) or ((sum_roi <= 2.0) & (sum_cost >= 100)):
                base_prop, base_num =6, 25
            else:
                base_prop, base_num =7, 30

        # 规则2：个人盘 修正 极端roi的情况
        for day in range(3, 31):
            sum_amount, sum_roi, sum_cost = get_roi(0, day, day) 
            sum_roi = sum_roi * 100
            if sum_amount > self.amount_thresh*day or day >= 30:
                if sum_roi > 5.0 or sum_roi < 0.3:
                    base_prop, base_num = 3, 10   ## 个人数据异常高、低，不稳定时，采取保守策略
                break
        print("基准配比：", base_prop, "基准素材量：", base_num)
        return base_prop, base_num

    def image_candidate(self,):
        """[逻辑：素材分级 近期评分 & 近期递补]

        Returns:
            [list]: [备选素材]
        """
        # 大盘基准
        base_prop, base_num = self.base_market()

        # 素材储备
        ## 评分素材
        image_pf = np.intersect1d(get_score_image(), get_good_image())  ## 评分计划需满足：1）历史有过600评分，且当前评分580以上 and 2）有付费
        print("评分素材量：", len(image_pf))
        pf = pd.DataFrame(image_pf, columns=['image_id'])
        ## 近期递补 - 大R
        image_dr = get_R_image()  ## 大R计划需满足：1）消耗4000以上 and 2）平均付费金额1000以上 or 3）存在单用户付费金额3000以上
        print("大R素材量：", len(image_dr))
        dr = pd.DataFrame(image_dr, columns=['image_id'])
        ## 近期递补 - 中量低付费
        image_lz = get_lz_image()  ## 有量低付费素材需满足：1）素材总消耗10000以上 and 2）素材总回款1个点以下 and 3）素材有18的付费
        print("起量低付费素材量：", len(image_lz))
        lz = pd.DataFrame(image_lz, columns=['image_id'])
        ## 近期递补 - 纯高量
        image_lg = get_lg_image()  ## 纯高量素材需满足：1）素材总消耗100000以上
        print("纯高量素材量", len(image_lg))
        lg = pd.DataFrame(image_lg, columns=['image_id'])
        print("总素材量：", len(list(set(image_pf.tolist() + image_dr.tolist() + image_lz.tolist() + image_lg.tolist()))), "\n", list(set(image_pf.tolist() + image_dr.tolist() + image_lz.tolist() + image_lg.tolist())))
        # 素材加权
        dr['weight'], pf['weight'], lg['weight'], lz['weight'] = 6, 8, 4, 2  ## 初始权重（可调）
        df = dr.append(pf).append(lg).append(lz)
        df.reset_index(drop=True, inplace=True)
        df['weight'] = df.groupby('image_id')['weight'].transform('max')  ## TODO:素材分级加权
        df['image_id'] = df['image_id'].astype(int)
        df.drop_duplicates(inplace=True)
        df.reset_index(drop=True, inplace=True)
        # print("分级加权结果", len(df), "\n", df)

        df = pd.merge(df, get_baseimage_amount(df['image_id'].values), on=['image_id'], how='left')  ## 2选1：平衡大盘消耗
        # df['image_run_date_amount'] = df['image_id'].apply(get_amount_perimage_my)  ## 2选1：平衡个人盘消耗
        df['weight'] = df.apply(lambda x:x.weight * (1 - x.image_run_date_amount / df['image_run_date_amount'].max()) if x.weight > 1 else x.weight, axis=1)  ## TODO:素材平衡消耗的降权
        df = df.sample(n=base_num, weights=df['weight']).reset_index(drop=True)  ## 加权采样
        print("素材池结果", "\n", df)
        
        return base_prop, df['image_id'].values

    def prop_personal(self,):
        """[逻辑：不同分级素材的个性化配比值]

        Returns:
            [dataframe]: [素材及素材配比]
        """
        # 候选素材
        base_prop, base_image = self.image_candidate()

        # 评分区间分组
        image_score = get_baseimage_score(base_image)
        image_score.reset_index(drop=True, inplace=True)
        image_score['score_bin'] = pd.cut(image_score['score'], bins=[0, 500, 520, 550, 570, 600, 1000], labels=[1, 2, 3, 4, 5, 6])
        image_score['prop'] = image_score['score_bin'].map({1:base_prop-2, 2:base_prop-1, 3:base_prop, 4:base_prop, 5:base_prop+1, 6:base_prop+2})  ## TODO:个性化配比
        print("个性化配比结果：", "\n", image_score[['image_id', 'score', 'prop']])
        return image_score[['image_id','prop']]  ## 素材ID、个性化配比值

    @staticmethod
    def plan_field(plan):
        """[逻辑：字段类型修正]

        Returns:
                [dataframe]: [新建计划]
        """
        plan = plan.drop(['create_time', 'create_date', 'label', 'prop'], axis=1)
        # 默认值
        plan['game_id'] = 1001446
        plan['platform'] = 2
        plan['op_id'] = 13678
        plan['flag'] = 'CESHI_GDT_MR_IOS'
        plan['game_name'] = '曙光信仰'
        plan['promoted_object_id'] = '1573087927'
        plan['operation'] = 'disable'
        plan['ad_account_id'] = plan['ad_account_id'].apply(lambda x:str(int(x)))
        # plan['platform'] = plan['platform'].map({1: ['ANDROID'], 2: ['IOS']})
        ## [SITE_SET_WECHAT]公众号和取值范围在1480、560、720、721、1064五种中adcreative_template_id小程序
        plan = plan[~((plan['site_set'] == "['SITE_SET_WECHAT']") & \
                                    (~plan['adcreative_template_id'].isin([1480, 560, 720, 721, 1064])))]
        ## 落地页ID
        plan['page_spec'] = plan.apply(
        lambda x: {'override_canvas_head_option': 'OPTION_CREATIVE_OVERRIDE_CANVAS',
                   'page_id': ''} if x.site_set == "['SITE_SET_MOMENTS']"
        else ({'override_canvas_head_option': 'OPTION_CREATIVE_OVERRIDE_CANVAS',
               'page_id': ''} if x.site_set == "['SITE_SET_WECHAT']" else np.nan), axis=1)
        plan['link_page_spec'] = plan.apply(
        lambda x: {'page_id': ''} if x.site_set == "['SITE_SET_MOMENTS']"
        else ({'page_id': ''} if x.site_set == "['SITE_SET_WECHAT']" else np.nan), axis=1)

        ## 朋友圈头像ID
        plan_1 = plan[plan['site_set'] == "['SITE_SET_MOMENTS']"]
        plan_2 = plan[plan['site_set'] != "['SITE_SET_MOMENTS']"]
        profile_id_dict = {'9217': '', '9218': '', '9219': '', '9220': '', '9221': '',
                           '9223': '', '9224': '', '9225': '', '9226': '', '9227': ''}
        plan_1['profile_id'] = plan_1['ad_account_id'].map(profile_id_dict)
        plan = plan_1.append(plan_2)
        plan.reset_index(drop=True, inplace=True)
        ## 目标定向字段映射
        plan['intention'] = plan['intention_targeting_tags'].apply(lambda x: {'targeting_tags': x})
        plan.drop(['intention_targeting_tags'], axis=1, inplace=True)
        ## 兴趣定向组合json
        plan['interest'] = plan.apply(lambda x:json.loads(
            x[['interest_category_id_list','interest_keyword_list']].rename(index={
               'interest_category_id_list': 'category_id_list',
               'interest_keyword_list': 'keyword_list'
               }).to_json()), axis=1)
        plan.drop(['interest_category_id_list', 'interest_keyword_list'], axis=1, inplace=True)
        ## 行为定向组合json
        plan['behavior'] = plan.apply(lambda x:json.loads(
            x[['behavior_category_id_list','behavior_intensity','behavior_keyword_list','behavior_scene','behavior_time_window']].rename(index={
               'behavior_category_id_list': 'category_id_list',
               'behavior_intensity': 'intensity',
               'behavior_keyword_list': 'keyword_list',
               'behavior_scene': 'scene',
               'behavior_time_window': 'time_window'
               }).to_json()), axis=1)
        plan.drop(['behavior_category_id_list', 'behavior_intensity', 'behavior_keyword_list', 'behavior_scene',
                          'behavior_time_window'], axis=1, inplace=True)
        plan['behavior'] = plan['behavior'].apply(lambda x: [x])
        ## 排除定向组合json
        plan['excluded_converted_audience'] = plan.apply(lambda x:json.loads(
            x[['conversion_behavior_list', 'excluded_dimension']].to_json()), axis=1)
        plan.drop(['conversion_behavior_list', 'excluded_dimension'], axis=1, inplace=True)
        ## 地域定向组合json
        plan['geo_location'] = plan.apply(lambda x:json.loads(
            x[['regions', 'location_types']].to_json()), axis=1)
        plan.drop(['regions', 'location_types'], axis=1, inplace=True)
        ## 其他定向组合json
        plan['targeting'] = plan.apply(lambda x:json.loads(
            x[['device_price', 'app_install_status', 'gender', 'game_consumption_level', 'age', 'network_type', 'excluded_converted_audience', 'geo_location', 'intention', 'interest', 'behavior']].to_json()), axis=1)
        plan.drop(['device_price', 'app_install_status', 'gender', 'game_consumption_level', 'age', 'network_type', 'excluded_converted_audience', 'geo_location', 'intention', 'interest', 'behavior'], axis=1, inplace=True)
        plan['targeting'] = plan['targeting'].apply(lambda x:location_type(x))

        plan['ad_account_id'] = plan['ad_account_id'].astype(int)
        plan['site_set'] = plan['site_set'].apply(lambda x:ast.literal_eval(x) if x != "nan" and not pd.isnull(x) else [])
        ## 时段周三周四更新凌晨不跑计划
        plan['time_series'] = plan['time_series'].apply(
            lambda x: x[0:96] + '1111111111000000000011' + x[118:144] + '1111111111000000000011' + x[166:])
        ## 不限预算
        plan['budget_mode'] = 0
        plan['daily_budget'] = 0
        
        ## TODO:创意中的子素材ID同步原素材ID
        plan['adcreative_elements'] = plan.apply(lambda x:image_elements(x), axis=1)
        plan.dropna(subset=['adcreative_elements'], inplace=True)  ## 如果没有对应尺寸的新素材直接pass
        
        plan.drop('page_type', axis=1, inplace=True)

        return plan


    def plan_create(self, ):
        """[逻辑：计划 账号素材资源分配 + 字段特征组合 + 字段类型修正]

       Returns:
            [dataframe]: [新建计划]  
        """
        # 素材 & 素材下的计划配比值
        image_prop = self.prop_personal()

        # 计划初始化
        plan = pd.DataFrame(image_prop, columns=['image_id','prop'])
        plan = plan.apply(lambda x:np.repeat(np.expand_dims(x.values, 0), x.prop, axis=0) ,axis=1)
        plan = pd.DataFrame(np.concatenate(plan), columns=['image_id','prop'])
        print("计划总数量：", len(plan))
        
        # 计划账号素材搭配
        # account_plannum = get_plan_num_peraccount(self.ad_account_id_gdt_mr)
        # account_plannum['weight'] = account_plannum.apply(lambda x:1 - x.plan_num / account_plannum['plan_num'].sum(), axis=1)  ## TODO:账号根据已建计划数量均衡配比
        # ad_account_id = account_plannum['ad_account_id'].sample(n=len(plan), weights=account_plannum['weight'])  ## 加权采样
        account_roi = pd.DataFrame({'ad_account_id':self.ad_account_id_gdt_mr, 'roi':[get_roi_peraccount_my(ad) * 100 if get_roi_peraccount_my(ad) > 0 else 1 for ad in self.ad_account_id_gdt_mr]})
        print("账号加权结果：", "\n", account_roi)
        ad_account_ids = account_roi['ad_account_id'].sample(n=len(plan), replace=True, weights=account_roi['roi'])  ## TODO:账号根据roi表现加权配比（固定权重配比优化）
        print("账号分布结果：", "\n", ad_account_ids.value_counts())
        ad_account_ids = ad_account_ids.values
        random.shuffle(ad_account_ids)
        plan['ad_account_id'] = ad_account_ids  ## TODO:账号配比（熵最大映射优化）

        # 计划字段特征组合
        plan = self.plan_assoc(plan)
        # 计划字段类型修正
        plan = self.plan_field(plan)

        # 请求输出
        print("[INFO]：动态计划创建完毕！")
        plan.to_csv('./plan_result.csv', index=0)  # 保存创建日志
        # plan = plan.iloc[:3]
        # get_ad_create(plan)
        

class FeaAssoc(object):
    """ 步骤2：计划特征组合类 """
    def __init__(self, ):
        self.gdt_time_series_cols = ['time_series']
        self.gdt_orient_cols_bid = ['optimization_goal', 'bid_strategy', 'bid_amount', 'deep_conversion_type', 'deep_conversion_behavior_spec', 'deep_conversion_worth_spec']  ## 出价方式
        self.gdt_orient_cols_expand = ['expand_enabled', 'expand_targeting']
        self.gdt_orient_cols_targeting = ['device_price', 'app_install_status', 'gender', 'game_consumption_level', 'age', 'network_type',
                                        'conversion_behavior_list', 'excluded_dimension', 'location_types', 'regions',
                                        'intention_targeting_tags', 'interest_category_id_list', 'interest_keyword_list',
                                        'behavior_category_id_list', 'behavior_intensity',
                                        'behavior_keyword_list', 'behavior_scene', 'behavior_time_window']
        
        self.gdt_new_cols = ['site_set', 'deep_link_url', 'adcreative_template_id', 'page_spec', 'page_type', 'link_page_spec',
                             'link_name_type', 'link_page_type', 'promoted_object_id', 'profile_id', 'promoted_object_type',
                             'automatic_site_enabled', 'label', 'adcreative_elements']

        self.df_create = DataRealTime()()
        self.df_create = self.df_create[self.df_create['game_id'].isin([1001444, 1001446, 1001447])]
        print("[INFO]: 近期优质计划数据提取完毕！")

    def __call__(self, plan):
        """ 类函数 """
        # TimeSeries字段 - 概率采样 self.df_create
        for col in self.gdt_time_series_cols:
            count_df = pd.DataFrame(data=self.df_create[col].value_counts()).reset_index()
            count_df.columns = ['col', 'counts']
            count_df['pct'] = count_df['counts'] / count_df['counts'].sum()
            plan[col] = plan.apply(lambda x: np.random.choice(count_df['col'].values, 1, p=count_df['pct'].values)[0], axis=1)
        
        # TODO:跑法相关字段 - 增加权重配比
        # print(self.df_create['optimization_goal'].value_counts())
        self.df_create['weight'] = self.df_create.groupby(['optimization_goal'])['image_id'].transform('count')
        orient_df = self.df_create[self.gdt_orient_cols_bid].sample(n=plan.shape[0], weights=self.df_create['weight']).reset_index(drop=True)  ## TODO:固定权重配比优化
        plan = pd.concat([plan, orient_df], axis=1)

        # 扩展定向字段 - 随机采样
        orient_df = self.df_create[self.gdt_orient_cols_expand].sample(n=plan.shape[0], replace=True).reset_index(drop=True)
        plan = pd.concat([plan, orient_df], axis=1)

        # 其他定向字段 - 随机采样
        orient_df = self.df_create[self.gdt_orient_cols_targeting].sample(n=plan.shape[0], replace=True).reset_index(drop=True)
        plan = pd.concat([plan, orient_df], axis=1)

        # TODO:版位相关字段 - 增加权重配比
        self.df_create['site_set'] = self.df_create['site_set'].map(str)
        # print(self.df_create['site_set'].value_counts())
        self.df_create['weight'] = self.df_create.groupby(['site_set'])['image_id'].transform('count')
        orient_df = self.df_create[self.gdt_new_cols].sample(n=plan.shape[0], weights=self.df_create['weight']).reset_index(drop=True)  ## TODO:固定权重配比优化
        plan = pd.concat([plan, orient_df], axis=1)

        plan['create_time'] = pd.to_datetime(pd.datetime.now())
        plan['create_date'] = pd.to_datetime(pd.datetime.now().date())
        plan.reset_index(drop=True, inplace=True)

        return plan


class DataRealTime(object):
    """ 步骤1：优质计划采集类 """
    def __init__(self, ):
        self.midgame_id = 1056
 
    @staticmethod
    def get_plan_info(game_id):
        game_id = list(map(lambda x: x['game_id'], game_id))
        game_id = [str(i) for i in game_id]
        game_id = ','.join(game_id)
        conn = pymysql.connect(host='db-slave-modeltoufang-001.ch', port=3306, user='model_read',
                               passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8', db='db_ptom')
        cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
        sql = '''
        /*手动查询*/
            SELECT
                    p.*,
                    b.image_id
                FROM
                    db_ptom.ptom_third_plan p
                left join 
                    db_ptom.ptom_plan b
                on p.plan_id=b.plan_id
                WHERE
                    p.game_id IN ({})
                    AND p.media_id = 16
                    AND p.platform = 2
                    AND p.create_time >= date( NOW() - INTERVAL 720 HOUR )
                    AND p.create_time <= date(NOW())
                                AND p.plan_id >= (
                                    select plan_id from db_ptom.ptom_plan
                                    where create_time >= date( NOW() - INTERVAL 720 HOUR )
                                    and create_time <= date( NOW() - INTERVAL 696 HOUR )
                                    limit 1
                                )
        '''
        finalSql = sql.format(game_id)
        result_df = pd.read_sql(finalSql, conn)
        cur.close()
        conn.close()
        return result_df

    @staticmethod
    def get_image_info():
        conn = pymysql.connect(host='db-slave-modelfenxi-001.ch', port=3306, user='model_read',
                               passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8')
        cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
        sql = '''
            SELECT
                a.chl_user_id AS channel_id,
                a.source_id,
                a.image_id,
                b.label_ids
            FROM
                db_data_ptom.ptom_plan a
                LEFT JOIN
                db_data_ptom.ptom_image_info b
            on a.image_id = b.image_id
            WHERE
                a.game_id IN ( SELECT dev_game_id AS game_id FROM db_data.t_game_config WHERE game_id = 1056 AND dev_game_id IS NOT NULL ) 
                AND a.media_id = 16
                AND a.platform = 2
                AND a.create_time >= date( NOW() - INTERVAL 720 HOUR ) 
            GROUP BY
                a.chl_user_id,
                a.source_id,
                a.image_id
        '''
        cur.execute(sql)
        result_df = pd.read_sql(sql, conn)
        cur.close()
        conn.close()
        return result_df

    @staticmethod
    def get_creative(game_id):
        game_id = list(map(lambda x: x['game_id'], game_id))
        game_id = [str(i) for i in game_id]
        game_id = ','.join(game_id)

        conn = pymysql.connect(host='db-slave-modeltoufang-001.ch', port=3306, user='model_read',
                               passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8')
        cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
        sql = '''
            /*手动查询*/ 
            SELECT
                b.chl_user_id AS channel_id,
                b.source_id,
                b.image_id,
                a.creative_param
            FROM
                db_ptom.ptom_batch_ad_task a
                LEFT JOIN db_ptom.ptom_plan b ON a.plan_name = b.plan_name 
            WHERE
                a.media_id = 16
                AND b.create_time >= date( NOW() - INTERVAL 720 HOUR )
                AND a.game_id IN ({})
                AND b.image_id is not null
        '''
        finalSql = sql.format(game_id)
        cur.execute(finalSql)
        result = pd.read_sql(finalSql, conn)
        cur.close()
        conn.close()
        return result

    @staticmethod
    def get_now_plan_roi(game_id):
        game_id = list(map(lambda x: x['game_id'], game_id))
        game_id = [str(i) for i in game_id]
        game_id = ','.join(game_id)

        conn = pymysql.connect(host='db-slave-modelfenxi-001.ch', port=3306, user='model_read',
                               passwd='aZftlm6PcFjN{DxIKOPr)BcutuJd<uYOC0P<8')
        cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
        sql = '''
            SELECT
                a.ad_account_id,
                b.channel_id,
                b.source_id,
                b.tdate,
                b.amount,
                b.new_role_money,
                b.new_role_money / b.amount AS roi,
                b.pay_role_user_num / b.create_role_num AS pay_rate 
            FROM
                db_data_ptom.ptom_plan a 
            left join
                db_stdata.st_lauch_report b
            on a.chl_user_id=b.channel_id and a.source_id=b.source_id

            WHERE
                b.tdate >= date( NOW() - INTERVAL 720 HOUR )
                AND a.platform = 2
                AND b.tdate_type = 'day'
                AND b.media_id = 16
                AND b.game_id IN ({})
                AND b.amount >= 500
                AND b.pay_role_user_num >= 1
                AND b.new_role_money >= 60
                AND (b.new_role_money / b.amount)>=0.01
        '''
        finalSql = sql.format(game_id)
        cur.execute(finalSql)
        result_df = pd.read_sql(finalSql, conn)
        cur.close()
        conn.close()
        result_df['tdate'] = pd.to_datetime(result_df['tdate'])
        result_df = result_df.sort_values('tdate')
        result_df = result_df.drop_duplicates(['channel_id', 'source_id'], keep='first')
        # result_df = result_df[result_df['roi'] >= 0.03]
        return result_df

    @staticmethod
    def get_plan_json(plan_info):
        plan_info.drop(['inventory_type', 'budget', 'bid_mode'], axis=1, inplace=True)
        plan_info.dropna(how='all', inplace=True, axis=1)
        plan_info.dropna(subset=['ad_info'], inplace=True)

        plan_info['ad_info'] = plan_info['ad_info'].apply(json.loads)
        temp = plan_info['ad_info'].apply(pd.Series)
        plan_info = pd.concat([plan_info, temp], axis=1)
        plan_info.drop('ad_info', axis=1, inplace=True)

        temp = plan_info['targeting'].apply(pd.Series)
        plan_info = pd.concat([plan_info, temp], axis=1)
        plan_info.drop('targeting', axis=1, inplace=True)

        temp = plan_info['deep_conversion_spec'].apply(pd.Series)
        plan_info = pd.concat([plan_info, temp], axis=1)
        plan_info.drop('deep_conversion_spec', axis=1, inplace=True)

        temp = plan_info['behavior_or_interest'].apply(pd.Series)
        plan_info = pd.concat([plan_info, temp], axis=1)
        plan_info.drop('behavior_or_interest', axis=1, inplace=True)
        plan_info.drop(0, axis=1, inplace=True)

        temp = plan_info['intention'].apply(pd.Series)
        plan_info = pd.concat([plan_info, temp], axis=1)
        plan_info.drop('intention', axis=1, inplace=True)
        plan_info = plan_info.rename(columns={'targeting_tags': 'intention_targeting_tags'})
        plan_info.drop(0, axis=1, inplace=True)

        temp = plan_info['interest'].apply(pd.Series)
        plan_info = pd.concat([plan_info, temp], axis=1)
        plan_info.drop('interest', axis=1, inplace=True)
        plan_info = plan_info.rename(
            columns={'category_id_list': 'interest_category_id_list', 'keyword_list': 'interest_keyword_list',
                    'targeting_tags': 'interest_targeting_tags'})
        plan_info.drop(0, axis=1, inplace=True)

        temp = plan_info['behavior'].apply(pd.Series)
        plan_info = pd.concat([plan_info, temp], axis=1)
        plan_info.drop('behavior', axis=1, inplace=True)
        temp = plan_info[0].apply(pd.Series)
        plan_info = pd.concat([plan_info, temp], axis=1)
        plan_info.drop(0, axis=1, inplace=True)
        plan_info = plan_info.rename(columns={'category_id_list': 'behavior_category_id_list',
                                            'intensity': 'behavior_intensity',
                                            'keyword_list': 'behavior_keyword_list',
                                            'scene': 'behavior_scene',
                                            'targeting_tags': 'behavior_targeting_tags',
                                            'time_window': 'behavior_time_window'})

        temp = plan_info['excluded_converted_audience'].apply(pd.Series)
        plan_info = pd.concat([plan_info, temp], axis=1)
        plan_info.drop('excluded_converted_audience', axis=1, inplace=True)
        plan_info.drop(0, axis=1, inplace=True)

        temp = plan_info['geo_location'].apply(pd.Series)
        plan_info = pd.concat([plan_info, temp], axis=1)
        plan_info.drop('geo_location', axis=1, inplace=True)
        plan_info.drop(0, axis=1, inplace=True)

        # 过滤一对多计划
        plan_info['ad_id_count'] = plan_info.groupby('plan_id')['ad_id'].transform('count')
        plan_info = plan_info[plan_info['ad_id_count'] == 1]

        # 删除纯买激活的计划
        plan_info = plan_info[~((plan_info['deep_conversion_type'].isna()) & (
                plan_info['optimization_goal'] == 'OPTIMIZATIONGOAL_APP_ACTIVATE'))]
        # 删除auto_audience=True 的记录，并且删除auto_audience字段
        plan_info[plan_info['auto_audience'] == False]
        plan_info = plan_info[['ad_account_id', 'game_id', 'channel_id', 'source_id', 'budget_mode',
                            'create_time', 'image_id', 'optimization_goal', 'time_series',
                            'bid_strategy', 'bid_amount', 'daily_budget', 'expand_enabled',
                            'expand_targeting', 'device_price', 'app_install_status',
                            'gender', 'game_consumption_level', 'age', 'network_type',
                            'deep_conversion_type',
                            'deep_conversion_worth_spec', 'intention_targeting_tags',
                            'interest_category_id_list', 'interest_keyword_list',
                            'behavior_category_id_list',
                            'behavior_intensity', 'behavior_keyword_list', 'behavior_scene',
                            'behavior_time_window',
                            'conversion_behavior_list', 'excluded_dimension', 'location_types',
                            'regions']]
        plan_info['deep_conversion_behavior_spec'] = np.nan
        return plan_info


    def __call__(self,):
        """ 采集 """
        game_id_dict = get_game_id(self.midgame_id)

        plan_info = self.get_plan_info(game_id_dict)
        image_info = self.get_image_info()

        creative_info = self.get_creative(game_id_dict)
        now_plan_roi = self.get_now_plan_roi(game_id_dict)

        # 计划
        plan_info = self.get_plan_json(plan_info)
        # 素材
        image_info.dropna(subset=['image_id'], inplace=True)
        image_info['image_id'] = image_info['image_id'].astype(int)
        # 创意
        creative_info['creative_param'] = creative_info['creative_param'].apply(json.loads)
        temp = creative_info['creative_param'].apply(pd.Series)
        temp = temp.drop('image_id', axis=1)
        creative_info = pd.concat([creative_info, temp], axis=1)
        creative_info.drop('creative_param', axis=1, inplace=True)
        creative_info.drop(['title', 'adcreative_template_parent', 'idea_type', 'adcreative_name', 'ideaName', '_creative'],
                        axis=1, inplace=True)
        creative_info['adcreative_elements_array'] = creative_info['adcreative_elements_array'].apply(lambda x: x[0])
        creative_info['adcreative_elements'] = creative_info['adcreative_elements_array'].apply(
            lambda x: x['adcreative_elements'])
        creative_info.drop('adcreative_elements_array', axis=1, inplace=True)
        creative_info = creative_info[['channel_id', 'source_id', 'image_id', 'deep_link_url',
                                    'adcreative_template_id', 'page_spec', 'page_type', 'site_set', 'label',
                                    'promoted_object_id', 'promoted_object_type', 'automatic_site_enabled',
                                    'link_name_type', 'link_page_type', 'profile_id', 'link_page_spec',
                                    'adcreative_elements']]
        plan_info = pd.merge(plan_info.drop(['image_id'], axis=1), creative_info, on=['channel_id', 'source_id'],
                            how='inner')

        plan_info.dropna(subset=['image_id'], inplace=True)
        plan_info['image_id'] = plan_info['image_id'].astype(int)
        plan_info['adcreative_template_id'] = plan_info['adcreative_template_id'].astype(int)
        plan_info['profile_id'] = plan_info['profile_id'].apply(lambda x: x if x != x else str(int(x)))
        plan_info['create_time'] = pd.to_datetime(plan_info['create_time'])
        plan_info_current = plan_info[plan_info['create_time'] >= pd.datetime.now() - pd.DateOffset(45)]
        image_info = image_info[image_info['image_id'].notna()]
        image_info['image_id'] = image_info['image_id'].astype(int)

        # 近期优质计划df_create
        df_create = pd.merge(plan_info_current, image_info, on=['channel_id', 'source_id', 'image_id'], how='left')
        df_create = pd.merge(df_create, now_plan_roi.drop(['ad_account_id'], axis=1), on=['channel_id', 'source_id'],
                            how='inner')

        # df_create = df_create[df_create['site_set'].notna()]  ## TODO:支持IOS自动版位
        df_create.reset_index(drop=True, inplace=True)

        return df_create
