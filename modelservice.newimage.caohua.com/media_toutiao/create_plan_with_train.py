# -*- coding:utf-8 -*-
"""
   File Name：     create_plan_with_train.py
   Description :   新建计划：计划属性自动化组合（末日）
   Author :        royce.mao
   date：          2021/6/15 10:05
"""

import pandas as pd
import numpy as np
import json
import ast
# os.chdir(os.path.dirname(__file__))
import pymysql
from impala.dbapi import connect
from impala.util import as_pandas
import random
import logging

import warnings
warnings.filterwarnings('ignore')
logger = logging.getLogger('CreatePlan')

from media_toutiao.script.trainer import train
from media_toutiao.script.printout import get_plan_online  ## 格式化输出
from media_toutiao.script.realtime_df import DataRealTime  ## 采集实时数据类

from config import get_var
dicParam = get_var()

#
# 打包接口
#
class TTCreatePlan:
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
        plan = self.new_create_plan()
        ret = json.dumps({"code": 200, "msg": "success!", "data": "create plan is success"})
        return plan


class NewCreatePlan(object):
    """ 新计划创建类 """
    def __init__(self, ):
        self.fea_assoc = FeaAssoc()

    @staticmethod
    def find_new_images(game_ids):
        """[新素材筛选 + 实时入库]

        Returns:
        """
        conn2 = pymysql.connect(host=dicParam['DB_SLAVE_TOUFANG_HOST'], port=int(dicParam['DB_SLAVE_TOUFANG_PORT']), user=dicParam['DB_SLAVE_TOUFANG_USERNAME'],
                                passwd=dicParam['DB_SLAVE_TOUFANG_PASSWORD'], db=dicParam['DB_SLAVE_TOUFANG_DATABASE'])
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
                    a.create_time >= date(NOW() - INTERVAL 72 HOUR)
                    AND a.label_ids not in ('27','279','303')
                    AND image_name like 'SSR%'
            '''
        cur2.execute(sql)
        image_df = pd.read_sql(sql, conn2)
        image_df['label_ids'] = image_df['label_ids'].str.replace( ',', ';')  ## 避免分隔符冲突
        image_df['game_ids'] = image_df['game_ids'].str.replace( ',', ';')  ## 避免分隔符冲突
        # 
        game_id = [str(i) for i in game_ids]
        game_id = ','.join(game_id)
        conn3 = pymysql.connect(host=dicParam['DB_SLAVE_FENXI_HOST'], port=int(dicParam['DB_SLAVE_FENXI_PORT']), user=dicParam['DB_SLAVE_FENXI_USERNAME'],
                                passwd=dicParam['DB_SLAVE_FENXI_PASSWORD'])
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
                a.create_time >= date(NOW() - INTERVAL 72 HOUR)  ## 近3天消耗
                AND b.tdate >= date(NOW() - INTERVAL 72 HOUR)  ## 近3天消耗
                AND b.tdate_type = 'day' 
                AND b.media_id = 10
                AND b.game_id IN ({})
        '''
        finalSql = sql.format(game_id)
        cur3.execute(finalSql)
        plan_df = pd.read_sql(finalSql, conn3)
        plan_df['tdate'] = pd.to_datetime(plan_df['tdate'])
        plan_df = plan_df.sort_values('tdate')
        plan_df = plan_df.drop_duplicates(['channel_id', 'source_id', 'tdate'], keep='first')

        # 关闭链接
        cur2.close(), conn2.close()
        cur3.close(), conn3.close()

        # TODO:3步走过滤要测的新素材（限定 游戏ID 标签ID 未建计划 未消耗满3000）
        # TODO:交集的game_ids
        game_ids = [str(game_id) for game_id in game_ids]
        image_df = image_df.loc[image_df.apply(lambda x:True if not pd.isnull(x['game_ids']) and len(list(set(game_ids).intersection(set(x['game_ids'].split(';'))))) > 0 else False, axis=1)]

        # TODO:新素材选择逻辑
        image_df_new = image_df.loc[image_df.apply(lambda x:True if not any(plan_df['image_id'] == x.image_id) else False, axis=1)]
        print("未建计划素材量:", len(image_df_new))  ## 未建过计划 新素材数量

        # TODO:返素材选择逻辑
        amount_df = plan_df.groupby('image_id')['amount'].sum().reset_index()  # 总消耗
        count_df = plan_df.groupby('image_id')['amount'].count().reset_index(name="counts")
        plan_df = pd.merge(count_df, amount_df, on="image_id")

        plan_df = plan_df.loc[plan_df['amount'] < 3000]
        image_df_old = image_df.loc[image_df['image_id'].isin(np.intersect1d(plan_df['image_id'].values, image_df['image_id'].values))]
        print("3000返新素材量:", len(image_df_old))  ## 返新素材数量

        image_df = pd.concat([image_df_new, image_df_old], ignore_index=True, join='inner')

        # ==============临时: 只5条
        image_df_dq = image_df[image_df['label_ids'].apply(lambda x:True if '277' in x else False)]
        if len(image_df_dq) >= 5:
            image_df = image_df_dq.reset_index(drop=True)
        else:
            image_df_lh = image_df[image_df['label_ids'].apply(lambda x:True if '20' in x else False)]
            image_df = image_df_dq.append(image_df_lh.sample(5-len(image_df_dq)))
            image_df = image_df.reset_index(drop=True)
        # ==============

        image_df['label_ids'] = image_df['label_ids'].astype(str)

        return image_df
        
    @staticmethod
    def plan_attr_assoc(image_group, ad_account_id_group):
        """ [计划资源分配]

        Args:
            score_image_group ([dataframe]): [新素材]
            ad_account_id_group ([list]): [账号集]
            columns ([list]): [新建计划涉及的字段组合]
            capacity ([int]): [账号的容积计划数量限制]
        """
        def filter_exclude_image(image_ids, media_id):
            """ 过滤违规 """
            image_ids = [str(i) for i in image_ids]
            image_ids = ','.join(image_ids)

            conn = pymysql.connect(host=dicParam['DB_SLAVE_FENXI_HOST'], port=int(dicParam['DB_SLAVE_FENXI_PORT']), user=dicParam['DB_SLAVE_FENXI_USERNAME'],
                                passwd=dicParam['DB_SLAVE_FENXI_PASSWORD'])
            cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
            sql = '''
                SELECT
                    image_id,
                    image_name
                FROM
                    db_data_ptom.ptom_image_info
                WHERE
                    image_id IN ({})
                    AND NOT FIND_IN_SET({}, exclude_media_ids)
            '''
            result = pd.read_sql(sql.format(image_ids, media_id), conn)
            # 关闭链接
            cur.close()
            conn.close()
            return result['image_id'].values.tolist()
        
        def robot_plan_num(image):
            ''' 测新机器人计划数量 '''
            conn = pymysql.connect(host=dicParam['DB_SLAVE_FENXI_HOST'], port=int(dicParam['DB_SLAVE_FENXI_PORT']), user=dicParam['DB_SLAVE_FENXI_USERNAME'],
                                   passwd=dicParam['DB_SLAVE_FENXI_PASSWORD'])
            cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
            sql = '''
            /*手动查询*/
            SELECT
                a.chl_user_id AS channel_id,
                a.source_id AS source_id,
                a.image_id
            FROM
                db_data_ptom.ptom_plan a
            WHERE
                a.create_time >= date(NOW() - INTERVAL 120 HOUR)  ## 最近5天
                AND a.media_id = 10
                AND a.op_id = 13268
                AND a.image_id = {}
            '''
            finalSql = sql.format(image)
            cur.execute(finalSql)
            plan_df = pd.read_sql(finalSql, conn)
            plan_df = plan_df.drop_duplicates(['channel_id', 'source_id'], keep='first')

            return len(plan_df) < 3

        dist_image_ids = image_group['image_id'].value_counts().index
        dist_image_ids = filter_exclude_image(dist_image_ids, 10)
        dist_image_ids = list(filter(lambda x: robot_plan_num(x), dist_image_ids))

        plan = pd.DataFrame()
        for image_id in sorted(dist_image_ids, key=lambda x: random.random()):
            for ad_account in ad_account_id_group:
                plan_part = pd.DataFrame({'ad_account_id': [ad_account], 'image_id': [image_id]})
                plan = plan.append(plan_part)
        
        assert len(plan) > 0, "没有组织起计划！"
        plan = pd.DataFrame(np.repeat(plan.values, 3, axis=0), columns=plan.columns)
        
        return plan

    @staticmethod
    def plan_dtc_screen(plan, plan_prob, ad_accounts, plan_num):
        """[候选计划筛选]

        Args:
        """
        # TODO：筛选
        # 80分位数的概率阈值
        prob_thresh = np.percentile(plan_prob, 80)
        
        # 账号素材分组prob概率最大者
        plan['rank_im_ad'] = plan.groupby(['image_id','ad_account_id'])['prob'].rank(ascending=False, method='first')
        plan_result = plan[plan['rank_im_ad'] <= 1]

        def im_1_ad(plan):
            ''' 每1素材1条计划 - 兼顾账号分配（置信度） '''
            images = []
            ad_accounts_np = np.array(ad_accounts)
            ad_counts = np.array([0] * len(ad_accounts), dtype=int)

            plan_result = pd.DataFrame()
            for _, row in plan.sort_values('prob', ascending=False).iterrows():
                if row['image_id'] not in images and len(ad_accounts) <= 1:
                    plan_result = plan_result.append(row.to_frame().T)
                    images.append(row['image_id'])
                elif row['image_id'] not in images and row['ad_account_id'] not in ad_accounts_np[ad_counts.argsort()[-1:]]:  # 小技巧
                    plan_result = plan_result.append(row.to_frame().T)
                    images.append(row['image_id'])
                    ad_counts[ad_accounts_np == row['ad_account_id']] += 1
                else:
                    continue
            
            return plan_result.reset_index(drop=True)
        
        # 每1素材只保留1条计划
        plan_result = im_1_ad(plan_result)
        
        # 概率阈值过滤
        plan_result = plan_result[plan_result['prob'] >= prob_thresh]
        
        # 账号加权采样过滤 - 20
        plan_result['weight'] = plan_result.groupby(['ad_account_id'])['game_id'].transform('count')
        if plan_result.shape[0] > plan_num:
            plan_result = plan_result.sample(plan_num, weights=plan_result['weight'])

        # 素材计划找回（找prob最高的一条）
        new_image_num = plan_result['image_id'].value_counts()
        for img in np.setdiff1d(plan['image_id'].values, new_image_num.index):
            add_plan = plan[plan['image_id'] == img].sort_values('prob', ascending=False)[:1]
            plan_result = plan_result.append(add_plan)
        plan_result = plan_result.drop(['create_time', 'create_date', 'label_ids', 'prob', 'rank_im_ad', 'weight'], axis=1)

        # TODO：输出
        plan_result['convertIndex'] = plan_result['deep_bid_type'].apply(lambda x: 23 if x == 'BID_PER_ACTION'
                                                                            else 24 if x == 'ROI_COEFFICIENT'
                                                                                else np.nan)
        # 性别限制
        plan_result['gender'] = 'GENDER_MALE'
        # 年龄限制
        plan_result['age'] = plan_result['age'].apply(lambda x: "['AGE_BETWEEN_24_30', 'AGE_BETWEEN_31_40', 'AGE_BETWEEN_41_49']")
        plan_result['age'] = plan_result['age'].apply(ast.literal_eval)

        # 优选广告位
        plan_result['inventory_type'] = plan_result['inventory_type'].map(str)
        plan_result['inventory_type'] = plan_result['inventory_type'].apply(lambda x: "[]" if x ==
                    "['INVENTORY_UNION_SLOT', 'INVENTORY_AWEME_FEED', 'INVENTORY_FEED', 'INVENTORY_UNION_SPLASH_SLOT', "
                    "'INVENTORY_VIDEO_FEED', 'INVENTORY_HOTSOON_FEED', 'INVENTORY_TOMATO_NOVEL']" else x)
        # 穿山甲版位delivery_range 处理
        plan_result['delivery_range'] = plan_result.apply(
            lambda x: 'UNION' if x.inventory_type == "['INVENTORY_UNION_SLOT']" else x.delivery_range, axis=1)
        # 穿山甲版位smart_bid_type 处理
        plan_result['smart_bid_type'] = plan_result.apply(
            lambda x: 'SMART_BID_CUSTOM' if x.inventory_type == "['INVENTORY_UNION_SLOT']" else x.smart_bid_type, axis=1)

        # 只跑优选广告位
        # plan_result['inventory_type'] = plan_result['inventory_type'].apply(lambda x: "[]")
        plan_result['inventory_type'] = plan_result['inventory_type'].apply(ast.literal_eval)

        # 限制手机价格
        plan_result['launch_price'] = plan_result['launch_price'].apply(lambda x: "[1000, 11000]")
        plan_result['launch_price'] = plan_result['launch_price'].apply(ast.literal_eval)

        plan_result['budget'] = plan_result.apply(lambda x: x.budget if x.budget >= x.cpa_bid else x.cpa_bid, axis=1)
        plan_result['budget'] = plan_result['budget'].apply(np.ceil)
        # plan_result['cpa_bid'] = plan_result['cpa_bid'].apply(lambda x: x if x >= 520 else random.randint(400, 480))
        plan_result['cpa_bid'] = plan_result[['cpa_bid','deep_bid_type']].apply(lambda x: x.cpa_bid if x.deep_bid_type == 'BID_PER_ACTION' else x.cpa_bid, axis=1)
        plan_result['operation'] = 'disable'
        plan_result['web_url'] = plan_result['game_id'].apply(lambda x:'https://www.chengzijianzhan.com/tetris/page/7044279938891825160/'
                                                            if x == 1001703 else
                                                            (''
                                                            if x == 1001772 else
                                                            ''))
        plan_result['plan_auto_task_id'] = "11002,10998,12098"
        plan_result['op_id'] = 13268
        plan_result['district'] = 'CITY'
        plan_result['flag'] = plan_result['smart_bid_type'].apply(lambda x:'CEXIN_NB' if x == 'SMART_BID_NO_BID' else 'CEXIN')
        plan_result['ad_account_id'] = plan_result['ad_account_id'].astype(int)
        plan_result['image_id'] = plan_result['image_id'].astype(int)

        # 文案过滤
        plan_result['title_list'] = plan_result['title_list'].map(str)
        # 人群包不限
        # plan_result['retargeting_tags_include'] = [[] for _ in range(len(plan_result))]
        # plan_result['retargeting_tags_exclude'] = [[] for _ in range(len(plan_result))]
        # 周5早上8点开跑

        plan_result.to_csv('./plan_result_new.csv', index=0)  # 保存创建日志

        return plan_result

    
    def __call__(self, ):
        """ 创建类函数 """
        # 提取素材
        new_images = self.find_new_images([1001703,1001772])  # 该方法得到新素材
        print("[INFO]：新素材提取完毕！")

        # 新建计划
        plan_create = self.plan_attr_assoc(new_images, [11597,11599,11598,11600,11596])  # !!!!测新账号
        plan_result, plan_prob = self.fea_assoc(plan_create)  # 全量组合 + 计划训练
        print("[INFO]: 源计划组合完毕！")

        # 过滤计划
        plan_result = self.plan_dtc_screen(plan_result, plan_prob, [11597,11599,11598,11600,11596], 20)  # !!!!测新账号
 
        # 提交计划
        # for col in plan_result.columns:
        #     print(col, type(plan_result[col].iloc[0]))
        # rsp = get_plan_online(plan_result)
        print(plan_result[['ad_account_id','game_id','image_id']])
        print("[INFO]: {}条新计划创建完毕！".format(len(plan_result)))
        return plan_result
        

class FeaAssoc(object):
    """ 计划特征组合类 """
    def __init__(self, ):
        self.game_id1 = 1001703  # 幸存者挑战2

        self.ad_account_id1 = [11597,11599,11598,11600,11596]  # !!!!测新账号

        self.df_create, self.df_train, self.image_info = DataRealTime()()

        # ===============临时: 只优选广告位
        self.df_create = self.df_create[self.df_create['inventory_type'].apply(lambda x:True if len(x) ==7 else False)].reset_index(drop=True)
        # ===============
        print("[INFO]: 历史数据提取完毕！")

    def __call__(self, plan):
        """ 组合类函数 """
        # 默认值
        plan['game_id'] = self.game_id1
        plan['platform'] = 1
        plan['platform'] = plan['platform'].apply(lambda x: '[ANDROID]' if x == 1 else '[IOS]')

        # 选android_osv
        count_df = pd.DataFrame(data=self.df_create['android_osv'].value_counts()).reset_index()
        count_df.columns = ['col', 'counts']
        count_df['pct'] = count_df['counts'] / count_df['counts'].sum()
        plan['android_osv'] = plan['platform'].apply(
            lambda x: 'NONE' if x == '[IOS]' else np.random.choice(count_df['col'].values, 1, p=count_df['pct'].values)[0])

        # 选ios_osv
        count_df = pd.DataFrame(data=self.df_create['ios_osv'].value_counts()).reset_index()
        count_df.columns = ['col', 'counts']
        count_df['pct'] = count_df['counts'] / count_df['counts'].sum()
        plan['ios_osv'] = plan['platform'].apply(
            lambda x: 'NONE' if x == '[ANDROID]' else np.random.choice(count_df['col'].values, 1, p=count_df['pct'].values)[
                0])

        # 选budget
        plan['budget'] = plan['platform'].apply(lambda x: 3300 if x == '[ANDROID]' else 4200)

        # 选'ad_keywords', 'title_list', 'third_industry_id'  创意
        sample_df = self.df_create[['ad_keywords', 'title_list', 'third_industry_id']]
        sample_df = sample_df.sample(n=plan.shape[0], replace=True).reset_index(drop=True)
        plan = pd.concat([plan, sample_df], axis=1)

        # 选'retargeting_tags_include','retargeting_tags_exclude'  人群包定向 版位
        sample_df = self.df_create[
            ['inventory_type', 'retargeting_tags_include', 'retargeting_tags_exclude', 'delivery_range',
            'city',
            'location_type', 'gender', 'age', 'ac', 'launch_price', 'auto_extend_enabled', 'hide_if_exists',
            'hide_if_converted',
            'schedule_time', 'flow_control_mode']]
        sample_df = sample_df.sample(n=plan.shape[0], replace=True).reset_index(drop=True)
        sample_df['inventory_type'] = sample_df['inventory_type'].apply(lambda x: list(filter(None, x)))
        plan = pd.concat([plan, sample_df], axis=1)

        # 选'interest_action_mode','action_scene','action_days','action_categories' ,'interest_categories' 行为兴趣
        sample_df = self.df_create[['interest_action_mode', 'action_scene', 'action_days', 'action_categories',
                            'interest_categories']]
        sample_df = sample_df.sample(n=plan.shape[0], replace=True).reset_index(drop=True)
        plan = pd.concat([plan, sample_df], axis=1)
        # plan['interest_action_mode'] = 'RECOMMEND'

        # 选'deep_bid_type','roi_goal','smart_bid_type','adjust_cpa','cpa_bid'出价方式
        sample_df = self.df_create[['deep_bid_type', 'roi_goal', 'smart_bid_type', 'adjust_cpa', 'cpa_bid']]
        sample_df = sample_df[sample_df['deep_bid_type'].isin(['ROI_COEFFICIENT', 'BID_PER_ACTION'])]
        sample_df = sample_df.sample(n=plan.shape[0], replace=True).reset_index(drop=True)

        plan = pd.concat([plan, sample_df], axis=1)

        plan['roi_goal'] = plan['deep_bid_type'].apply(lambda x:0.02 if x=='ROI_COEFFICIENT' else np.nan)
        plan['create_time'] = pd.to_datetime(pd.datetime.now())
        plan['create_date'] = pd.to_datetime(pd.datetime.now().date())

        # 实时数据训练
        print("训练数据游戏包分布：", self.df_train['game_id'].value_counts())
        print("训练数据标签分布：", self.df_train['label'].value_counts())
        plan = train(self.image_info, self.df_train, plan)
        print("[INFO]: 实时计划训练完毕！")  ## 测试用日志

        return plan


if __name__ == "__main__":
    # todo:脚本测试通过！
    plan_creator = NewCreatePlan()
    plan = plan_creator()
