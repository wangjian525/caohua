# -*- coding:utf-8 -*-
"""
   File Name：     create_plan_with_train_dg.py
   Description :   新建计划：计划属性自动化组合（帝国）
   Author :        royce.mao
   date：          2021/6/30 15:45
"""


import pandas as pd
import numpy as np
import json
import os
import time
# os.chdir(os.path.dirname(__file__))
import pymysql
from impala.dbapi import connect
from impala.util import as_pandas
import random
import logging
from prefixspan import PrefixSpan

import warnings
warnings.filterwarnings('ignore')
logger = logging.getLogger('CreatePlanDg')

from media_toutiao.script.trainer import train
from media_toutiao.script.printout import get_plan_online  ## 格式化输出
from media_toutiao.script.realtime_df_dg import DataRealTime  ## 采集实时数据类

#
# 打包接口
#
class TTCreatePlanDg:
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
        ret = json.dumps({"code": 200, "msg": "success!", "data": "dg create plan is success"})
        return ret


class NewCreatePlan(object):
    """ 新计划创建类 """
    def __init__(self, ):
        self.image_score_pre = NewImageScore()
        self.fea_assoc = FeaAssoc()

    @staticmethod
    def find_new_images(dg_game, is_back=True):
        """[新素材筛选 + 实时入库]

        Returns:
        """
        # 88数据库 游标1
        conn1 = connect(host='192.168.0.97', port=10000, auth_mechanism='PLAIN', user='hadoop',
                    password='Ycjh8FxiaoMtShZRd3-97%3hCEL0CK4ns1w', database='default')
        cur1 = conn1.cursor()
        sql_engine = 'set hive.execution.engine=tez'
        sql = 'select image_id,label_ids,score from dws.dws_image_score_d where media_id=10 and dt>=\'2021-01-01\''
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
        cur2.execute(sql)  ## or a.image_id in (34738,34906,35247)
        image_df = pd.read_sql(sql, conn2)
        image_df['label_ids'] = image_df['label_ids'].str.replace( ',', ';')  ## 避免分隔符冲突
        image_df['game_ids'] = image_df['game_ids'].str.replace( ',', ';')  ## 避免分隔符冲突

        # 79数据库 游标3
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
                AND b.media_id = 10
                AND b.game_id = {}
        '''
        finalSql = sql.format(dg_game)
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
        image_df = image_df.loc[image_df.apply(lambda x:True if not pd.isnull(x['game_ids']) and str(dg_game) in set(x['game_ids'].split(';')) else False, axis=1)]
        # 新素材的image_game有TT标识
        image_df = image_df.loc[image_df.apply(lambda x:True if 'TT' in x['image_name'] else False, axis=1)]
        
        if not is_back:
            # 过滤新素材方案一：直接匹配没有评分的素材 +  有评分但总消耗在1000以上5000以内的素材
            image_df_new = image_df.loc[image_df.apply(lambda x:True if not any(score_df['image_id'] == x.image_id) else False, axis=1)]
            print("no score new num:", len(image_df_new))  ## 未评分新素材数量
            print("no score new id:", image_df_new['image_id'].values.tolist())
            plan_df = plan_df.groupby('image_id')['amount'].sum()
            plan_df = pd.DataFrame({'image_id':plan_df.index,'amount':plan_df.values})
            plan_df = plan_df.loc[(plan_df['amount'] >= 1000) & (plan_df['amount'] <= 5000)]  # todo:上下限调整
            image_df_old = image_df.loc[image_df['image_id'].isin(np.intersect1d(plan_df['image_id'].values, image_df['image_id'].values))]
            print("back new num:", len(image_df_old))  ## 返新素材数量
            image_df = pd.concat([image_df_new, image_df_old], ignore_index=True, join='inner')
            # image_df = image_df_new
        else:  
            # 过滤新素材方案二：匹配没有评分且没有建过计划的素材 + 有评分但总消耗在1000以上5000以内的素材
            image_df_new = image_df.loc[image_df.apply(lambda x:True if not any(plan_df['image_id'] == x.image_id) else False, axis=1)]
            print("pure new num:", len(image_df_new))  ## 纯新素材数量
            plan_df = plan_df.groupby('image_id')['amount'].sum()
            plan_df = pd.DataFrame({'image_id':plan_df.index,'amount':plan_df.values})
            plan_df = plan_df.loc[(plan_df['amount'] >= 1000) & (plan_df['amount'] <= 5000)]  # todo:上下限调整
            image_df_old = image_df.loc[image_df['image_id'].isin(np.intersect1d(plan_df['image_id'].values, image_df['image_id'].values))]
            print("back new num:", len(image_df_old))  ## 返新素材数量
            image_df = pd.concat([image_df_new, image_df_old], ignore_index=True, join='inner')

        image_df['score'] = np.nan
        image_df['label'] = 0
        # image_df['tdate'] = time.strftime('%Y-%m-%d', time.localtime(int(time.time())))
 
        # 实时入库 第1次
        csv_path = 'new_image_{}.csv'.format(time.strftime('%Y%m%d%H%M%S', time.localtime(int(time.time()))))
        image_df.to_csv(csv_path, index=0, encoding='utf_8_sig',header=None)  ## label暂未实时修正的结果，且score也是初始化的NaN
        while not os.path.exists(csv_path):
            time.sleep(1)
        
        # os.system("hadoop fs -rm -r hdfs://masters/user/hive/warehouse/test.db/new_image_test_dg")
        # os.system("hadoop fs -mkdir hdfs://masters/user/hive/warehouse/test.db/new_image_test_dg")
        # os.system("hadoop fs -put {} hdfs://masters/user/hive/warehouse/test.db/new_image_test_dg".format(csv_path))
        # os.system("hive -e \"msck repair table test.new_image_test_dg\"")
        os.remove(csv_path)
        
    @staticmethod
    def plan_attr_assoc(score_image_group, ad_account_id_group, columns, capacity):  # 具体建模过程见Plan.ipynb
        """ [计划资源分配]

        Args:
            score_image_group ([dataframe]): [新素材]
            ad_account_id_group ([list]): [账号集]
            columns ([list]): [新建计划涉及的字段组合]
            capacity ([int]): [账号的容积计划数量限制]
        """
        # score_image_group = score_image_group.loc[score_image_group.apply(lambda x:True if int(x['image_id']) >= 35400 else False, axis=1)]  ## todo:临时
        
        dist_image_ids = score_image_group['image_id'].value_counts().index
        plan = pd.DataFrame(columns=columns)

        for image_id in sorted(dist_image_ids, key=lambda x: random.random()):  ## 按异质id标签遍历
            # 素材下新建计划
            sample = random.choice(score_image_group[score_image_group['image_id'] == image_id][['label_ids', 'score']].values)
            label = sample[0]
            score = sample[1]
            plan_part = pd.DataFrame({'image_id': [image_id], 'label_ids': [label], 'image_score': [score]})
            plan = plan.append(plan_part)
        # 复制3条
        plan = pd.DataFrame(np.repeat(plan.values, 3, axis=0), columns=columns)
        # plan['ad_account_id'] = np.nan

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
        plan_result['android_osv'] = np.nan
        plan_result['op_id'] = 13678
        plan_result['flag'] = 'CESHI'
        plan_result['operation'] = 'disable'
        # plan_result['flow_control_mode'] = plan_result.apply(lambda x:random.choice(['FLOW_CONTROL_MODE_FAST', 'FLOW_CONTROL_MODE_BALANCE']), axis=1)
        plan_result['launch_price'] = 1
        plan_result['launch_price'] = plan_result['launch_price'].map({1: [1000, 11000]})
        plan_result['ac'] = 1
        plan_result['ac'] = plan_result['ac'].map({1: ['WIFI', '4G']})
        
        plan_result['image_id'] = plan_result['image_id'].astype(int)
        plan_result['platform'] = plan_result['platform'].map({1: ['ANDROID'], 2: ['IOS']})
        plan_result['convertIndex'] = plan_result['deep_bid_type'].apply(lambda x: 13 if x == 'BID_PER_ACTION' else 14)
        
        # plan_result['inventory_type'] = 'INVENTORY_FEED,INVENTORY_AWEME_FEED,INVENTORY_TOMATO_NOVEL,INVENTORY_UNION_SLOT,INVENTORY_VIDEO_FEED'
        # plan_result['inventory_type'] = plan_result['inventory_type'].apply(lambda x: x.split(','))
        plan_result['age'] = 'AGE_BETWEEN_18_23,AGE_BETWEEN_24_30,AGE_BETWEEN_31_40,AGE_BETWEEN_41_49'
        plan_result['age'] = plan_result['age'].apply(lambda x: x.split(','))
        
        plan_result['budget'] = plan_result.apply(lambda x: x.budget if x.budget >= np.nan_to_num(x.cpa_bid) else x.cpa_bid, axis=1)
        plan_result['budget'] = plan_result['budget'].apply(np.ceil)
        plan_result['cpa_bid'] = plan_result['cpa_bid'].apply(lambda x: x if x >= 620 and x <=670 else random.randint(620, 670))
        plan_result['schedule_time'] = plan_result['schedule_time'].apply(
        lambda x: x[0:96] + '1111111111000000000000' + x[118:144] + '1111111111000000000000' + x[166:])
        # plan_result['schedule_time'] = ('1'*10 + '0'*14 + '1'*6 + '0'*6 + '1' * 12) * 5 + ('1'*16 + '0'*32) + ('0'*19 + '1'*29)
        plan_result['city'] = plan_result['city'].apply(
        lambda x: x if x != [] else [11, 12, 13, 14, 15, 21, 22, 23, 31, 32, 33, 34, 35, 36, 37, 41, 420200, 420300,
                                     420500, 420600, 420700, 420800, 420900, 421000, 421100, 421200, 421300, 422800,
                                     429004, 429005, 429006, 429021, 43, 44, 45, 46, 50, 51, 52, 53, 54, 61, 62, 63, 64,
                                     65, 71, 81, 82])
        plan_result['district'] = 'CITY'
        plan_result['web_url'] = 'https://www.chengzijianzhan.com/tetris/page/6977187066200522789/'
        plan_result = plan_result.apply(lambda x: x.fillna({i: [] for i in plan_result.index}))  ## 空list替换空值
        
        plan_result['android_osv'] = plan_result['android_osv'].apply(lambda x:np.nan if isinstance(x, list) else x)
        plan_result['action_days'] = plan_result['action_days'].apply(lambda x:np.nan if isinstance(x, list) else x)
        plan_result['retargeting_type'] = plan_result['retargeting_type'].apply(lambda x:np.nan if isinstance(x, list) else x)
        plan_result['adjust_cpa'] = plan_result['adjust_cpa'].apply(lambda x:np.nan if isinstance(x, list) else x)
        plan_result['roi_goal'] = plan_result['roi_goal'].apply(lambda x:np.nan if isinstance(x, list) else x)
        
        plan_result.to_csv('./plan_result.csv', index=0)  ## 保存创建日志

        return plan_result

    
    def __call__(self, ):
        """ 创建类函数 """
        # 新素材
        self.find_new_images(cfg.DG_GAME, False)  ## 该方法实时更新测新库test.new_image_test_dg
        
        score_images = self.image_score_pre('帝国')  ## 该方法实时提取测新库中的新素材
        print("[INFO]：新素材提取成功！")  ## 测试用日志
        # todo: keep='first'代表保留入库记录不再重复测新（未评分的素材不再参与），keep='last'代表重建入库记录重新测新（未评分的素材再次参与）
        score_images.drop_duplicates(subset=['image_id'], keep='last', inplace=True)  ## 建计划之前去重：避免只有1次入库异常素材的重复使用
        
        score_images['label_ids'] = score_images['label_ids'].astype(str)
        score_images_associate = score_images[score_images['label'] == 1]

        # 计划：创建 + 建模
        plan_associate = self.plan_attr_assoc(score_images_associate, cfg.AD_ACCOUNT_ID_GROUP_DG, cfg.ENCODER_COLS + cfg.HOTTING_COLS + cfg.CONT_COLS, cfg.AD_ACCOUNT_CAPACITY)
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
        # os.system("hadoop fs -rm -r hdfs://masters/user/hive/warehouse/test.db/new_image_test_dg")
        # os.system("hadoop fs -mkdir hdfs://masters/user/hive/warehouse/test.db/new_image_test_dg")
        # os.system("hadoop fs -put {} hdfs://masters/user/hive/warehouse/test.db/new_image_test_dg".format(csv_path))
        # os.system("hive -e \"msck repair table test.new_image_test_dg\"")
        os.remove(csv_path)
 
        # 格式化输出
        # plan_result = plan_result.head(1)
        # rsp = get_plan_online(plan_result)  ## todo
        print("[INFO]: 新建计划创建完毕！")  ## 测试用日志
        return plan_result
        

class FeaAssoc(object):
    """ 计划特征组合类 """
    def __init__(self, ):
        self.encode_cols = list(set(cfg.ENCODER_COLS) - set(['ad_account_id', 'image_id', 'game_id']))  ## 数字类别标签
        self.hottin_cols = list(set(cfg.HOTTING_COLS) - set(['label_ids']))  ## Embedding标签
        self.hottin_num = cfg.HOTTING_NUM  ## 每个HOTTING_COLS标签下，最多需要勾选的分组数量，注不能超过分组的总数量
        
        self.orient_cols_group = cfg.ORIENT_COLS_GROUP  ## 未进入模型的部分定向标签
        self.orient_cols_action = cfg.ORIENT_COLS_ACTION  ## 未进入模型的部分定向标签
        self.orient_cols_bid = cfg.ORIENT_COLS_BID  ## 未进入模型的部分定向标签
        
        self.new_cols = cfg.NEW_COLS  ## 未进入模型的部分新标签
        self.contin_cols = ['budget', 'cpa_bid']  ## 连续值标签

        self.df_, self.df_create, self.df_image, self.now_plan_create = DataRealTime()(cfg)
        print("[INFO]: 实时数据提取完毕！")  ## 测试用日志
        

    def __call__(self, plan):
        """ 组合类函数 """
        # 默认值
        plan['game_id'] = cfg.DG_GAME
        plan['platform'] = 1
        self.df = self.df_[self.df_['label'] == 1]  ## 所有好计划做为有效采样集

        # 未确定的数字类别标签组合 - 概率采样 self.df_create
        for col in self.encode_cols:
            count_df = pd.DataFrame(data=self.df_create[col].value_counts()).reset_index()
            count_df.columns = ['col', 'counts']
            if col == 'inventory_type':
                count_df['col'] = count_df['col'].apply(lambda x: list(filter(None, x)))
            count_df['pct'] = count_df['counts'] / count_df['counts'].sum()
            plan[col] = plan.apply(lambda x: np.random.choice(count_df['col'].values, 1, p=count_df['pct'].values)[0], axis=1)
        
        # 未确定的Embedding标签组合 - 频繁项集 self.df
        for col, num in zip(self.hottin_cols, self.hottin_num):
            valid_df = self.df[~self.df[col].isnull()]
            prefix = valid_df[col].to_list()  ## [eval(row) for row in valid_df[col].to_list()]
            ps = PrefixSpan(prefix)

            freq_item_pd = pd.DataFrame()
            for i in range(num):  ## 最小长度为1，最大长度为num的频繁项集
                ps.minlen, ps.maxlen = i+1, i+1
                freq_item = pd.DataFrame(ps.topk(3), columns=[['counts','col']])
                if not len(freq_item):
                    continue
                freq_item['pct'] = (freq_item[['counts']] / freq_item[['counts']].sum()) * (i+1) / num  ## 越长的项集权重越大
                freq_item_pd = freq_item_pd.append(freq_item)
            freq_item_pd['pct'] = freq_item_pd[['pct']] / freq_item_pd[  ['pct']].sum()
            plan[col] = plan.apply(lambda x: np.random.choice(freq_item_pd[['col']].values.flatten(), 1, p=freq_item_pd[['pct']].values.flatten())[0], axis=1)
        
        # 未进入模型的部分定向标签 - 随机采roi或pay_rate排名考前20%部分的定向 self.df_create
        orient_df = self.df_create[self.orient_cols_group].sample(n=plan.shape[0], replace=True).reset_index(drop=True)
        plan = pd.concat([plan, orient_df], axis=1)
        orient_df = self.df_create[self.orient_cols_action].sample(n=plan.shape[0], replace=True).reset_index(drop=True)
        plan = pd.concat([plan, orient_df], axis=1)
        orient_df = self.df_create[self.orient_cols_bid].sample(n=plan.shape[0], replace=True).reset_index(drop=True)
        plan = pd.concat([plan, orient_df], axis=1)

        # 未进入模型的新定向标签 - 随机采优质策略 self.df_create
        new_df = self.df_create[self.new_cols].sample(n=plan.shape[0], replace=True).reset_index(drop=True)
        plan = pd.concat([plan, new_df], axis=1)

        # 连续值标签 - "budget"按原来设置，"cpa_bid"、"roi_goal"随机采优质策略 self.df_create
        plan['budget'] = plan['platform'].apply(lambda x: 3300 if x == 1 else 4000)
        plan['cpa_bid'] = self.df_create['cpa_bid'].sample(n=plan.shape[0], replace=True).reset_index(drop=True)
        plan['create_time'] = pd.to_datetime(pd.datetime.now())
        plan['create_date'] = pd.to_datetime(pd.datetime.now().date())

        # 实时数据训练
        plan_prob = train(self.df_, self.df_image, plan, cfg)
        print("[INFO]: 实时计划训练完毕！")  ## 测试用日志

        return plan, plan_prob


if __name__ == "__main__":
    # todo:脚本测试通过！
    plan_creator = NewCreatePlan()
    plan = plan_creator()
