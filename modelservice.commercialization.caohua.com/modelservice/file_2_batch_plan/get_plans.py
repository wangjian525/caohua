# -*- coding:utf-8 -*-

import web
import math
import json
import logging
import datetime
import traceback
import warnings
warnings.filterwarnings('ignore', category=FutureWarning)

from modelservice.file_2_batch_plan.utils.plan_utils import *
# import sys
# sys.path.append("..")
import modelservice.yaml_conf as yaml_conf
logger = logging.getLogger('PlanCreate')


confs = yaml_conf.get_config()  # !!! 一对一计划参数YAML全局配置

class PlanCreate:
    """ 批量计划 """
    def POST(self, ):
        ret = self.main()
        return ret

    def __init__(self, ):
        pass

    def get_image_recall(self, ):
        """ 素材 """
        if len(self.image_lib) > 0:
            return self.image_lib
        else:
            image_lib = get_images(self.game_id, self.media_id, self.nums)
            return image_lib

    def get_field_recall(self, ):
        """ 要素 """
        try:
            field_sprea = get_fields(self.game_id, self.media_id)
        except Exception as e:
            field_sprea = None
        return field_sprea

    def get_plans(self, image_lib, field_sprea):
        """ 计划 """
        # 分媒体创建计划
        plan_result = get_plans_media(confs,
                                      media_id=self.media_id,  # int COMMENT '媒体'  -- （决定配置）
                                      platform=self.platform,  # int COMMENT '平台'  -- （决定配置）
                                      game_id=self.game_id,  # int COMMENT '游戏'    -- （决定配置）
                                      account_ids=self.account_ids,  # list COMMENT '账号集'
                                      nums=self.nums,  # int COMMENT '计划量（总量）' 
                                      image_lib=image_lib,  # !!! list COMMENT '素材库' （中间变量）
                                      field_sprea=field_sprea)  # !!! dict COMMENT '要素分布' （中间变量）
        return plan_result

    def main(self, ):
        try:
            param_json = web.data()
            param_json = json.loads(param_json)
            self.media_id = param_json['media_id']       # int COMMENT '媒体' 
            self.platform = param_json['platform']       # int COMMENT '平台' 
            self.game_id = param_json['game_id']         # int COMMENT '游戏' 
            self.account_ids = param_json['account_ids'] # list COMMENT '账号集' 
            self.nums = param_json['nums']               # int COMMENT '计划量（总量）' 
            self.image_lib = param_json['image_lib']     # list COMMENT '素材库' 
            self.doc_lib = param_json['doc_lib']         # !!! list COMMENT '文案库'（使用待定）
            # 素材召回
            print("[INFO-IMAGING]:素材召回中...")
            self.image_lib = self.get_image_recall()
            print("[INFO-IMAGING]:素材召回完毕！", self.image_lib)
            # 要素召回
            print("[INFO-FIELDING]:要素召回中...")
            self.field_sprea = self.get_field_recall()
            print("[INFO-FIELDING]:要素召回完毕！")
            # 计划创建
            print("[INFO-PLANING]:计划创建中...")
            plan_result = self.get_plans(self.image_lib, self.field_sprea)
            plan_result.to_csv('./aimodel/plan_result.csv', index=0, encoding="utf_8_sig")  # 保存创建日志
            print("[INFO-PLANING]:计划创建完毕！")
            # 请求
            print("[INFO-POSTING]:请求接口中...")
            rsp_data = get_ad_create(plan_result, self.media_id)
            print("[INFO-POSTING]:请求接口完毕！")
        except Exception as e:
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logging.error("plan create error! at {}".format(timestamp))
            logging.error(e)
            logging.error("\n" + traceback.format_exc())
            return json.dumps({"code": 500, "msg": str(e), "datetime": timestamp})
        
        ret = json.dumps({"code": 200, "msg": "success!", "data": "plan create success!"})
        return ret


def get_images(game_id, media_id, num):  # !!! 方法不区分媒体
    """ 素材召回 """
    def image_flow(game_id, media_id, score, nodt=False):
        if not nodt:
            image_score = get_score_image(media_id, score)
        else:
            image_score = get_score_image_nodt(media_id, score)
        image_score = filter_game_image(image_score, game_id)
        image_score = filter_exclude_image(image_score, media_id)
        image_lib = get_sort_image(image_score, media_id)
        return image_lib

    try:
        image_lib = image_flow(game_id, media_id, 500)
    except Exception as e:
        image_lib = image_flow(game_id, media_id, 500, True)

    image_lib_set = list(set(image_lib))
    image_lib_set.sort(key=image_lib.index)
    image_lib_set = image_lib_set[:math.ceil(num / 2)] if len(image_lib_set) > math.ceil(num / 2) else image_lib_set
    # image_lib_set = list(set(image_lib_set) - set([48201,]))
    # image_lib_set = image_lib_set + [48201]
    return image_lib_set


def get_fields(game_id, media_id):  # !!! 方法区分媒体
    """ 要素召回 """ 
    game_ids = get_game_id(game_id)
    fields = get_field_media(game_ids, media_id)
    return fields


if __name__ == '__main__':
    # 测试
    creater_plan = PlanCreate(16, 1, 1001834, [9153], 10, [], [])  # !!! 输入形式3
    creater_plan.main()
