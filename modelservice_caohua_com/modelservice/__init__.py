from modelservice.__myconf__ import set_var
#
# 全局线上参数配置
#
set_var()
from modelservice.ptom_predict import PtomPredict
from modelservice.curve_fit import CurveFit
from modelservice.image_sorce import ImageScore
from modelservice.image_sorce_dg import ImageScoreDg
from modelservice.image_sorce_jp import ImageScoreJp
from modelservice.image_sorce_tk import ImageScoreTk
from modelservice.image_sorce_ddj import ImageScoreDdj
from modelservice.create_plan import CreatePlan
from modelservice.create_plan_2 import CreatePlan2
from modelservice.create_plan_dg import CreatePlanDg
from modelservice.create_plan_dg_gz import CreatePlanDggz
from modelservice.create_plan_gdt import CreatePlangdt
from modelservice.create_plan_gdt_dg import CreatePlangdtdg
from modelservice.create_plan_gdt_jp import CreatePlangdtjp
from modelservice.account_filter import AccountFilter
from modelservice.create_plan_gdt_mr_ios import CreatePlangdtMrIos
from modelservice.create_plan_tt_dg_ios import CreatePlanttdgIOS
from modelservice.create_plan_gdt_dg_ios import CreatePlangdtdgIOS
from modelservice.nmi import CalculateNmi
from modelservice.pay_7_predict import Pay7Predict
from modelservice.recommended_amount import RecommendedAmount
