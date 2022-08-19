# 天工模型-商业化版本
本次重构主要针对以下几点：
* 1、所有业务应用程序化封装为一个工程项目整体；
* 2、业务参数动态化处理，或者yaml配置处理；
* 3、业务规则尽量标准化统一；
* 4、执行日志全局配置，可定位模块、时间、脚本、行数；
* 5、代码加密；

## 目录结构

```python
|-- aimodel
|   |-- best_est_LGB_1056.pkl
|   |-- best_est_RF_1056.pkl
|   |-- best_est_XGB_1056.pkl
|   |-- cutxwoex.json
|   `-- plan_result.csv
|-- build
|   |-- config
|   |   |-- application.yml
|   |   |-- confing.yml
|   |   |-- logging.yml
|   |   `-- modelservice.log
|   |-- modelservice
|   |   |-- __init__.py
|   |   |-- file_1_image_score
|   |   |   |-- __init__.py
|   |   |   |-- get_image_prediction.cpython-37m-x86_64-linux-gnu.so
|   |   |   |-- get_image_trainer.cpython-37m-x86_64-linux-gnu.so
|   |   |   `-- utils
|   |   |       |-- __init__.py
|   |   |       `-- image_utils.cpython-37m-x86_64-linux-gnu.so
|   |   |-- file_2_batch_plan
|   |   |   |-- __init__.py
|   |   |   |-- get_plans.cpython-37m-x86_64-linux-gnu.so
|   |   |   `-- utils
|   |   |       |-- __init__.py
|   |   |       `-- plan_utils.cpython-37m-x86_64-linux-gnu.so
|   |   |-- file_3_monitor
|   |   |   |-- __init__.py
|   |   |   |-- get_plan_monitor.cpython-37m-x86_64-linux-gnu.so
|   |   |   `-- utils
|   |   |       |-- __init__.py
|   |   |       `-- monitor_utils.cpython-37m-x86_64-linux-gnu.so
|   |   `-- yaml_conf.cpython-37m-x86_64-linux-gnu.so
|   |-- run.bat
|   |-- start.sh
|   |-- stop.sh
|   `-- web_application.cpython-37m-x86_64-linux-gnu.so
|-- config
|   |-- application.yml
|   |-- confing.yml
|   |-- logging.yml
|   `-- modelservice.log
|-- modelservice
|   |-- __init__.py
|   |-- file_1_image_score
|   |   |-- __init__.py
|   |   |-- get_image_prediction.py
|   |   |-- get_image_trainer.py
|   |   `-- utils
|   |       |-- __init__.py
|   |       `-- image_utils.py
|   |-- file_2_batch_plan
|   |   |-- __init__.py
|   |   |-- get_plans.py
|   |   `-- utils
|   |       |-- __init__.py
|   |       `-- plan_utils.py
|   |-- file_3_monitor
|   |   |-- __init__.py
|   |   |-- get_plan_monitor.py
|   |   `-- utils
|   |       |-- __init__.py
|   |       `-- monitor_utils.py
|   `-- yaml_conf.py
|-- run.bat
|-- run.py
|-- setup.py
|-- start.sh
|-- stop.sh
`-- web_application.py
```

## 环境要求

* web
* cython  
* python-devel
* gcc
* sklearn
* xgboost
* lightgbm
* pymysql
* impala
* multiprocessing
## 功能模块

**其中**，加密后单独的代码目录为<font color='red'>build </font>，一个py文件对应一个.so文件，可执行setup.py批量打包。

| 1、素材评分             | 2、批量计划            | 3、模型监控         |
| ------------------ | ----------------- | -------------- |
| file_1_image_score | file_2_batch_plan | file_3_monitor |
## Getting Started

Python脚本启动服务：
```python
python run.py --vfe   # 启动加密版
# python run.py  # 启动源码版
```
```python
urls = (
    '/', Index,
    '/ImageScoreTrain', ImageTrainer,
    '/ImageScorePred', ImageScorer,
    '/PlanCreate', PlanCreate,
    '/PlanMonitor', PlanMonitor
)
```

### 1、素材训练

请求参数（样例）

```python
{"mgame_id":1056}
```
响应信息

```python
{"code": 200, "msg": "success!", "data": "image score train success!"}
```

### 2、素材评分

请求参数（样例）

```python
{"mgame_id":1056}
```

响应信息

```python
{"code": 200, "msg": "success!", "data": "image score pred success!"}
```

### 3、批量计划

请求参数（样例）

```python
{
 "game_id": 1001994,
 "media_id": 16,
 "platform": 1,
 "account_ids": [11537,11538],
 "nums": 10,
 "image_lib": [],
 "doc_lib":[]
 }
```

响应信息

```python
{"code": 200, "msg": "success!", "data": "plan create success!"}
```



### 4、模型1监控

请求参数（样例）

```python
{
  "mgameId": "1056",
  "mediaId": 45,
  "modelType": 1,
  "columns": 
 ["channel_id","source_id","plan_name","model_run_datetime","create_time","media_id","game_id","platform","source_run_date_amount","create_role_num","create_role_cost","create_role_pay_num","create_role_pay_cost","create_role_pay_sum","create_role_roi","create_role_pay_rate","create_role_pay_num_cum","learning_type","learning_time_dt","learning_time_hr","deep_bid_type"
  ],
  "data": 
  [[21378,1181763,"1124七雄争霸_ROI__n24系xt8pp__091_4","2022-05-20 00:00:00","2022-05-20 00:00:00",45,1001858,1,3000,0,100,0,0.0,0.0,0.009,0,0,null,null,null,"ROI_COEFFICIENT"],
   [21378,1181764,"1124七雄争霸_ROI__n24系xt81.5千__091_9","2021-11-24 00:00:00","2021-11-24 00:00:00",45,1001858,1,20000,0,9.59,0,0.0,0.0,0.0,0,0,null,null,null,"ROI_COEFFICIENT"]]
}
```

响应信息

```python
{"code": 200, "msg": "success!", "data": "plan monitor success!"}
```



### 5、模型2监控

请求参数（样例）

```python
{ "mgameId": "1051",
  "mediaId": 45,
  "modelType": 2,
  "columns":["channel_id","source_id","plan_name","model_run_datetime","create_time","media_id","game_id","platform","data_win","source_run_date_amount","create_role_num","create_role_cost","create_role_pay_num","create_role_pay_cost","create_role_pay_sum","create_role_roi","create_role_retain_1d","create_role_pay_rate","create_role_pay_num_cum","learning_type","learning_time_dt","learning_time_hr","deep_bid_type","cum_amount","cum_day","cum_role_cost"],
  "data":
  [[21207,1482312,"BD_45_女神_0610_14_510_T1anJ1_1","2022-06-20 00:00:00","2022-06-10 00:00:00",45,1001482,2,1,46.33,2,23.1650000,0,0.000000,0.00,0E-7,0.0000,0.00000,0,null,null,null,"DEEP_BID_DEFAULT",997.30,10,66.4866667],[21207,1482313,"BD_45_女神_0610_14_500_T1anJ1_12","2022-06-20 00:00:00","2022-06-10 00:00:00",45,1001482,2,1,19.97,1,19.9700000,0,0.000000,0.00,0E-7,0.0000,0.00000,0,null,null,null,"DEEP_BID_DEFAULT",1170.82,10,234.1640000],[21207,1482324,"BD_45_女神_0610_14_501_T1anJ1_8","2022-06-20 00:00:00","2022-06-10 00:00:00",45,1001482,2,1,0.09,0,0.0900000,0,0.000000,0.00,0E-7,0.0000,0,0,null,null,null,"DEEP_BID_DEFAULT",698.99,10,232.9966667],[21207,1482325,"BD_45_女神_0610_14_510_T1anJ1_13","2022-06-20 00:00:00","2022-06-10 00:00:00",45,1001482,2,1,156.61,2,78.3050000,0,0.000000,0.00,0E-7,0.0000,0.00000,0,null,null,null,"DEEP_BID_DEFAULT",2396.04,10,70.4717647]]}
```

响应信息

```python
{"code": 200, "msg": "success!", "data": "plan monitor success!"}
```



## 注意

- 1、.so文件的python版本编译环境和运行环境必须一致，客户本地部署可考虑连环境一起打包镜像；
- 2、.so文件执行的error报错信息，就是python的日志，方便定位问题；
- 3、待定...