# 收益管理模型

## 目录结构
```text
.
├── assist # 辅助数据包
│   └── AssistData.java
├── config # 配置包
│   ├── AsyncConfiguration 线程池配置包
│   ├── ConfigDao #配置加载包
│   └──  DataSourceConfig #数据源配置包
├── control # 控制器
├── dao # MVC设计模式中Dao数据库操作类
├── service # MVC设计模式中业务实现类
├── jobHandler # 调度任务执行器包
│   ├──CabinAdjustJob #舱位适配
│   ├──FeatureRunnerJob #特征计算
│   ├──ReadAvJob #AV数据获取解析
│   ├──ReadCtripPriceJob #OTA数据获取解析
│   └── ...... #其他原数据解析
├── merge_model # 特征数据合并
├── utils # 工具类
├── CharacteristicsApplication.java #项目启动类
├── resources# 静态资源包
│   ├── price 价格特征相关sql
│   ├── traffic 其他相关sql

