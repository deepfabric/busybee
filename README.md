# Busybee
Busybee是一个用户自动化营销系统的流程引擎，支持多租户。支持简易的流程定义。

## 流程定义
Busybee采用Protobuf格式来定义流程，流程相关结构如下：
```
// 流程定义
message Workflow {
    uint64         id        = 1;   // 流程ID, required
    uint64         tenantID  = 2;   // 租户ID, required
    string         name      = 3;   // 流程名称, required
    repeated Step  steps     = 4;   // 流程节点集合, 第一个元素为start节点, required
    int64          stopAt    = 5;   // 流程有效期，精确掉秒的timestamp, 默认0，表示永不过期, optional
}

// 流程节点
message Step {
    string    name        = 1;  // 节点名称，流程定义中唯一，流程使用name字段来在流程中跳转, required
    Execution execution   = 2;  // 节点计算信息，有Direct, Timer, Branch, Parallel四种类型, required
    string    enterAction = 3;  // 用户进入该节点的动作，格式有业务自己定义，在流程变更通知中，会原样通知给业务系统, optional
    string    leaveAction = 4;  // 用户离开该节点的动作，格式有业务自己定义，在流程变更通知中，会原样通知给业务系统, optional
    int32     ttl         = 5;  // 有效期，单位秒，从用户进入该节点开始计算，可以用来完成实际业务中，需要用户在规定时间内完成某些操作（例如消费优惠券），在用户进入该节点的时候，引擎会原样传递TTL值给业务系统, optional
}

// 节点计算信息
message Execution {
    ExectuionType               type      = 1; // 计算类型枚举值, required
    TimerExecution              timer     = 2; // 定时器类型, optional
    DirectExecution             direct    = 3; // 直接执行，无条件执行, optional
    repeated ConditionExecution branches  = 4; // 分支执行类型，按照条件选择某一个分支执行, optional
    ParallelExecution           parallel  = 5; // 并行执行各个分支, optional
}

// 定时器
message TimerExecution {
    Expr   condition           = 1; // 表达式条件，满足条件才执行, optional
    string cron                = 2; // 定时器的cron表达式, required
    string nextStep            = 3; // 跳转到的下一个节点, required
    bool   useStepCrowdToDrive = 4; // 是否使用当前节点上的所有用户作为输入执行, 默认值false, optional
}

// 无条件执行器
message DirectExecution {
    string nextStep = 1; // 跳转到的下一个节点, required
}

// 条件执行器
message ConditionExecution {
    Expr      condition = 1; // 表达式条件，满足条件才执行, required
    Execution execution = 2; // 计算信息，optional
    string    nextStep  = 3; // 跳转到的下一个节点, optional
}

// 并行执行器
message ParallelExecution {
    string             nextStep  = 1; // 跳转到的下一个节点, required
    repeated Execution parallels = 2; // 并行分支定义，required
}

// 表达式
message Expr {
    bytes          value = 1; // 表达式内容, required
    ExprResultType type  = 2; // 表达式返回类型，默认bool, optional
}

// 表达式返回类型
enum ExprResultType {
    BoolResult = 0; // bool类型
    BMResult   = 1; // 人群bitmap集合
}

// 计算类型枚举
enum ExectuionType {
    Direct    = 0;
    Timer     = 1;
    Branch    = 2;
    Parallel  = 3;
}
```

## 流程对外提供的接口
提供的所有接口，在[Busybee-sdk](https://github.com/deepfabric/busybee-sdk)中提供。

### TenantInit
初始化一个租户, 初始化租户创建一下几个资源:

* 创建input事件队列，存放业务系统的用户终端埋点采集数据用来驱动流程
* 创建output队列，存放用户在流程节点中变更的通知信息，业务系统用来消费继续驱动流程
* 创建流程执行资源并行度，并行度越高占用资源越多，流程运行越快

### StartInstance
启动一个流程定义的实例

### LastInstance
获取一个流程的最新的一个instance

### HistoryInstance
返回一个流程历史已经完成的实例的snapshot信息

### UpdateCrowd
更新正在运行流程的人群集合

### UpdateWorkflow
更新正在运行流程的定义

### StopInstance
停止一个流程实例

### InstanceCountState
查询流程当前所有节点上有多少人停留在上面

### InstanceStepState
查询流程的某个节点的人群详情

## 关于表达式定义
Busybee有自己的表达式引擎，支持很多表达式计算来支撑流程。

* 支持变量的+,-,*,/ 计算: {num:var1} + 10,  {num:var1} - 10, {num:var1} * 10, {num:var1} / 10
* 支持判断 ==, !=, >, >=, <, <=, ~以及!~（正则匹配或者不匹配）
* 支持bitmap操作: {bm:var1} && {bm:var2}, {bm:var1} || {bm:var2},  {bm:var1} !&& {bm:var2},  {bm:var1} ^|| {bm:var2}
* 变量获取：
   * 支持从kv 存储里面获取: {kv.var_name}
   * 支持从profile获取用户信息: {profile.var_name}
   * 支持动态变量(动态变量从kv中获取): 
       * {dyna.prefix_%d.year|month|day}(获取当前年月日和prefix_%d拼接成kv里面的key，再返回kv里的value)
       * {dyna.prefix_%s.event.attr}(获取event的attr属性和prefix_%d拼接成kv里面的key，再返回kv里的value)
       * {dyna.prefix_%s.kv.key}(获取kv里面的的key属性和prefix_%d拼接成kv里面的key，再返回kv里的value)
       * {dyna.prefix_%s.profile.attr}(获取用户profile里面的的attr属性和prefix_%d拼接成kv里面的key，再返回kv里的value)
* 支持一些系统函数
   * year          获取当前年份
   * month         获取当前月份
   * day           获取当前日期
   * week          获取当前星期
   * time          获取当前时间，格式"hhmmss", 时分秒
   * date          获取当前时间，格式"yyMMdd", 年月日
   * datetime      获取当前时间，格式"yyMMdd hhmmss", 年月日时分秒,24小时
   * timestamp     获取当前时间，获取当前时间戳, 精度到秒
   * wf_crowd      获取当前流程的人群bitmap
   * wf_step_crowd 获取当前节点的人群bitmap
* 支持用() 包装任意复杂度的表达式
* 表达式最终需要返回一个bool值或者bitmap，返回bool代表当前的用户满足条件，分会bitmap代表bitmap内部所有的用户满足条件条件