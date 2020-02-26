# busybee管理后台接口
本文描述了busybee的后台管理接口文档

## 状态码说明
| Code | 说明  |
| ------------- | ------------- | 
| 0 | 成功 |
| 1 | 失败 |

## 返回Result说明
```json
{
    "code": 0,
    "data": {},
    "error": "",
}
```
|  字段 | 说明  |
| ------------- | ------------- | 
| code | 返回码 |
| data |  返回json数据，可以是对象也可以是数组，看接口指定 |
| error | 错误信息，状态码不为`0`的时候，这个字段不为空，否则为空 |

## 枚举
### 流程状态枚举
|  枚举值 | 说明  |
| ------------- | ------------- | 
| 1 | starting，正在启动 |
| 2 | started，已经启动，正在运行 |
| 3 | stopping， 正在停止 |
| 4 | stopped，已经停止 |

### worker状态枚举
|  枚举值 | 说明  |
| ------------- | ------------- | 
| 1 | running，运行中|
| 2 | stoped，已经停止|

## 流程管理
### 流程列表
#### URL PATH
`/v1/workflows/{tenantId}?after=0&size=10`

#### HTTP Method
`GET`

#### URL Params
| Query String  Name | 说明  |Optional | Default Value |
| ------------- | ------------- | ------------- | ------------- | 
| tenantId  | 租户ID  | No |  |
| after  | 起始workflowId，后一页的ID是前一页最后一个记录的ID+1  | Yes | 0 |
| size  | 获取多少条记录  | Yes | 10 |

#### Result JSON
```json
{
    "code": 0,
    "data": [
        {
            "id": 1,
            "define": "",
            "crowd": 2000000,
            "workers": 100,
            "status": 1,
            "startedAt": 100000,
            "stoppedAt": 100000, 
            "version": 100,
        },
        {
            "id": 2,
            "define": "",
            "crowd": 2000000,
            "workers": 100,
            "status": 2,
            "startedAt": 100000,
            "stoppedAt": 100000, 
            "version": 100,
        }
    ]
}
```

|  字段 | 说明  |
| ------------- | ------------- | 
| id | 流程ID |
| define | 流程定义，JSON字符串 |
| crowd |  流程包含的总人数 |
| workers | 流程的worker个数 |
| status | 枚举，参见流程状态枚举 |
| startedAt | 流程启动完毕的时间戳，精确到秒 |
| stoppedAt | 流程停止完毕的时间戳，精确到秒 |
| version | 流程定义版本，每次更新，版本都会增加 |

### 流程详情
#### URL PATH
`/v1/workflows/{tenantId}/{workflowId}`

#### HTTP Method
`GET`

#### URL Params
| Query String  Name | 说明  |Optional | Default Value |
| ------------- | ------------- | ------------- | ------------- | 
| tenantId  | 租户ID  | No |  |
| workflowId | workflowId  | No |  |

#### Result JSON
```json
{
    "code": 0,
    "data": {
        "id": 1,
        "define": "",
        "crowd": 2000000,
        "workers": 100,
        "status": 1,
        "startedAt": 100000,
        "stoppedAt": 100000, 
        "version": 100,
    }
}
```

|  字段 | 说明  |
| ------------- | ------------- | 
| id | 流程ID |
| define | 流程定义，JSON字符串 |
| crowd |  流程包含的总人数 |
| workers | 流程的worker个数 |
| status | 枚举，参见流程状态枚举 |
| startedAt | 流程启动完毕的时间戳，精确到秒 |
| stoppedAt | 流程停止完毕的时间戳，精确到秒 |
| version | 流程定义版本，每次更新，版本都会增加 |

### 流程Worker详情
#### URL PATH
`/v1/workflows/{tenantId}/{workflowId}/workers`

#### HTTP Method
`GET`

#### URL Params
| Query String  Name | 说明  |Optional | Default Value |
| ------------- | ------------- | ------------- | ------------- | 
| tenantId  | 租户ID  | No |  |
| workflowId | workflowId  | No |  |

#### Result JSON
```json
{
    "code": 0,
    "data": [
        {
            "workerIdx": 0,
            "status": 1,
            "committedOffset": 10000,
            "version": 100,
        },
        {
            "workerIdx": 1,
            "status": 2,
            "committedOffset": 10000,
            "version": 100,
        }
    ]
}
```

|  字段 | 说明  |
| ------------- | ------------- | 
| workerIdx | worker index，[0, workflow workers 字段指定的个数) |
| status | 枚举，参见worker状态枚举 |
| committedOffset | 在Event input queue中已经消费的offset |
| version | worker版本，每次更新，版本都会增加 |

### 流程节点详情
#### URL PATH
`/v1/workflows/{tenantId}/{workflowId}/steps`

#### HTTP Method
`GET`

#### URL Params
| Query String  Name | 说明  |Optional | Default Value |
| ------------- | ------------- | ------------- | ------------- | 
| tenantId  | 租户ID  | No |  |
| workflowId | workflowId  | No |  |

#### Result JSON
```json
{
    "code": 0,
    "data": [
        {
            "step": "step 1",
            "crowd": 100,
            "timer": false,
        },
        {
            "step": "step 2",
            "crowd": 100,
            "timer": false,
        }
    ]
}
```

|  字段 | 说明  |
| ------------- | ------------- | 
| step | 流程中定义的节点名称 |
| crowd | 处于当前节点上的人数 |
| timer | 是否是一个timer类型的step |


### 添加人群到节点
把指定人群放置到目标节点
#### URL PATH
`/v1/workflows/{tenantId}/{workflowId}/steps/{stepName}`

#### URL Params
| Query String  Name | 说明  |Optional | Default Value |
| ------------- | ------------- | ------------- | ------------- | 
| tenantId  | 租户ID  | No |  |
| workflowId | workflowId  | No |  |
| stepName | 节点名称  | No |  |

#### HTTP Method
`PUT`

#### HTTP Body
Bitmap序列化的二进制数据

#### Result JSON
```json
{
    "code": 0,
}
```

### 从节点移除人群
把指定人群从目标节点移除
#### URL PATH
`/v1/workflows/{tenantId}/{workflowId}/steps/{stepName}`

#### URL Params
| Query String  Name | 说明  |Optional | Default Value |
| ------------- | ------------- | ------------- | ------------- | 
| tenantId  | 租户ID  | No |  |
| workflowId | workflowId  | No |  |
| stepName | 节点名称  | No |  |

#### HTTP Method
`DELETE`

#### HTTP Body
Bitmap序列化的二进制数据

#### Result JSON
```json
{
    "code": 0,
}
```

### 停止worker
停止worker
#### URL PATH
`/v1/workflows/{tenantId}/{workflowId}/workers/{workerIndex}/stop`

#### URL Params
| Query String  Name | 说明  |Optional | Default Value |
| ------------- | ------------- | ------------- | ------------- | 
| tenantId  | 租户ID  | No |  |
| workflowId | workflowId  | No |  |
| workerIndex | worker index  | No |  |

#### HTTP Method
`PUT`

#### Result JSON
```json
{
    "code": 0,
}
```

### 启动worker
启动worker
#### URL PATH
`/v1/workflows/{tenantId}/{workflowId}/workers/{workerIndex}/start`

#### URL Params
| Query String  Name | 说明  |Optional | Default Value |
| ------------- | ------------- | ------------- | ------------- | 
| tenantId  | 租户ID  | No |  |
| workflowId | workflowId  | No |  |
| workerIndex | worker index  | No |  |

#### HTTP Method
`PUT`

#### Result JSON
```json
{
    "code": 0,
}
```

### 销毁worker
销毁worker，销毁后，当前worker上的人群会均分到剩余的workers上去
#### URL PATH
`/v1/workflows/{tenantId}/{workflowId}/workers/{workerIndex}`

#### URL Params
| Query String  Name | 说明  |Optional | Default Value |
| ------------- | ------------- | ------------- | ------------- | 
| tenantId  | 租户ID  | No |  |
| workflowId | workflowId  | No |  |
| workerIndex | worker index  | No |  |

#### HTTP Method
`DELETE`

#### Result JSON
```json
{
    "code": 0,
}
```

### 队列管理

### Profile管理

### ID-Mapping管理