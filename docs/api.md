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

## 队列管理
### 租户队列列表
#### URL PATH
`/v1/queues/{tenantId}`

#### HTTP Method
`GET`

#### URL Params
| Query String  Name | 说明  |Optional | Default Value |
| ------------- | ------------- | ------------- | ------------- | 
| tenantId  | 租户ID  | No |  |

#### Result JSON
```json
{
    "code": 0,
    "data": [
        {
            "id": 1,
            "inputs": [
                {
                    "partition": 1,
                    "maxOffset": 1000,
                    "cleanedOffset": 500,
                    "consumers": [
                        {
                            "name": "consumer1",
                            "completedOffset": 10,
                            "lastTimestamp": 1000000
                        },
                        {
                            "name": "consumer2",
                            "completedOffset": 12,
                            "lastTimestamp": 1000000
                        }
                    ]  
                },
                {
                    "partition": 2,
                    "maxOffset": 1000,
                    "cleanedOffset": 500,
                    "consumers": [
                        {
                            "name": "consumer1",
                            "completedOffset": 10,
                            "lastTimestamp": 1000000
                        },
                        {
                            "name": "consumer2",
                            "completedOffset": 12,
                            "lastTimestamp": 1000000
                        }
                    ]  
                }
            ],
            "output": {
                "maxOffset": 1000,
                "cleanedOffset": 500,
                "consumers": [
                    {
                        "name": "consumer1",
                        "completedOffset": 10,
                        "lastTimestamp": 1000000
                    },
                    {
                        "name": "consumer2",
                        "completedOffset": 12,
                        "lastTimestamp": 1000000
                    }
                ]  
            }
        }
    ]
}
```

* 外层结构体

|  字段 | 说明  |
| ------------- | ------------- | 
| id | 租户ID |
| inputs | 租户的input的事件流的队列，input队列分成多个partition，每个partition有独立的offset管理 |
| output |  租户的output的事件流的队列，output只有一个partition |


* inputs数组结构体

|  字段 | 说明  |
| ------------- | ------------- | 
| partition | partition ID |
| maxOffset | 最大的offset，标记着最后一个队列元素的offset |
| cleanedOffset | 已经清理的offset，maxOffset-cleanedOffset代表着磁盘上还存储的队列的元素的个数 |
| consumers |  消费者集合 |

* ouput数组结构体

|  字段 | 说明  |
| ------------- | ------------- | 
| maxOffset | 最大的offset，标记着最后一个队列元素的offset |
| cleanedOffset | 已经清理的offset，maxOffset-cleanedOffset代表着磁盘上还存储的队列的元素的个数 |
| consumers |  消费者集合 |

* consumers数组结构体

|  字段 | 说明  |
| ------------- | ------------- | 
| name | 消费者名称，唯一标示一个consumer |
| completedOffset | 当前消费者已经消费完成的offset |
| lastTimestamp | 该消费者最后一个拉取消息的时间戳，Unix时间戳，精确到秒 |


### 清理队列
强制清理租户下所有的队列的存储空间，释放磁盘存储空间

#### URL PATH
`/v1/queues/{tenantId}/clean`

#### HTTP Method
`DELETE`

#### URL Params
| Query String  Name | 说明  |Optional | Default Value |
| ------------- | ------------- | ------------- | ------------- | 
| tenantId  | 租户ID  | No |  |

#### Result JSON
```json
{
    "code": 0,
}
```

### 修改consumer的completedOffset
修改某一个consumer的completedOffset，让这个consumer跳过消费一些数据，或者重新消费一些数据

#### URL PATH
`/v1/queues/{tenantId}/offset`

#### HTTP Method
`PUT`

#### URL Params
| Query String  Name | 说明  |Optional | Default Value |
| ------------- | ------------- | ------------- | ------------- | 
| tenantId  | 租户ID  | No |  |

#### HTTP Body
```json
{
    "consumer": "consumer name",
    "inputs": [
        {
            "partition": 1,
            "completedOffset": 100
        }
    ],
    "output": 100,
}
```

|  字段 | 说明 |
| ------------- | ------------- | 
| consumer | 哪一个consumer |
| inputs | 可以为空数组，代表不修改inputs的offset，内部元素是需要修改的partition的offset，也就是支持批量更新 |
| output | output的completedOffset |

#### Result JSON
```json
{
    "code": 0,
}
```

### 添加队列元素
向队列中添加一些数组元素

#### URL PATH
`/v1/queues/{tenantId}/items`

#### HTTP Method
`PUT`

#### URL Params
| Query String  Name | 说明  |Optional | Default Value |
| ------------- | ------------- | ------------- | ------------- | 
| tenantId  | 租户ID  | No |  |

#### HTTP Body
```json
{
    "inputs": [
        {
            "partition": 1,
            "items": [
                "元素1内容的base64编码",
                "元素2内容的base64编码",
                "元素3内容的base64编码",
            ]
        },
        {
            "partition": 2,
            "items": [
                "元素1内容的base64编码",
                "元素2内容的base64编码",
                "元素3内容的base64编码",
            ]
        }
    ],
    "output": [
        "元素1内容的base64编码",
        "元素2内容的base64编码",
        "元素3内容的base64编码",
    ]
}
```

|  字段 | 说明 |
| ------------- | ------------- | 
| consumer | 哪一个consumer |
| inputs | 可以为空数组，代表不添加inputs的队列item，支持批量更新，items数组中的每个元素是二进制数据的base64编码 |
| output | 可以为空数组, items数组中的每个元素是二进制数据的base64编码 |

#### Result JSON
```json
{
    "code": 0,
}
```

## Profile管理
## ID-Mapping管理