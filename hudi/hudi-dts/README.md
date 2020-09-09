## 1. 说明

通过Spark StructStreaming订阅DTS数据，可处理多库多表，以HUDI格式写入OSS，并将HUDI表自动同步至DLA，在DLA可即时查询查询。

代码入口类：SparkHudiDts

## 2. 前提

### 2.1 编译

#### 2.1.1 编译DTS

* 下载[https://github.com/LioRoger/subscribe_example](https://github.com/LioRoger/subscribe_example)
* 使用命令 `mvn clean install -DskipTests -Drat.skip=true` 编译

### 2.2 服务及白名单

* 开通DLA、DTS服务
* 在DLA控制开启访问DLA白名单，具体可参考[连接DLA](https://help.aliyun.com/document_detail/110829.html?spm=a2c4g.11186623.6.603.494f327fOSaVoc)

### 2.3 创建库

* 需要提前在DLA中创建对应同步的库

## 3. 配置

> 注意：默认读取/tmp/config.properties配置文件，若自定义，可在程序后带路径启动

### 3.1 DTS配置
| 配置项                 | 说明                                                       |
| ---------------------- | ---------------------------------------------------------- |
| dtsUsername           | 订阅DTS用户名，可在 DTS控制台 -> 订阅配置 -> 基本信息 查看 |
| dtsPassword           | 订阅DTS密码，可在 DTS控制台 -> 订阅配置 -> 基本信息 查看   |
| offset                 | 消费位点                                                   |
| groupId                | 消费组ID，可在 DTS控制台->数据消费 查看                    |
| bootstrapServer       | 订阅DTS密码，可在 DTS控制台 -> 订阅配置 -> 网络 查看       |
| maxOffsetsPerTrigger | 每批次拉多少条数据，默认10000                              |
| subscribeTopic        | 订阅topic，可在 DTS控制台 -> 订阅配置 -> 基本信息 查看     |

### 3.2 DLA配置

| 配置项             | 说明                                  |
| ------------------ | ------------------------------------- |
| enable.sync.to.dla | 是否开启自动同步至DLA，默认开启(true) |
| dla.username      | 同步至DLA JDBC用户名                  |
| dla.password       | 同步至DLA JDBC密码                    |
| dla.jdbc.url       | 同步至DLA JDBC链接                    |
| dla.database       | 同步至DLA的库名                        |


> 注意：开启ENABLE_SYNC_TO_DLA时，需要将deps/dla-hudi-format-1.0-SNAPSHOT.jar放入classpath，以便自动同步

### 3.3 HUDI配置

| 配置项                                  | 说明                                            |
| --------------------------------------- | ----------------------------------------------- |
| basePath                               | 同步DTS至OSS的根目录，默认为/tmp/hudi_dts_demo/ |
| hoodie.compact.inline                   | 写入时是否开启内联compaction，默认为true        |
| hoodie.compact.inline.max.delta.commits | 写入多少个delta提交时进行compaction，默认为10   |
| hoodie.table.type                       | hudi表类型，默认为MERGE_ON_READ                 |
| hoodie.insert.shuffle.parallelism       | insert并发度，默认为3                           |
| hoodie.upsert.shuffle.parallelism       | upssert并发度，默认为3                          |
| hoodie.enable.timeline.server           | 是否开启timeline，默认关闭(false)               |

### 3.4 其他配置
| 配置项                                  | 说明                                            |
| --------------------------------------- | ----------------------------------------------- |
| convert.all.types.to.string             | 是否将所有类型转化为String类型，默认为false |
| convert.decimal.to.string               | 是否将decimal类型转化为String类型，默认为false        |
| decimal.columns.definition   | decimal类型定义，格式为 表名:列名1,precision,scale;列名2,precision,scale 如tableName1:c1,10,2;c2,5,2#tableName2:c,4,2  |

> 注意：如果设置decimal.columns.definition,那么会按照设置的precision/scale确定decimal字段精度,否则会从数据中解析精度(与源库可能不相同);
因此建议设置，或者设置convert.decimal.to.string为true，即将decimal类型转化为String类型


## 使用过程有任何问题，欢迎扫描钉钉群交流

![钉钉群](./pics/dingding.png)
