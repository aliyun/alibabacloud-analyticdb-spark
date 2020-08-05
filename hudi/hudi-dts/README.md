## 1. 说明

通过Spark StructStreaming订阅DTS数据，可处理多库多表，以HUDI格式写入OSS，并将HUDI表自动同步至DLA，在DLA可即时查询查询。

代码入口类：SparkHudiDts

## 2. 前提

### 2.1 编译

#### 2.1.1 编译Hudi

* 下载[https://github.com/lw309637554/incubator-hudi/tree/release-0.5.2-DLA](https://github.com/lw309637554/incubator-hudi/tree/release-0.5.2-DLA)
* 使用命令 `mvn clean install -DskipTests -Drat.skip=true` 编译或参考[https://github.com/apache/hudi#building-apache-hudi-from-source](https://github.com/apache/hudi#building-apache-hudi-from-source)

#### 2.1.2 编译DTS

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
| DTS_USERNAME           | 订阅DTS用户名，可在 DTS控制台 -> 订阅配置 -> 基本信息 查看 |
| DTS_PASSWORD           | 订阅DTS密码，可在 DTS控制台 -> 订阅配置 -> 基本信息 查看   |
| OFFSET                 | 消费位点                                                   |
| GROUPID                | 消费组ID，可在 DTS控制台->数据消费 查看                    |
| BOOTSTRAP_SERVER       | 订阅DTS密码，可在 DTS控制台 -> 订阅配置 -> 网络 查看       |
| MAX_OFFSET_PER_TRIGGER | 每批次拉多少条数据，默认10000                              |
| SUBSCRIBE_TOPIC        | 订阅topic，可在 DTS控制台 -> 订阅配置 -> 基本信息 查看     |

### 3.2 DLA配置

| 配置项             | 说明                                  |
| ------------------ | ------------------------------------- |
| ENABLE_SYNC_TO_DLA | 是否开启自动同步至DLA，默认开启(true) |
| DLA_USERNAME       | 同步至DLA JDBC用户名                  |
| DLA_PASSWORD       | 同步至DLA JDBC密码                    |
| DLA_JDBC_URL       | 同步至DLA JDBC链接                    |

> 注意：开启ENABLE_SYNC_TO_DLA时，需要将deps/dla-hudi-format-1.0-SNAPSHOT.jar放入classpath，以便自动同步

### 3.3 HUDI配置

| 配置项                                  | 说明                                            |
| --------------------------------------- | ----------------------------------------------- |
| BATH_PATH                               | 同步DTS至OSS的根目录，默认为/tmp/hudi_dts_demo/ |
| HOODIE_COMPACT_INLINE                   | 写入时是否开启内联compaction，默认为true        |
| HOODIE_COMPACT_INLINE_MAX_DELTA_COMMITS | 写入多少个delta提交时进行compaction，默认为10   |
| HOODIE_TABLE_TYPE                       | hudi表类型，默认为MERGE_ON_READ                 |
| HOODIE_INSERT_SHUFFLE_PARALLELISM       | insert并发度，默认为3                           |
| HOODIE_UPSERT_SHUFFLE_PARALLELISM       | upssert并发度，默认为3                          |
| HOODIE_ENABLE_TIMELINE_SERVER           | 是否开启timeline，默认关闭(false)               |


## 使用过程有任何问题，欢迎扫描钉钉群交流

![钉钉群](./pics/dingding.png)
