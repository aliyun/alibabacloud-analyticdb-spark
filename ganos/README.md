## DLA Ganos 测试案例

### 数据准备
Ganos案例部分数据已经包含在src/main/resources文件夹中

SpatialJoin案例需要单独下载如下数据：
* [LC08_L1TP_121035_20190702_20190706_01_T1.tif](https://dla-ganos-bj.oss-cn-beijing.aliyuncs.com/public/LC08_L1TP_121035_20190702_20190706_01_T1.TIF)
* [srtm_60_05.tif](https://dla-ganos-bj.oss-cn-beijing.aliyuncs.com/public/srtm_60_05.tif)

下载完成后将文件放在src/main/resources目录下。

### 案例介绍

DLA Ganos案例包含两部分：
#### 数据源 DataSource
包含DLA Ganos支持的数据源：
* Lindorm
* PolarDB
* OSS
* GeoTiff
* GeoMesa

#### 测试案例
* LocalAlgorithm: 基本栅格代数运算.
* NDVI: 自定义UDF，计算NDVI.
* Masking: 栅格数据掩膜(Masking)操作.
* OSS2HBase: 加载OSS栅格数据，进行拼接、重投影、创建金字塔等操作，最后写入Lindorm(HBase)中.
* SpatialJoin: 将空间参考CRS和空间范围不同的栅格数据按照空间进行Join操作.
* Classificaiton: 基于SparkML进行遥感影像监督分类.