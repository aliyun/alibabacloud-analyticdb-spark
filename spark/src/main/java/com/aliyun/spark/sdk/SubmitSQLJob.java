package com.aliyun.spark.sdk;


import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.PutObjectRequest;
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.openanalytics_open.model.v20180619.*;
import com.aliyuncs.profile.DefaultProfile;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * 演示如何使用Java SDK操作数据湖分析的spark作业
 *
 * @author aliyun
 */
public class SubmitSQLJob {
    /**
     * 提交一个作业到数据湖分析Serverless Spark
     *
     * @param virtualClusterName 数据湖分析虚拟集群名称
     * @param jobConfig          提交Spark作业的描述文件,需要是JSON格式
     * @return Spark JobId, 提交作业成功, 返回作业的ID, 用于后续的状态跟踪
     * @throws ClientException 提交作业可能因为网络原因等抛出错误
     */
    public static String submitSparkJob(IAcsClient client,
                                        String virtualClusterName,
                                        String jobConfig) throws ClientException {
        // 初始化Request, 填入集群名称和作业内容
        SubmitSparkJobRequest request = new SubmitSparkJobRequest();
        request.setVcName(virtualClusterName);
        request.setConfigJson(jobConfig);
        // 提交作业, 返回Spark作业的JobId
        SubmitSparkJobResponse response = client.getAcsResponse(request);
        return response.getJobId();
    }
    /**
     * 返回一个Spark Job当前的状态
     *
     * @param sparkJobId 用户Spark作业的ID
     * @return 返回Spark作业的状态, 类型为String
     * @throws ClientException 提交作业可能因为网络原因等抛出错
     */
    public static String getSparkJobStatus(IAcsClient client,
                                           String virtualClusterName,
                                           String sparkJobId) throws ClientException {
        // 初始化Request, 填入spark job id
        GetJobStatusRequest request = new GetJobStatusRequest();
        request.setJobId(sparkJobId);
        request.setVcName(virtualClusterName);
        // 提交作业, 返回Spark作业的状态码
        GetJobStatusResponse response = client.getAcsResponse(request);
        return response.getStatus();
    }
    /**
     * 停止一个Spark Job
     *
     * @param sparkJobId         用户Spark作业的ID
     * @param virtualClusterName 数据湖分析虚拟集群名称
     * @return 无返回值
     * @throws ClientException 提交作业可能因为网络原因等抛出错
     */
    public static void killSparkJob(IAcsClient client,
                                    String virtualClusterName,
                                    String sparkJobId) throws ClientException {
        // 初始化Request, 填入spark job id
        KillSparkJobRequest request = new KillSparkJobRequest();
        request.setVcName(virtualClusterName);
        request.setJobId(sparkJobId);
        // 提交作业, 返回Spark作业的状态码
        KillSparkJobResponse response = client.getAcsResponse(request);
    }
    /**
     * 返回一个Spark Job的日志
     *
     * @param client     客户端
     * @param virtualClusterName 数据湖分析虚拟集群名称
     * @param sparkJobId         用户Spark作业的ID
     * @return 返回Spark作业的状态, 类型为String
     * @throws ClientException 提交作业可能因为网络原因等抛出错
     */
    public static String getSparkJobLog(IAcsClient client,
                                 String virtualClusterName,
                                 String sparkJobId) throws ClientException {

        // 初始化Request, 填入spark job id
        GetJobLogRequest request = new GetJobLogRequest();
        request.setJobId(sparkJobId);
        request.setVcName(virtualClusterName);
        // 提交作业, 返回Spark作业的日志
        GetJobLogResponse response = client.getAcsResponse(request);
        return response.getData();
    }
    /**
     * 查询某个虚拟集群上提交的Spark作业, 通过翻页可以遍历所有的历史作业信息
     *
     * @param client     客户端
     * @param pageNumber 查询的页码, 从1开始
     * @param pageSize   每页返回数量
     * @throws ClientException 提交作业可能因为网络原因等抛出错
     */
    public static void listSparkJob(IAcsClient client,
                                    String virtualClusterName,
                                    int pageNumber,
                                    int pageSize) throws ClientException {
        // 初始化Request, 填入spark job id
        ListSparkJobRequest request = new ListSparkJobRequest();
        request.setVcName(virtualClusterName);
        request.setPageNumber(pageNumber); // pageNumber 从1开始
        request.setPageSize(pageSize);
        // 提交作业, 返回Spark作业的状态码
        ListSparkJobResponse response = client.getAcsResponse(request);
        // 获取任务列表
        GsonBuilder gsonBuilder = new GsonBuilder().disableHtmlEscaping().setPrettyPrinting();
        Gson gson = gsonBuilder.create();
        List<ListSparkJobResponse.DataResult.Data> sparkJobList = response.getDataResult().getJobList();
        for (ListSparkJobResponse.DataResult.Data job : sparkJobList) {
            System.err.println("--------------------------------------------------------");
            System.err.println("jobInfo: " + gson.toJson(job));
            System.err.println("jobConfigJson: " + gson.toJson(new JsonParser().parse(job.getDetail())));
            System.err.println("--------------------------------------------------------");
        }
    }
    //TODO: 添加base64编码单个sql的例子， main函数抽象一下
    public static void main(String[] args) throws Exception {
        // 提交任务必须的参数
        if(args.length < 6){
            System.err.println("args0: regionId (e.g cn-hangzhou) args1: accessKeyId args2: accessKeySecret args3: virtualClusterName " +
                    "args4: sqlFileLocation args5: ossUploadPath");
            System.exit(1);
        }
        //vc所在的region
        String region = args[0];
        //阿里云RAM accessKeyId
        String accessKeyId = args[1];
        //阿里云RAM accessKeySecret
        String accessKeySecret = args[2];
        //提交作业的vc Name
        String virtualClusterName = args[3];
        //可以是本地文件绝对路径，也可以是oss文件路径，如果是oss文件路径则直接作为sqls参数，如果是本地文件则会将本地文件读出后进行base64编码再上传到oss
        String sqlFileLocation = args[4];
        //上传sql文件的ossEndpoint 如 oss-cn-hangzhou.aliyuns.com
        String ossEndpoint = "oss-"+region+".aliyuncs.com";
        //上传sql文件的oss目录路径 如 oss://resource/sql/
        String ossUploadPath = args[5];
        JsonConfig jsonConfig = new JsonConfig();
        //设置作业的名称
        jsonConfig.setName("SparkSQL");

        //设置作业运行的Spark参数
        HashMap<String, String> conf = new HashMap<String, String>(){
            {
                //设置driver的规格 2c8g
                put("spark.driver.resourceSpec","medium");
                //设置executor的个数 2
                put("spark.executor.instances", "2");
                //设置executor的规格 2c8g
                put("spark.executor.resourceSpec", "medium");
                //开启默认的oss连接器
                put("spark.dla.connectors", "oss");
                //开启sql使用dla元数据服务
                put("spark.sql.hive.metastore.version", "dla");
            }
        };
        jsonConfig.setConf(conf);

        //设置sql
        configSqls(jsonConfig, sqlFileLocation, ossEndpoint, accessKeyId, accessKeySecret, ossUploadPath);
        //您也可以使用base64编码你的sqls数组（防止出现json中的需要转义的字符，DLA Spark会自动解码base64编码的sql。每个数组元素是一条sql
        //configSqls(jsonConfig, sqlsArray);
        // 需要是一个合法的JSON格式的字符串
        GsonBuilder gsonBuilder = new GsonBuilder().disableHtmlEscaping().setPrettyPrinting();
        Gson gson = gsonBuilder.create();
        String jobConfig = gson.toJson(jsonConfig);
        System.err.println("before submit job: " + jobConfig);

        // 初始化阿里云平台开发Client
        DefaultProfile profile = DefaultProfile.getProfile(region, accessKeyId, accessKeySecret);
        IAcsClient client = new DefaultAcsClient(profile);
        // 提交任务
        String sparkJobId = submitSparkJob(client, virtualClusterName, jobConfig);
        // 轮询任务状态, 超时未完成则杀死任务
        long startTime = System.currentTimeMillis();
        List<String> finalStatusList = Arrays.asList("error", "success", "dead", "killed");
        while (true) {
            String status = getSparkJobStatus(client, virtualClusterName, sparkJobId);
            if (finalStatusList.contains(status)) {
                System.out.println("Job went to final status");
                break;
            } else if ((System.currentTimeMillis() - startTime) > 100000000) {
                // 如果超时则杀死job
                System.out.println("Kill expire time job");
                killSparkJob(client, virtualClusterName, sparkJobId);
                break;
            }
            // 打印状态, 等待5秒, 进入下一轮查询
            System.out.printf("Job %s status is %s%n", sparkJobId, status);
            Thread.sleep(5000);
        }
        // 打印作业的日志
        String logDetail = getSparkJobLog(client, virtualClusterName, sparkJobId);
        System.out.println(logDetail);
        // 打印最近10条作业的明细
        listSparkJob(client, virtualClusterName, 1, 10);
    }

    /**
     * 上传至oss file
     * @param jsonConfig jsonconfig 对象
     * @param sqlFile sqlfile的路径（本地路径或者oss路径）
     * @param ossEndpoint oss的endpoint  i.e oss-cn-hangzhou.aliyuncs.com
     * @param ossKeyId 访问oss所需的 accessKeyId
     * @param ossKeySecret 访问oss 所需的 accessKeySecret
     * @param ossUploadPath 将本地sql文件上传到ossUploadPath指定的目录
     * @throws Exception putObject 或者 URI抛出的异常
     */
    public static void configSqls(JsonConfig jsonConfig, String sqlFile, String ossEndpoint, String ossKeyId, String ossKeySecret, String ossUploadPath) throws Exception{
        String[] sqlsArray;
        if(sqlFile.startsWith("oss://")){
            sqlsArray = new String[]{sqlFile};
        }else{
            OSS oss = getOSSClient(ossEndpoint, ossKeyId, ossKeySecret);
            String sqlFilePath =  ossUploadPath +  UUID.randomUUID().toString() + ".sql";
            URI uri = new URI(sqlFilePath);
            String bucket = uri.getHost();
            String key = uri.getPath().substring(1);
            oss.putObject(bucket, key, new FileInputStream(sqlFilePath));
            sqlsArray = new String[]{sqlFilePath};
        }
        jsonConfig.setSqls(Arrays.asList(sqlsArray));
    }

    public static void configSqls(JsonConfig jsonConfig, String[] sqls){
        sqls = Arrays.stream(sqls).map(sql -> new String(Base64.getEncoder().encode(sql.getBytes()))).toArray(String[]::new);
        jsonConfig.setSqls(Arrays.asList(sqls));
    }


    public static OSS getOSSClient(String _ossEndpoint, String _accessKeyId, String _accessSecretId){
        return new OSSClientBuilder().build(_ossEndpoint, _accessKeyId, _accessSecretId);
    }
}
