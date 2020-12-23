package com.aliyun.spark.sdk;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;


public class JsonConfig {
    private String className;
    private Map<String, String> conf;
    private List<String> jars;
    private List<String> pyFiles;
    private List<String> files;
    private List<String> sqls;
    private String name;
    private String file;
    private List<String> args;

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public Map<String, String> getConf() {
        return conf;
    }

    public void setConf(Map<String, String> conf) {
        this.conf = conf;
    }

    public List<String> getJars() {
        return jars;
    }

    public void setJars(List<String> jars) {
        this.jars = jars;
    }

    public List<String> getPyFiles() {
        return pyFiles;
    }

    public void setPyFiles(List<String> pyFiles) {
        this.pyFiles = pyFiles;
    }

    public List<String> getSqls() {
        return sqls;
    }

    public void setSqls(List<String> sqls) {
        this.sqls = sqls;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file = file;
    }

    public List<String> getArgs() {
        return args;
    }

    public void setArgs(List<String> args) {
        this.args = args;
    }

    public List<String> getFiles() {
        return files;
    }

    public void setFiles(List<String> files) {
        this.files = files;
    }


    public static void configPyFiles(JsonConfig jsonConfig, String _pyFiles){
        configInfos(_pyFiles, jsonConfig::setPyFiles);
    }

    public static void configFiles(JsonConfig jsonConfig, String _files){
        configInfos(_files, jsonConfig::setFiles);
    }

    public static void configJars(JsonConfig jsonConfig, String _jars){
        configInfos(_jars, jsonConfig::setJars);
    }

    public static void configArgs(JsonConfig jsonConfig, List<String> _args){
        if (_args != null && !_args.isEmpty()) {
            jsonConfig.setArgs(_args);
        }
    }

    private static void configInfos(String infoStr, SetInfosInterface func){
        if(!StringUtils.isBlank(infoStr)){
            List<String> infos = Arrays.asList(infoStr.split(","));
            func.setInfo(infos);
        }
    }

    interface SetInfosInterface{
        void setInfo(List<String> infos);
    }



}

