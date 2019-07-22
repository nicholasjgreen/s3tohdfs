package com.github.nicholasjgreen.s3tohdfs;

public class Constants {
    public static String AppName = "s3tohdfs";
    public static String JarName = "s3tohdfs.jar";

    // Hdfs properties
    public static String HdfsBaseAppPath = "/apps";
    public static String HdfsAppPath = HdfsBaseAppPath + "/" + AppName;
    public static String HdfsJarPath = HdfsAppPath + "/" + JarName;

}
