package com.github.nicholasjgreen.s3tohdfs;

import java.io.File;
import java.io.IOException;
import java.net.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

public class ApplicationMaster {

    public static void main(String[] args) throws Exception {
        final int n = Integer.valueOf(args[0]);

        System.out.println("Running ApplicationMaster!");
        System.out.println("Args: ");
        for(int i = 0; i < args.length; i++){
            System.out.println(args[i]);
        }

        System.out.println("ENV CLASSPATH: " + System.getenv("CLASSPATH"));
        System.out.println("MY CLASSPATH ...");
        ClassLoader cl = ClassLoader.getSystemClassLoader();
        if (cl instanceof URLClassLoader) {
            for (java.net.URL url : ((URLClassLoader)cl).getURLs()) {
                System.out.println(url);
            }
        }
        System.out.println("---[END]---");

        // Initialize clients to ResourceManager and NodeManagers
        Configuration conf = new YarnConfiguration();

        AMRMClient<ContainerRequest> rmClient = AMRMClient.createAMRMClient();
        rmClient.init(conf);
        rmClient.start();

        NMClient nmClient = NMClient.createNMClient();
        nmClient.init(conf);
        nmClient.start();

        // Register with ResourceManager
        System.out.println("registerApplicationMaster 0");
        rmClient.registerApplicationMaster("", 0, "");
        System.out.println("registerApplicationMaster 1");

        // Priority for worker containers - priorities are intra-application
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        Path jarPath = new Path("/apps/s3tohdfs/s3tohdfs-0.1.0-jar-with-dependencies.jar");
        jarPath = FileSystem.get(conf).makeQualified(jarPath);

        // Setup jar for Container
        LocalResource appMasterJar = Records.newRecord(LocalResource.class);
        setupContainerJar(conf, jarPath, appMasterJar);

        // Setup CLASSPATH for Container
        Map<String, String> containerEnv = new HashMap<String, String>();
        setupContainerEnv(conf, containerEnv);

        // Set up resource type requirements for Container
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(256);
        capability.setVirtualCores(1);

        // Make container requests to ResourceManager
        for (int i = 0; i < n; ++i) {
            ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
            System.out.println("Making res-req " + i);
            rmClient.addContainerRequest(containerAsk);
        }

        // Obtain allocated containers and launch
        int allocatedContainers = 0;
        // We need to start counting completed containers while still allocating
        // them since intial ones may complete while we're allocating subsequent
        // containers and if we miss those notifications, we'll never see them again
        // and this ApplicationMaster will hang indefinitely.
        int completedContainers = 0;
        while (allocatedContainers < n) {
            AllocateResponse response = rmClient.allocate(0);
            for (Container container : response.getAllocatedContainers()) {
                ++allocatedContainers;

                // Launch container by create ContainerLaunchContext
                ContainerLaunchContext appContainer =
                        Records.newRecord(ContainerLaunchContext.class);
                appContainer.setLocalResources(
                        Collections.singletonMap("simpleapp.jar", appMasterJar));
                appContainer.setEnvironment(containerEnv);
                String command = "$JAVA_HOME/bin/java" +
                        " -Xmx256M" +
                        " com.github.nicholasjgreen.s3tohdfs.RetrieveFromS3" +
                        " " + String.valueOf(allocatedContainers) +
                        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";
                System.out.println("Command: " + command);
                appContainer.setCommands(Collections.singletonList(command));

                System.out.println("Launching container " + allocatedContainers);
                nmClient.startContainer(container, appContainer);
            }
            for (ContainerStatus status : response.getCompletedContainersStatuses()) {
                ++completedContainers;
                System.out.println("Completed container " + completedContainers);
            }
            Thread.sleep(100);
        }

        // Now wait for the remaining containers to complete
        while (completedContainers < n) {
            AllocateResponse response = rmClient.allocate(completedContainers / n);
            for (ContainerStatus status : response.getCompletedContainersStatuses()) {
                ++completedContainers;
                System.out.println("Completed container " + completedContainers);
            }
            Thread.sleep(100);
        }

        // Un-register with ResourceManager
        rmClient.unregisterApplicationMaster(
                FinalApplicationStatus.SUCCEEDED, "", "");
        System.out.println("Done!");
    }

    private static void setupContainerJar(Configuration conf, Path jarPath, LocalResource appMasterJar) throws IOException {
        FileStatus jarStat = FileSystem.get(conf).getFileStatus(jarPath);
        appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
        appMasterJar.setSize(jarStat.getLen());
        appMasterJar.setTimestamp(jarStat.getModificationTime());
        appMasterJar.setType(LocalResourceType.FILE);
        appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC);
    }


    private static void setupContainerEnv(Configuration conf, Map<String, String> containerEnv) {
//        String classPathEnv = "$CLASSPATH:./*";
        String classPathEnv = "./*";
        containerEnv.put("CLASSPATH", classPathEnv);

        String[] defaultYarnAppClasspath = conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH);
        System.out.println("*** YARN_APPLICATION_CLASSPATH: " +
                Arrays.asList(defaultYarnAppClasspath != null ? defaultYarnAppClasspath : new String[]{}));

        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            System.out.println("--> " + c);
            Apps.addToEnvironment(containerEnv, ApplicationConstants.Environment.CLASSPATH.name(),
                    c.trim(), File.pathSeparator);
        }

      /*Apps.addToEnvironment(containerEnv,
          ApplicationConstants.Environment.CLASSPATH.name(),
          ApplicationConstants.Environment.PWD.$() + File.separator + "*");
*/
        System.out.println("*** APP CONTAINER ENV: " +containerEnv);
    }

}
