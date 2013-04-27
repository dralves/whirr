/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.whirr.service.hadoop;

import static org.apache.whirr.RolePredicates.role;
import static org.apache.whirr.service.hadoop.HadoopCluster.getJobTracker;
import static org.apache.whirr.service.hadoop.HadoopCluster.getNamenode;
import static org.apache.whirr.util.Utils.preferredAddress;
import static org.apache.whirr.util.Utils.preferredHostname;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;

import org.apache.whirr.Cluster;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.FirewallManager.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopNameNodeClusterActionHandler extends HadoopClusterActionHandler {

  private static final Logger LOG =
    LoggerFactory.getLogger(HadoopNameNodeClusterActionHandler.class);
  
  public static final String ROLE = "hadoop-namenode";
  
  @Override
  public String getRole() {
    return ROLE;
  }
  
  @Override
  protected void doBeforeConfigure(ClusterActionEvent event) throws IOException {
    Cluster cluster = event.getCluster();
    
    Instance namenode = cluster.getInstanceMatching(role(ROLE));
    event.getFirewallManager().addRules(
        Rule.create()
          .destination(namenode)
          .ports(HadoopCluster.NAMENODE_WEB_UI_PORT),
        Rule.create()
          .source(preferredAddress(namenode,event.getClusterSpec()).getHostAddress())
          .destination(namenode)
          .ports(HadoopCluster.NAMENODE_PORT, HadoopCluster.JOBTRACKER_PORT)
    );
    
  }
  
  @Override
  protected void afterConfigure(ClusterActionEvent event) throws IOException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();
    
    // TODO: wait for TTs to come up (done in test for the moment)
    
    LOG.info("Completed configuration of {} role {}", clusterSpec.getClusterName(), getRole());

    LOG.info("Namenode web UI available at http://{}:{}",
      HadoopCluster.getNamenode(cluster).getPublicAddress(), HadoopCluster.NAMENODE_WEB_UI_PORT);

    Properties config = createClientSideProperties(clusterSpec, getNamenode(cluster), getJobTracker(cluster));
    createClientSideHadoopSiteFile(clusterSpec, config);
    createProxyScript(clusterSpec, cluster);
    Properties combined = new Properties();
    combined.putAll(cluster.getConfiguration());
    combined.putAll(config);
    event.setCluster(new Cluster(cluster.getInstances(), combined));
  }

  private Properties createClientSideProperties(ClusterSpec clusterSpec,
      Instance namenode, Instance jobtracker) throws IOException {
    Properties config = new Properties();
    // set the public ui's to the public addresses independently of the preference
    config.setProperty("dfs.http.address", namenode.getPublicHostName());
    config.setProperty("mapred.job.tracker.http.address", jobtracker.getPublicHostName());

    // set the internal addresses to whatever the user chose
    config.setProperty("hadoop.job.ugi", "root,root");
    config.setProperty("fs.default.name", String.format("hdfs://%s:8020/", preferredHostname(namenode, clusterSpec)));
    if (jobtracker != null) {
      config.setProperty("mapred.job.tracker", String.format("%s:8021", preferredHostname(jobtracker, clusterSpec)));
    }
    config.setProperty("hadoop.socks.server", "localhost:6666");
    config.setProperty("hadoop.rpc.socket.factory.class.default", "org.apache.hadoop.net.SocksSocketFactory");
    // See https://issues.apache.org/jira/browse/HDFS-3068
    config.setProperty("dfs.client.use.legacy.blockreader", "true");
    if (clusterSpec.getProvider().endsWith("ec2")) {
      config.setProperty("fs.s3.awsAccessKeyId", clusterSpec.getIdentity());
      config.setProperty("fs.s3.awsSecretAccessKey", clusterSpec.getCredential());
      config.setProperty("fs.s3n.awsAccessKeyId", clusterSpec.getIdentity());
      config.setProperty("fs.s3n.awsSecretAccessKey", clusterSpec.getCredential());
    }
    return config;
  }

  private void createClientSideHadoopSiteFile(ClusterSpec clusterSpec, Properties config) {
    File configDir = getConfigDir(clusterSpec);
    File hadoopSiteFile = new File(configDir, "hadoop-site.xml");
    HadoopConfigurationConverter.createClientSideHadoopSiteFile(hadoopSiteFile, config);
  }
  
  private File getConfigDir(ClusterSpec clusterSpec) {
    File configDir = new File(new File(System.getProperty("user.home")),
        ".whirr");
    configDir = new File(configDir, clusterSpec.getClusterName());
    configDir.mkdirs();
    return configDir;
  }
  
  private void createProxyScript(ClusterSpec clusterSpec, Cluster cluster) {
    File configDir = getConfigDir(clusterSpec);
    File hadoopProxyFile = new File(configDir, "hadoop-proxy.sh");
    try {
      HadoopProxy proxy = new HadoopProxy(clusterSpec, cluster);
      InetAddress namenode = getNamenode(cluster).getPublicAddress();
      String script = String.format("echo 'Running proxy to Hadoop cluster at %s. " +
          "Use Ctrl-c to quit.'\n", namenode.getHostName())
          + Joiner.on(" ").join(proxy.getProxyCommand());
      Files.write(script, hadoopProxyFile, Charsets.UTF_8);
      hadoopProxyFile.setExecutable(true);
      LOG.info("Wrote Hadoop proxy script {}", hadoopProxyFile);
    } catch (IOException e) {
      LOG.error("Problem writing Hadoop proxy script {}", hadoopProxyFile, e);
    }
  }

}
