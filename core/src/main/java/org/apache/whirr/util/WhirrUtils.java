package org.apache.whirr.util;

import java.io.PrintStream;

import org.apache.whirr.Cluster;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterSpec;

public class WhirrUtils {

  /**
   * Prints an ssh command that can be used to login into the nodes
   * 
   * @param out
   * @param clusterSpec
   * @param cluster
   */
  public static void printAccess(PrintStream out, ClusterSpec clusterSpec,
      Cluster cluster, int maxPrint) {
    out.println("You can log into instances using the following ssh commands:");

    String user = clusterSpec.getLoginUser() != null ? clusterSpec
        .getLoginUser() : clusterSpec.getClusterUser();

    String pkFile = clusterSpec.getPrivateKeyFile().getAbsolutePath();

    int counter = 0;
    for (Instance instance : cluster.getInstances()) {

      out.printf(
          "'ssh -i %s -o \"UserKnownHostsFile /dev/null\" -o StrictHostKeyChecking=no %s@%s'\n",
          pkFile, user, instance.getPublicIp());

      if (counter > maxPrint) {
        out.println("... Too many instances, truncating.");
        break;
      }
      counter++;
    }
  }
}
