package org.apache.whirr.service;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertSame;
import static junit.framework.Assert.fail;
import static org.apache.whirr.service.DryRunModuleTest.getScriptName;

import java.util.Collection;
import java.util.Map;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.whirr.ClusterController;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.DryRunModule.DryRun;
import org.jclouds.compute.callables.RunScriptOnNode;
import org.jclouds.compute.domain.NodeMetadata;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Iterables;

public class StatesDryRunTest {

  @Before
  public void setUp() {
    DryRunModule.resetDryRun();
  }

  @Test
  public void testStagesDontOverlapSingleNode() throws Exception {
    CompositeConfiguration config = new CompositeConfiguration();
    config.setProperty("whirr.provider", "stub");
    config.setProperty("whirr.cluster-name", "stub-test");
    config.setProperty("whirr.instance-templates",
        "1 service-a+service-b+service-c+service-e");
    config.setProperty("whirr.state-store", "memory");

    ClusterSpec clusterSpec = ClusterSpec.withTemporaryKeys(config);
    ClusterController controller = new ClusterController();

    controller.launchCluster(clusterSpec);
    controller.destroyCluster(clusterSpec);

    DryRun dryRun = DryRunModule.getDryRun();
    Collection<RunScriptOnNode> executions = dryRun.getExecutions().values();

    // With this configuration we expect scripts for service a to be executed
    // first, then scripts for service b and and e (which depend on a) and then
    // scripts for service c (which depends on b)
    assertSame("Wrong number of scripts.", 10, executions.size());
    assertEquals("Wrong execution order", "configure-service-a",
        getScriptName(Iterables.get(executions, 1)));
    assertEquals("Wrong execution order", "configure-service-b_service-e",
        getScriptName(Iterables.get(executions, 2)));
    assertEquals("Wrong execution order", "configure-service-c",
        getScriptName(Iterables.get(executions, 3)));
    assertEquals("Wrong execution order", "start-service-a",
        getScriptName(Iterables.get(executions, 4)));
    assertEquals("Wrong execution order", "start-service-b_service-e",
        getScriptName(Iterables.get(executions, 5)));
    assertEquals("Wrong execution order", "start-service-c",
        getScriptName(Iterables.get(executions, 6)));
    assertEquals("Wrong execution order", "destroy-service-a",
        getScriptName(Iterables.get(executions, 7)));
    assertEquals("Wrong execution order", "destroy-service-b_service-e",
        getScriptName(Iterables.get(executions, 8)));
    assertEquals("Wrong execution order", "destroy-service-c",
        getScriptName(Iterables.get(executions, 9)));
  }

  @Test
  public void testStagesDontOverlapTwoNodes() throws Exception {
    CompositeConfiguration config = new CompositeConfiguration();
    config.setProperty("whirr.provider", "stub");
    config.setProperty("whirr.cluster-name", "stub-test");
    config.setProperty("whirr.instance-templates",
        "1 service-a+service-b+service-c, 1 service-a+service-e");
    config.setProperty("whirr.state-store", "memory");

    ClusterSpec clusterSpec = ClusterSpec.withTemporaryKeys(config);
    ClusterController controller = new ClusterController();

    controller.launchCluster(clusterSpec);
    controller.destroyCluster(clusterSpec);

    DryRun dryRun = DryRunModule.getDryRun();
    Map<NodeMetadata, Collection<RunScriptOnNode>> executionsPerNode = dryRun
        .getExecutions().asMap();

    Collection<RunScriptOnNode> node1Executions = null;
    Collection<RunScriptOnNode> node2Executions = null;

    Collection<RunScriptOnNode> execution = Iterables.get(
        executionsPerNode.values(), 0);
    if (execution.size() == 10) {
      node1Executions = execution;
      node2Executions = Iterables.get(executionsPerNode.values(), 1);
    } else if (execution.size() == 7) {
      node2Executions = execution;
      node1Executions = Iterables.get(executionsPerNode.values(), 1);
    } else {
      fail("Expected wither 10 or 7 executions but was: " + execution.size());
    }

    // With this configuration we expect scripts for service a to be executed
    // first on both nodes and at the same time, then scripts for service b and
    // e, which depend on a,in both nodes at the same time and then the script
    // for service c on the first node.

    assertSame("Wrong number of executions", 10, node1Executions.size());
    assertSame("Wrong number of executions", 7, node2Executions.size());

    // both nodes execute setup
    assertEquals("Wrong script execution order", "setup-dralves",
        getScriptName(Iterables.get(node1Executions, 0)));
    assertEquals("Wrong script execution order", "setup-dralves",
        getScriptName(Iterables.get(node2Executions, 0)));
    // both nodes configure the first stage (service-a)
    assertEquals("Wrong script execution order", "configure-service-a",
        getScriptName(Iterables.get(node1Executions, 1)));
    assertEquals("Wrong script execution order", "configure-service-a",
        getScriptName(Iterables.get(node2Executions, 1)));
    // both nodes configure the second stage (service-b for node 1 and service-e
    // for node 2)
    assertEquals("Wrong script execution order", "configure-service-b",
        getScriptName(Iterables.get(node1Executions, 2)));
    assertEquals("Wrong script execution order", "configure-service-e",
        getScriptName(Iterables.get(node2Executions, 2)));
    // node 1 configures the third stage
    assertEquals("Wrong script execution order", "configure-service-c",
        getScriptName(Iterables.get(node1Executions, 3)));
    // same as before but for the start phase
    assertEquals("Wrong script execution order", "start-service-a",
        getScriptName(Iterables.get(node1Executions, 4)));
    assertEquals("Wrong script execution order", "start-service-a",
        getScriptName(Iterables.get(node2Executions, 3)));
    assertEquals("Wrong script execution order", "start-service-b",
        getScriptName(Iterables.get(node1Executions, 5)));
    assertEquals("Wrong script execution order", "start-service-e",
        getScriptName(Iterables.get(node2Executions, 4)));
    assertEquals("Wrong script execution order", "start-service-c",
        getScriptName(Iterables.get(node1Executions, 6)));
    // same as before but for the destroy phase
    assertEquals("Wrong script execution order", "destroy-service-a",
        getScriptName(Iterables.get(node1Executions, 7)));
    assertEquals("Wrong script execution order", "destroy-service-a",
        getScriptName(Iterables.get(node2Executions, 5)));
    assertEquals("Wrong script execution order", "destroy-service-b",
        getScriptName(Iterables.get(node1Executions, 8)));
    assertEquals("Wrong script execution order", "destroy-service-e",
        getScriptName(Iterables.get(node2Executions, 6)));
    assertEquals("Wrong script execution order", "destroy-service-c",
        getScriptName(Iterables.get(node1Executions, 9)));
  }
}
