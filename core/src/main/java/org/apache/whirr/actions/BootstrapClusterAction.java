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

package org.apache.whirr.actions;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.whirr.Cluster;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterAction;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.InstanceTemplate;
import org.apache.whirr.compute.BootstrapTemplate;
import org.apache.whirr.compute.NodeStarterFactory;
import org.apache.whirr.compute.StartupProcess;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandler;
import org.apache.whirr.service.FirewallManager;
import org.apache.whirr.service.jclouds.StatementBuilder;
import org.apache.whirr.service.jclouds.TemplateBuilderStrategy;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.Template;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * A {@link org.apache.whirr.ClusterAction} that starts instances in a cluster
 * in parallel and runs bootstrap scripts on them.
 */
public class BootstrapClusterAction extends ClusterAction {

  private static final Logger LOG = LoggerFactory
      .getLogger(BootstrapClusterAction.class);

  private final NodeStarterFactory nodeStarterFactory;

  public BootstrapClusterAction(
      final Function<ClusterSpec, ComputeServiceContext> getCompute,
      final Map<String, ClusterActionHandler> handlerMap) {
    this(getCompute, handlerMap, new NodeStarterFactory());
  }

  BootstrapClusterAction(
      final Function<ClusterSpec, ComputeServiceContext> getCompute,
      final Map<String, ClusterActionHandler> handlerMap,
      final NodeStarterFactory nodeStarterFactory) {
    super(getCompute, handlerMap);
    this.nodeStarterFactory = nodeStarterFactory;
  }

  @Override
  protected String getAction() {
    return ClusterActionHandler.BOOTSTRAP_ACTION;
  }

  /**
   * BoostrapClusterAction, contrary to all other actions does not have to
   * comply with stages, so its gets its own execute method where cluster action
   * events still have a one-to-one mapping with InstanceTemplate.
   */
  public Cluster execute(ClusterSpec clusterSpec, Cluster cluster)
      throws IOException, InterruptedException {

    Cluster newCluster = cluster;

    Map<InstanceTemplate, ClusterActionEvent> eventMap = Maps
        .newLinkedHashMap();
    for (InstanceTemplate instanceTemplate : clusterSpec.getInstanceTemplates()) {
      if (shouldIgnoreInstanceTemplate(instanceTemplate)) {
        continue; // skip execution if this group of instances is not in
                  // target
      }
      StatementBuilder statementBuilder = new StatementBuilder();
      ComputeServiceContext computeServiceContext = getCompute().apply(
          clusterSpec);

      FirewallManager firewallManager = new FirewallManager(
          computeServiceContext, clusterSpec, newCluster);

      ClusterActionEvent event = new ClusterActionEvent(getAction(),
          clusterSpec, instanceTemplate, newCluster, statementBuilder,
          getCompute(), firewallManager);

      for (String role : instanceTemplate.getRoles()) {
        if (roleIsInTarget(role)) {
          safeGetActionHandler(role).beforeAction(event);
        }
      }

      newCluster = event.getCluster();

      eventMap.put(instanceTemplate, event);
    }

    LOG.info("Bootstrapping cluster");

    ExecutorService executorService = Executors.newCachedThreadPool();
    Map<InstanceTemplate, Future<Set<? extends NodeMetadata>>> futures = Maps
        .newHashMap();

    // initialize startup processes per InstanceTemplates
    for (Entry<InstanceTemplate, ClusterActionEvent> entry : eventMap
        .entrySet()) {

      final int maxNumberOfRetries = clusterSpec.getMaxStartupRetries();
      StatementBuilder statementBuilder = entry.getValue()
          .getStatementBuilder();

      ComputeServiceContext computeServiceContext = getCompute().apply(
          clusterSpec);
      final ComputeService computeService = computeServiceContext
          .getComputeService();

      final Template template = BootstrapTemplate.build(clusterSpec,
          computeService, statementBuilder, new TemplateBuilderStrategy(),
          entry.getKey());

      if (template.getOptions() != null) {
        template.getOptions().nameTask(
            "bootstrap-" + Joiner.on('_').join(entry.getKey().getRoles()));
      }

      Future<Set<? extends NodeMetadata>> nodesFuture = executorService
          .submit(new StartupProcess(clusterSpec.getClusterName(), entry
              .getKey().getNumberOfInstances(), entry.getKey()
              .getMinNumberOfInstances(), maxNumberOfRetries, entry.getKey()
              .getRoles(), computeService, template, executorService,
              nodeStarterFactory));
      futures.put(entry.getKey(), nodesFuture);
    }

    Set<Instance> instances = Sets.newLinkedHashSet();
    for (Entry<InstanceTemplate, Future<Set<? extends NodeMetadata>>> entry : futures
        .entrySet()) {
      Set<? extends NodeMetadata> nodes;
      try {
        nodes = entry.getValue().get();
      } catch (ExecutionException e) {
        // Some of the StartupProcess decided to throw IOException,
        // to fail the cluster because of insufficient successfully started
        // nodes after retries
        throw new IOException(e);
      }
      Set<String> roles = entry.getKey().getRoles();
      instances.addAll(getInstances(roles, nodes));
    }
    cluster = new Cluster(instances);

    for (Entry<InstanceTemplate, ClusterActionEvent> entry : eventMap
        .entrySet()) {
      if (shouldIgnoreInstanceTemplate(entry.getKey())) {
        continue;
      }
      ClusterActionEvent event = eventMap.get(entry.getKey());
      for (String role : entry.getKey().getRoles()) {
        if (roleIsInTarget(role)) {
          event.setCluster(newCluster);
          safeGetActionHandler(role).afterAction(event);
          // cluster may have been updated by handler
          newCluster = event.getCluster();
        }
      }
    }
    return newCluster;
  }

  private Set<Instance> getInstances(final Set<String> roles,
      Set<? extends NodeMetadata> nodes) {
    return Sets.newLinkedHashSet(Collections2.transform(
        Sets.newLinkedHashSet(nodes), new Function<NodeMetadata, Instance>() {
          @Override
          public Instance apply(NodeMetadata node) {
            return new Instance(node.getCredentials(), roles, Iterables.get(
                node.getPublicAddresses(), 0), Iterables.get(
                node.getPrivateAddresses(), 0), node.getId(), node);
          }
        }));
  }

}
