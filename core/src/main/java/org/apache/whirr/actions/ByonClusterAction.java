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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.whirr.Cluster;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandler;
import org.apache.whirr.service.jclouds.StatementBuilder;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.domain.ComputeMetadata;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.domain.Credentials;
import org.jclouds.scriptbuilder.domain.OsFamily;
import org.jclouds.scriptbuilder.domain.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class ByonClusterAction extends ScriptBasedClusterAction {

  private static final Logger LOG = LoggerFactory
      .getLogger(ByonClusterAction.class);

  private final String action;

  public ByonClusterAction(String action,
      Function<ClusterSpec, ComputeServiceContext> getCompute,
      Map<String, ClusterActionHandler> handlerMap) {
    super(getCompute, handlerMap);
    this.action = action;
  }

  @Override
  protected String getAction() {
    return action;
  }

  @Override
  protected void doAction(ClusterSpec spec, Cluster cluster,
      List<List<ClusterActionEvent>> eventsPerStage)
      throws InterruptedException, IOException {

    List<NodeMetadata> nodes = Lists.newArrayList();
    int numberAllocated = 0;
    Set<Instance> allInstances = Sets.newLinkedHashSet();
    final Credentials credentials = new Credentials(spec.getClusterUser(),
        spec.getPrivateKey());
    final ComputeService computeService = getCompute().apply(spec)
        .getComputeService();

    for (List<ClusterActionEvent> stage : eventsPerStage) {
      for (ClusterActionEvent event : stage) {
        StatementBuilder statementBuilder = event.getStatementBuilder();
        if (statementBuilder.isEmpty()) {
          continue; // skip execution if we have an empty list
        }

        if (numberAllocated == 0) {
          for (ComputeMetadata compute : computeService.listNodes()) {
            if (!(compute instanceof NodeMetadata)) {
              throw new IllegalArgumentException(
                  "Not an instance of NodeMetadata: " + compute);
            }
            nodes.add((NodeMetadata) compute);
          }
        }

        int num = event.getInstanceTemplate().getNumberOfInstances();
        final List<NodeMetadata> templateNodes = nodes.subList(numberAllocated,
            numberAllocated + num);
        numberAllocated += num;

        final Set<Instance> templateInstances = getInstances(credentials, event
            .getInstanceTemplate().getRoles(), templateNodes);
        allInstances.addAll(templateInstances);

        for (final Instance instance : templateInstances) {
          final Statement statement = statementBuilder.build(spec, instance);

          event.addEventCallable(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
              LOG.info("Running script on: {}", instance.getId());

              if (LOG.isDebugEnabled()) {
                LOG.debug("Running script:\n{}",
                    statement.render(OsFamily.UNIX));
              }

              computeService.runScriptOnNode(instance.getId(), statement);
              LOG.info("Script run completed on: {}", instance.getId());

              return null;
            }
          });
        }
      }
    }

    fireActionEventsInParallel(spec, cluster, eventsPerStage);

    for (List<ClusterActionEvent> stageEvents : eventsPerStage) {
      for (ClusterActionEvent event : stageEvents) {
        for (Future<?> future : event.getEventFutures()) {
          try {
            future.get();
          } catch (ExecutionException e) {
            throw new IOException(e.getCause());
          }
        }
      }
    }

    if (action.equals(ClusterActionHandler.BOOTSTRAP_ACTION)) {
      cluster = new Cluster(allInstances);
      for (List<ClusterActionEvent> stageEvents : eventsPerStage) {
        for (ClusterActionEvent event : stageEvents) {
          event.setCluster(cluster);
        }
      }
    }
  }

  private Set<Instance> getInstances(final Credentials credentials,
      final Set<String> roles, Collection<NodeMetadata> nodes) {
    return Sets.newLinkedHashSet(Collections2.transform(
        Sets.newLinkedHashSet(nodes), new Function<NodeMetadata, Instance>() {
          @Override
          public Instance apply(NodeMetadata node) {
            String publicIp = Iterables.get(node.getPublicAddresses(), 0);
            return new Instance(credentials, roles, publicIp, publicIp, node
                .getId(), node);
          }
        }));
  }

}
