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

import static org.apache.whirr.RolePredicates.onlyRolesIn;
import static org.jclouds.compute.options.RunScriptOptions.Builder.overrideLoginCredentials;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.whirr.Cluster;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterAction;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandler;
import org.apache.whirr.service.jclouds.StatementBuilder;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.domain.ExecResponse;
import org.jclouds.domain.Credentials;
import org.jclouds.domain.LoginCredentials;
import org.jclouds.scriptbuilder.domain.OsFamily;
import org.jclouds.scriptbuilder.domain.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;

/**
 * A {@link ClusterAction} that provides the base functionality for running
 * scripts on instances in the cluster.
 */
public abstract class ScriptBasedClusterAction extends ClusterAction {

  private static final Logger LOG = LoggerFactory
      .getLogger(ScriptBasedClusterAction.class);

  protected ScriptBasedClusterAction(
      Function<ClusterSpec, ComputeServiceContext> getCompute,
      Map<String, ClusterActionHandler> handlerMap) {
    super(getCompute, handlerMap, ImmutableSet.<String> of(), ImmutableSet
        .<String> of());
  }

  protected ScriptBasedClusterAction(
      Function<ClusterSpec, ComputeServiceContext> getCompute,
      Map<String, ClusterActionHandler> handlerMap, Set<String> targetRoles,
      Set<String> targetInstanceIds) {
    super(getCompute, handlerMap, targetRoles, targetInstanceIds);
  }

  protected void doAction(ClusterSpec spec, Cluster cluster,
      List<List<ClusterActionEvent>> eventsPerStage)
      throws InterruptedException, IOException {

    final String phaseName = getAction();
    final Credentials credentials = new Credentials(spec.getClusterUser(),
        spec.getPrivateKey());
    final ComputeService computeService = getCompute().apply(spec)
        .getComputeService();

    for (List<ClusterActionEvent> stage : eventsPerStage) {
      for (ClusterActionEvent event : stage) {
        eventSpecificActions(event);
        StatementBuilder statementBuilder = event.getStatementBuilder();
        if (statementBuilder.isEmpty()) {
          continue; // skip execution if we have an empty list
        }
        Set<Instance> instances = cluster
            .getInstancesMatching(onlyRolesIn(event.getInstanceTemplate()
                .getRoles()));
        LOG.info("Starting to run scripts on cluster for phase {} "
            + "on instances: {}", phaseName, asString(instances));
        for (final Instance instance : instances) {
          if (instanceIsNotInTarget(instance)) {
            continue; // skip the script execution
          }
          final Statement statement = statementBuilder.build(spec, instance);
          event.addEventCallable(new Callable<ExecResponse>() {
            @Override
            public ExecResponse call() {

              LOG.info("Running {} phase script on: {}", phaseName,
                  instance.getId());
              if (LOG.isDebugEnabled()) {
                LOG.debug(
                    "{} phase script on: {}\n{}",
                    new Object[] { phaseName, instance.getId(),
                        statement.render(OsFamily.UNIX) });
              }

              try {
                System.out.println(statement.render(OsFamily.UNIX));
                return computeService.runScriptOnNode(
                    instance.getId(),
                    statement,
                    overrideLoginCredentials(
                        LoginCredentials.fromCredentials(credentials))
                        .runAsRoot(true).nameTask(
                            phaseName + "-"
                                + Joiner.on('_').join(instance.getRoles())));
              } finally {
                LOG.info("{} phase script run completed on: {}", phaseName,
                    instance.getId());
              }
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
            @SuppressWarnings("unchecked")
            ExecResponse execResponse = ((Future<ExecResponse>) future).get();
            if (execResponse.getExitCode() != 0) {
              LOG.error("Error running " + phaseName + " script: {}",
                  execResponse);
            } else {
              LOG.info("Successfully executed {} script: {}", phaseName,
                  execResponse);
            }
          } catch (ExecutionException e) {
            throw new IOException(e.getCause());
          }
        }
      }
    }

    LOG.info("Finished running {} phase scripts on all cluster instances",
        phaseName);

    postRunScriptsActions(eventsPerStage);
  }

  protected void eventSpecificActions(ClusterActionEvent entry)
      throws IOException {
  }

  protected void postRunScriptsActions(
      List<List<ClusterActionEvent>> eventsPerStage) throws IOException {
  }

}
