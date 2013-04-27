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

import static com.google.common.base.Preconditions.checkState;
import static org.jclouds.compute.predicates.NodePredicates.inGroup;
import static org.jclouds.util.Predicates2.retry;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.InstanceTemplate;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandler;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.googlecomputeengine.GoogleComputeEngineApi;
import org.jclouds.googlecomputeengine.GoogleComputeEngineApiMetadata;
import org.jclouds.googlecomputeengine.config.UserProject;
import org.jclouds.googlecomputeengine.domain.Operation;
import org.jclouds.googlecomputeengine.features.FirewallApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.cache.LoadingCache;

/**
 * A {@link ClusterAction} for tearing down a running cluster and freeing up all
 * its resources.
 */
public class DestroyClusterAction extends ScriptBasedClusterAction {

  private static final Logger LOG = LoggerFactory
      .getLogger(DestroyClusterAction.class);

  public DestroyClusterAction(
      final Function<ClusterSpec, ComputeServiceContext> getCompute,
      final LoadingCache<String, ClusterActionHandler> handlerMap) {
    super(getCompute, handlerMap);
  }

  @Override
  protected String getAction() {
    return ClusterActionHandler.DESTROY_ACTION;
  }

  @Override
  protected void postRunScriptsActions(
      Map<InstanceTemplate, ClusterActionEvent> eventMap) throws IOException {
    ClusterSpec clusterSpec = eventMap.values().iterator().next()
        .getClusterSpec();
    LOG.info("Destroying " + clusterSpec.getClusterName() + " cluster");
    ComputeService computeService = getCompute().apply(clusterSpec)
        .getComputeService();
    computeService.destroyNodesMatching(inGroup(clusterSpec.getClusterName()));
    LOG.info("Cluster {} destroyed", clusterSpec.getClusterName());

    // delete dangling firewalls
    try {
      if (GoogleComputeEngineApiMetadata.CONTEXT_TOKEN.isAssignableFrom(computeService.getContext().getBackendType())) {
        Injector injector = computeService.getContext().utils().injector();
        GoogleComputeEngineApi api = computeService.getContext().unwrap(GoogleComputeEngineApiMetadata.CONTEXT_TOKEN).getApi();
        String userProject = injector.getInstance(Key.get(new TypeLiteral<Supplier<String>>() {}, UserProject.class)).get();
        FirewallApi firewallApi = api.getFirewallApiForProject(userProject);
        firewallApi.delete("jclouds-" + clusterSpec.getClusterName() + "-internal");
        firewallApi.delete("jclouds-" + clusterSpec.getClusterName() + "-external");
        // wait for the last one or deleting the network (on context close) won't work
        waitOperationComplete(injector, firewallApi.delete("jclouds-" + clusterSpec.getClusterName() + "-additional"));
      }
    } catch (Exception e) {
      LOG.error("Error deleting dangling firewalls.", e);
    }
  }

  private static void waitOperationComplete(Injector injector, Operation operation){
    AtomicReference<Operation> op  = new AtomicReference<Operation>(operation);
    Predicate<AtomicReference<Operation>> operationDonePredicate = injector
      .getInstance(Key.get(new TypeLiteral<Predicate<AtomicReference<Operation>>>() {}));
    retry(operationDonePredicate, 60, 1, TimeUnit.SECONDS).apply(op);
    checkState(op.get().getStatus() == Operation.Status.DONE, "operation was not completed");
    checkState(!op.get().getHttpError().isPresent(), "operation finished with errors: "+op.get().getErrors());
  }

}
