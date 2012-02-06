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

package org.apache.whirr;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;

import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandler;
import org.apache.whirr.service.DependencyAnalyzer;
import org.apache.whirr.service.FirewallManager;
import org.apache.whirr.service.jclouds.StatementBuilder;
import org.jclouds.compute.ComputeServiceContext;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.ComputationException;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Performs an action on a cluster. Example actions include bootstrapping
 * (launching, creating), configuring, or running an arbitrary command on the
 * cluster.
 */
public abstract class ClusterAction {

  private final Function<ClusterSpec, ComputeServiceContext> getCompute;
  protected final Map<String, ClusterActionHandler> handlerMap;
  protected final ImmutableSet<String> targetInstanceIds;
  protected final ImmutableSet<String> targetRoles;
  private static final AtomicInteger EVENT_THREAD_COUNTER = new AtomicInteger();
  protected static final ExecutorService EXECUTORS = Executors
      .newCachedThreadPool(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
          Thread thread = new Thread(r, "ClusterEventThread["
              + EVENT_THREAD_COUNTER.getAndIncrement() + "]");
          thread.setDaemon(true);
          return thread;
        }
      });

  static {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        EXECUTORS.shutdown();
      }
    });
  }

  protected ClusterAction(
      final Function<ClusterSpec, ComputeServiceContext> getCompute) {
    this.getCompute = getCompute;
    this.handlerMap = HandlerMapFactory.create();
    this.targetInstanceIds = null;
    this.targetRoles = null;
  }

  protected ClusterAction(
      Function<ClusterSpec, ComputeServiceContext> getCompute,
      Map<String, ClusterActionHandler> handlerMap) {
    this(getCompute, handlerMap, ImmutableSet.<String> of(), ImmutableSet
        .<String> of());
  }

  protected ClusterAction(
      Function<ClusterSpec, ComputeServiceContext> getCompute,
      Map<String, ClusterActionHandler> handlerMap, Set<String> targetRoles,
      Set<String> targetInstanceIds) {
    this.getCompute = getCompute;
    this.handlerMap = checkNotNull(handlerMap, "handlerMap");
    this.targetRoles = ImmutableSet.copyOf(checkNotNull(targetRoles,
        "targetRoles"));
    this.targetInstanceIds = ImmutableSet.copyOf(checkNotNull(
        targetInstanceIds, "targetInstanceIds"));
  }

  protected Function<ClusterSpec, ComputeServiceContext> getCompute() {
    return getCompute;
  }

  protected abstract String getAction();

  protected void doAction(ClusterSpec spec, Cluster cluster,
      List<List<ClusterActionEvent>> eventsPerStage)
      throws InterruptedException, IOException {
  }

  public Cluster execute(ClusterSpec clusterSpec, Cluster cluster)
      throws IOException, InterruptedException {

    Cluster newCluster = cluster;

    // In order to segregate dependency execution we need to have a separate
    // ClusterActionEvent not only per instance template but also per role.
    List<Set<String>> stages = DependencyAnalyzer.buildStages(clusterSpec,
        handlerMap);

    List<List<ClusterActionEvent>> eventsPerStage = Lists.newArrayList();

    for (Set<String> stageRoles : stages) {

      // find the templates with roles in this stage
      List<InstanceTemplate> stageTemplates = findTemplatesWithRoles(
          clusterSpec.getInstanceTemplates(), stageRoles);

      List<ClusterActionEvent> stageEvents = Lists.newArrayList();

      // now not all roles of these templates might be executed in this stage so
      // we must select which roles we want within the template.
      for (InstanceTemplate stageTemplate : stageTemplates) {
        if (shouldIgnoreInstanceTemplate(stageTemplate)) {
          continue; // skip execution if this group of instances is not in
                    // target
        }

        StatementBuilder statementBuilder = new StatementBuilder();
        ComputeServiceContext computeServiceContext = getCompute().apply(
            clusterSpec);
        FirewallManager firewallManager = new FirewallManager(
            computeServiceContext, clusterSpec, newCluster);

        ClusterActionEvent event = new ClusterActionEvent(getAction(),
            clusterSpec, stageTemplate, newCluster, statementBuilder,
            getCompute(), firewallManager);

        Set<String> stateAndTemplateRoles = Sets.intersection(stageRoles,
            stageTemplate.getRoles());

        long eventExecutionWaitTime = 0L;
        for (String role : stateAndTemplateRoles) {
          if (roleIsInTarget(role)) {
            safeGetActionHandler(role).beforeAction(event);
            long handlerExecutionWaitTime = safeGetActionHandler(role)
                .getOnlineDelayMillis();
            if (handlerExecutionWaitTime > eventExecutionWaitTime) {
              eventExecutionWaitTime = handlerExecutionWaitTime;
            }
          }
        }

        event.setExecutionWaitTime(eventExecutionWaitTime);
        stageEvents.add(event);

        // cluster may have been updated by handler
        newCluster = event.getCluster();
      }

      eventsPerStage.add(stageEvents);
    }

    doAction(clusterSpec, newCluster, eventsPerStage);

    // cluster may have been updated by action
    newCluster = eventsPerStage.get(0).get(0).getCluster();

    for (List<ClusterActionEvent> stageEvents : eventsPerStage) {
      for (ClusterActionEvent event : stageEvents) {
        if (shouldIgnoreInstanceTemplate(event.getInstanceTemplate())) {
          continue;
        }
        for (String role : event.getInstanceTemplate().getRoles()) {
          if (roleIsInTarget(role)) {
            event.setCluster(newCluster);
            safeGetActionHandler(role).afterAction(event);

            // cluster may have been updated by handler
            newCluster = event.getCluster();
          }
        }
      }
    }

    return newCluster;
  }

  /**
   * Fires all event actions in parallel, respecting stages.
   * 
   * @param spec
   * @param cluster
   * @param eventsPerStage
   */

  protected <T> void fireActionEventsInParallel(ClusterSpec spec,
      Cluster cluster, List<List<ClusterActionEvent>> eventsPerStage)
      throws InterruptedException {
    fireActionEventsInParallel(spec, cluster, eventsPerStage, EXECUTORS);
  }

  /**
   * Fires all event actions in parallel, respecting stages.
   * 
   * @param spec
   * @param cluster
   * @param eventsPerStage
   */
  @SuppressWarnings("unchecked")
  protected <T> void fireActionEventsInParallel(ClusterSpec spec,
      Cluster cluster, List<List<ClusterActionEvent>> eventsPerStage,
      ExecutorService executors) throws InterruptedException {

    long executionWaitTime = 0L;
    for (List<ClusterActionEvent> stageEvents : eventsPerStage) {
      Collection<Callable<T>> callables = Lists.newArrayList();
      for (ClusterActionEvent event : stageEvents) {
        if (event.getExecutionWaitTime() > executionWaitTime) {
          executionWaitTime = event.getExecutionWaitTime();
        }
        for (@SuppressWarnings("rawtypes")
        Callable callable : event.getEventCallables()) {
          callables.add(callable);
        }
      }

      List<Future<T>> futures = executors.invokeAll(callables);
      int futureCounter = 0;
      for (ClusterActionEvent event : stageEvents) {
        for (int i = 0; i < event.getEventCallables().size(); i++) {
          event.addEventFuture(futures.get(futureCounter++));
        }
      }
      Thread.sleep(executionWaitTime);
    }

  }

  protected boolean shouldIgnoreInstanceTemplate(InstanceTemplate template) {
    return targetRoles.size() != 0
        && containsNoneOf(template.getRoles(), targetRoles);
  }

  protected boolean roleIsInTarget(String role) {
    return targetRoles.size() == 0 || targetRoles.contains(role);
  }

  /**
   * Try to get an {@see ClusterActionHandler } instance or throw an
   * IllegalArgumentException if not found for this role name
   */
  protected ClusterActionHandler safeGetActionHandler(String role) {
    try {
      ClusterActionHandler handler = handlerMap.get(role);
      if (handler == null) {
        throw new IllegalArgumentException("No handler for role " + role);
      }
      return handler;

    } catch (ComputationException e) {
      throw new IllegalArgumentException(e.getCause());
    }
  }

  private boolean containsNoneOf(Set<String> querySet, final Set<String> target) {
    return !Iterables.any(querySet, new Predicate<String>() {
      @Override
      public boolean apply(@Nullable String role) {
        return target.contains(role);
      }
    });
  }

  protected List<InstanceTemplate> findTemplatesWithRoles(
      Collection<InstanceTemplate> templates, Set<String> roles) {
    List<InstanceTemplate> templatesWithRoles = Lists.newArrayList();
    for (InstanceTemplate template : templates) {
      if (!Sets.intersection(template.getRoles(), roles).isEmpty()) {
        templatesWithRoles.add(template);
      }
    }
    return templatesWithRoles;
  }

  protected String asString(Set<Instance> instances) {
    return Joiner.on(", ").join(
        Iterables.transform(instances, new Function<Instance, String>() {
          @Override
          public String apply(@Nullable Instance instance) {
            return instance == null ? "<null>" : instance.getId();
          }
        }));
  }

  protected boolean instanceIsNotInTarget(Instance instance) {
    if (targetInstanceIds.size() != 0) {
      return !targetInstanceIds.contains(instance.getId());
    }
    if (targetRoles.size() != 0) {
      return containsNoneOf(instance.getRoles(), targetRoles);
    }
    return false;
  }

}
