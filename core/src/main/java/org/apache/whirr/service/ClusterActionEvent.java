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

package org.apache.whirr.service;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.InstanceTemplate;
import org.apache.whirr.service.jclouds.StatementBuilder;
import org.jclouds.compute.ComputeServiceContext;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

/**
 * An event object which is fired when a {@link org.apache.whirr.ClusterAction}
 * occurs.
 */
public class ClusterActionEvent {

  private String action;
  private ClusterSpec clusterSpec;
  private InstanceTemplate instanceTemplate;
  private Cluster cluster;
  private StatementBuilder statementBuilder;
  private FirewallManager firewallManager;
  private Function<ClusterSpec, ComputeServiceContext> getCompute;
  private List<Callable<?>> eventCallables;
  private List<Future<?>> eventFutures;
  private long executionWaitTime;

  public ClusterActionEvent(String action, ClusterSpec clusterSpec,
      InstanceTemplate instanceTemplate, Cluster cluster,
      Function<ClusterSpec, ComputeServiceContext> getCompute,
      FirewallManager firewallManager) {
    this(action, clusterSpec, instanceTemplate, cluster, null, getCompute,
        firewallManager);
  }

  public ClusterActionEvent(String action, ClusterSpec clusterSpec,
      InstanceTemplate instanceTemplate, Cluster cluster,
      StatementBuilder statementBuilder,
      Function<ClusterSpec, ComputeServiceContext> getCompute,
      FirewallManager firewallManager) {
    this.action = action;
    this.clusterSpec = clusterSpec;
    this.instanceTemplate = instanceTemplate;
    this.cluster = cluster;
    this.statementBuilder = statementBuilder;
    this.getCompute = getCompute;
    this.firewallManager = firewallManager;
    this.eventCallables = Lists.newArrayList();
    this.eventFutures = Lists.newArrayList();
  }

  public Cluster getCluster() {
    return cluster;
  }

  public void setCluster(Cluster cluster) {
    this.cluster = cluster;
  }

  public String getAction() {
    return action;
  }

  public ClusterSpec getClusterSpec() {
    return clusterSpec;
  }

  public InstanceTemplate getInstanceTemplate() {
    return instanceTemplate;
  }

  public Function<ClusterSpec, ComputeServiceContext> getCompute() {
    return getCompute;
  }

  public StatementBuilder getStatementBuilder() {
    return statementBuilder;
  }

  public FirewallManager getFirewallManager() {
    return firewallManager;
  }

  public void addEventCallable(Callable<?> eventCallable) {
    this.eventCallables.add(eventCallable);
  }

  public void addEventFuture(Future<?> eventFuture) {
    this.eventFutures.add(eventFuture);
  }

  public List<Callable<?>> getEventCallables() {
    return eventCallables;
  }

  public List<Future<?>> getEventFutures() {
    return eventFutures;
  }

  public void setExecutionWaitTime(long executionWaitTime) {
    this.executionWaitTime = executionWaitTime;
  }

  public long getExecutionWaitTime() {
    return executionWaitTime;
  }

}
