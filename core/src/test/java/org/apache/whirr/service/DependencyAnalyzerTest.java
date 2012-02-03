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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.HandlerMapFactory;
import org.junit.Test;

import com.google.common.collect.Sets;
import com.jcraft.jsch.JSchException;

public class DependencyAnalyzerTest {

  public static class TestServiceA extends ClusterActionHandlerSupport {

    @Override
    public String getRole() {
      return "service-a";
    }

  }

  public static class TestServiceB extends ClusterActionHandlerSupport {

    @Override
    public String getRole() {
      return "service-b";
    }

    @Override
    public Set<String> getRequiredRoles() {
      return Sets.newHashSet("service-a");
    }

  }

  public static class TestServiceC extends ClusterActionHandlerSupport {

    @Override
    public String getRole() {
      return "service-c";
    }

    @Override
    public Set<String> getRequiredRoles() {
      return Sets.newHashSet("service-b");
    }

  }

  @Test
  public void testRootServiceOnly() throws IOException, ConfigurationException,
      JSchException {
    CompositeConfiguration config = new CompositeConfiguration();
    config.setProperty("whirr.provider", "stub");
    config.setProperty("whirr.cluster-name", "stub-test");
    config.setProperty("whirr.instance-templates", "1 service-a");
    config.setProperty("whirr.state-store", "memory");

    ClusterSpec clusterSpec = ClusterSpec.withTemporaryKeys(config);

    List<Set<String>> stages = DependencyAnalyzer.buildStages(
        clusterSpec.getInstanceTemplates(), HandlerMapFactory.create());

    assertEquals("Was expecting a single stage", 1, stages.size());
    assertTrue("Was expecting service-a to be in the first stage", stages
        .get(0).contains("service-a"));
  }

  @Test
  public void testLinearDependecies() throws IOException,
      ConfigurationException, JSchException {
    CompositeConfiguration config = new CompositeConfiguration();
    config.setProperty("whirr.provider", "stub");
    config.setProperty("whirr.cluster-name", "stub-test");
    config.setProperty("whirr.instance-templates",
        "1 service-b, 1 service-a, 1 service-c");
    config.setProperty("whirr.state-store", "memory");

    ClusterSpec clusterSpec = ClusterSpec.withTemporaryKeys(config);

    List<Set<String>> stages = DependencyAnalyzer.buildStages(
        clusterSpec.getInstanceTemplates(), HandlerMapFactory.create());

    assertEquals("Was expecting three stages", 3, stages.size());
    assertTrue("Was expecting service-a to be in the first stage", stages
        .get(0).contains("service-a"));
    assertTrue("Was expecting service-b to be in the second stage",
        stages.get(1).contains("service-b"));
    assertTrue("Was expecting service-c to be in the third stage", stages
        .get(2).contains("service-c"));
  }
}
