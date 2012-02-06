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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.InstanceTemplate;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import edu.uci.ics.jung.graph.DirectedSparseGraph;
import edu.uci.ics.jung.graph.Graph;

/**
 * Analyzes the dependencies of roles and creates a multistage graph. Each phase
 * in the graph may be executed in parallel.
 * 
 */
public class DependencyAnalyzer {

  public static final String DEPENDENCY_PROPERTY = "whirr.role-dependency.";

  private static int edgeCounter;

  public static List<Set<String>> buildStages(ClusterSpec spec,
      Map<String, ClusterActionHandler> handlerMap) {
    edgeCounter = 0;
    // process dependency overrides from configuration and set them in the
    // handlers before proceeding to building the graph
    processDependencyOverrides(spec, handlerMap);
    Graph<String, Integer> rolesGraph = buildGraph(
        collectRolesFromTemplates(spec.getInstanceTemplates()), handlerMap);
    Map<String, Integer> levelMappings = Maps.newLinkedHashMap();
    List<Set<String>> stages = Lists.newArrayList();
    Set<String> roots = Sets.newLinkedHashSet();
    // find the roots, i.e. the roles that depend on no one
    for (String role : rolesGraph.getVertices()) {
      if (rolesGraph.inDegree(role) == 0) {
        levelMappings.put(role, 0);
        roots.add(role);
      }
    }

    stages.add(roots);
    Set<String> nextStage = roots;
    int levelCounter = 0;
    while (true) {
      nextStage = processNextStage(nextStage, levelCounter++, levelMappings,
          rolesGraph);
      if (nextStage.isEmpty()) {
        break;
      }
      stages.add(nextStage);
    }
    return stages;
  }

  private static void processDependencyOverrides(ClusterSpec spec,
      Map<String, ClusterActionHandler> handlerMap) {
    Configuration config = spec
        .getConfigurationForKeysWithPrefix(DEPENDENCY_PROPERTY);
    for (Iterator<?> iter = config.getKeys(); iter.hasNext();) {
      String key = (String) iter.next();
      String role = key.split(".")[2];
      if (handlerMap.containsKey(role)) {
        handlerMap.get(key).getRequiredRoles().clear();
        handlerMap.get(key).getRequiredRoles()
            .addAll(processOverridenRoles(config.getString(key)));
      }
    }
  }

  private static Set<String> processOverridenRoles(String roles) {
    String[] splitRoles = roles.split(",");
    Set<String> finalRoles = Sets.newHashSet();
    for (String splitRole : splitRoles) {
      finalRoles.add(splitRole.trim());
    }
    return finalRoles;
  }

  private static Set<String> processNextStage(Set<String> parents,
      int currentLevel, Map<String, Integer> levelMappings,
      Graph<String, Integer> rolesGraph) {
    Set<String> children = Sets.newLinkedHashSet();
    for (String parent : parents) {
      children.addAll(rolesGraph.getSuccessors(parent));
    }
    for (String child : children) {
      if (levelMappings.containsKey(child)) {
        throw new IllegalStateException();
      }
      levelMappings.put(child, currentLevel);
    }
    return children;
  }

  private static Set<String> collectRolesFromTemplates(
      Collection<InstanceTemplate> instanceTemplates) {
    // so that start orders are as deterministic as possible
    Set<String> roles = Sets.newLinkedHashSet();
    for (InstanceTemplate template : instanceTemplates) {
      roles.addAll(template.getRoles());
    }
    return roles;
  }

  private static void addRoleAndParents(String role,
      Graph<String, Integer> rolesGraph,
      Map<String, ClusterActionHandler> handlerMap) {
    if (handlerMap.get(role) == null) {
      throw new ServiceNotFoundException(role);
    }
    rolesGraph.addVertex(role);
    // get this role's parents
    Set<String> parents = handlerMap.get(role).getRequiredRoles();
    // make sure the parents are in the graph if any.
    for (String parent : parents) {
      if (!rolesGraph.containsVertex(parent)) {
        addRoleAndParents(parent, rolesGraph, handlerMap);
      }
      rolesGraph.addEdge(edgeCounter++, parent, role);
    }
  }

  private static Graph<String, Integer> buildGraph(Set<String> templateRoles,
      Map<String, ClusterActionHandler> handlerMap) {
    Graph<String, Integer> rolesGraph = new DirectedSparseGraph<String, Integer>();
    for (String role : templateRoles) {
      if (!rolesGraph.containsVertex(role)) {
        addRoleAndParents(role, rolesGraph, handlerMap);
      }
    }
    return rolesGraph;
  }

}
