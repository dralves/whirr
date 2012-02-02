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
import java.util.Map;
import java.util.Set;

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

  private int edgeCounter = 0;

  public List<Set<String>> getStages(List<InstanceTemplate> instanceTemplates,
      Map<String, ClusterActionHandler> handlerMap) {
    Graph<String, Integer> rolesGraph = buildGraph(
        collectRolesFromTemplates(instanceTemplates), handlerMap);
    Map<String, Integer> levelMappings = Maps.newLinkedHashMap();
    List<Set<String>> stages = Lists.newArrayList();
    Set<String> roots = Sets.newLinkedHashSet();
    // find the roots, ie the roles that depend on no one
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
      stages.add(nextStage);
      if (nextStage.isEmpty()) {
        break;
      }
    }
    return stages;
  }

  private Set<String> processNextStage(Set<String> parents, int currentLevel,
      Map<String, Integer> levelMappings, Graph<String, Integer> rolesGraph) {
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

  private Set<String> collectRolesFromTemplates(
      List<InstanceTemplate> instanceTemplates) {
    // so that start orders are as deterministic as possible
    Set<String> roles = Sets.newLinkedHashSet();
    for (InstanceTemplate template : instanceTemplates) {
      roles.addAll(template.getRoles());
    }
    return roles;
  }

  private void addRoleAndParents(String role,
      Graph<String, Integer> rolesGraph,
      Map<String, ClusterActionHandler> handlerMap) {
    if (handlerMap.get(role) == null) {
      throw new ServiceNotFoundException(role);
    }
    // get this role's parents
    Set<String> parents = handlerMap.get(role).getDependedOnRoles();
    // make sure the parents are in the graph if any.
    for (String parent : parents) {
      if (!rolesGraph.containsVertex(parent)) {
        addRoleAndParents(parent, rolesGraph, handlerMap);
      }
      rolesGraph.addEdge(edgeCounter++, parent, role);
    }

  }

  private Graph<String, Integer> buildGraph(Set<String> templateRoles,
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
