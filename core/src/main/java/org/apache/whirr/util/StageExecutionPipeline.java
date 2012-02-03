package org.apache.whirr.util;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;

import org.apache.whirr.InstanceTemplate;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;

public class StageExecutionPipeline {

  private final ExecutorService executors;
  private final ConcurrentLinkedQueue<String> stageQueue;

  public StageExecutionPipeline(ExecutorService executors) {
    this.executors = executors;
    this.stageQueue = new ConcurrentLinkedQueue<String>();
  }

  /**
   * Executes the provided {@link Callable} respecting dependency execution
   * order.
   * 
   * @param callablesByRole
   * @return
   */
  public synchronized <T> Map<String, ListenableFuture<T>> execute(
      Map<String, Callable<T>> callablesByRole) {

    // build the stages based on the dependencies
    List<Set<String>> stages;

    return null;
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
}
