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

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import org.apache.commons.io.IOUtils;
import org.apache.whirr.Cluster;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterSpec;
import org.jclouds.aws.util.AWSUtils;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.ec2.EC2ApiMetadata;
import org.jclouds.ec2.EC2Client;
import org.jclouds.ec2.domain.IpProtocol;
import org.jclouds.googlecomputeengine.GoogleComputeEngineApi;
import org.jclouds.googlecomputeengine.GoogleComputeEngineApiMetadata;
import org.jclouds.googlecomputeengine.config.UserProject;
import org.jclouds.googlecomputeengine.domain.Firewall;
import org.jclouds.googlecomputeengine.domain.Network;
import org.jclouds.googlecomputeengine.domain.Operation;
import org.jclouds.googlecomputeengine.features.FirewallApi;
import org.jclouds.googlecomputeengine.options.FirewallOptions;
import org.jclouds.javax.annotation.Nullable;
import org.jclouds.openstack.nova.v2_0.NovaApiMetadata;
import org.jclouds.openstack.nova.v2_0.domain.Ingress;
import org.jclouds.openstack.nova.v2_0.domain.SecurityGroup;
import org.jclouds.openstack.nova.v2_0.extensions.SecurityGroupApi;
import org.jclouds.scriptbuilder.domain.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Sets.union;
import static org.jclouds.scriptbuilder.domain.Statements.exec;
import static org.jclouds.util.Predicates2.retry;

public class FirewallManager {

  public static class StoredRule {
    private Rule rule;
    private List<String> cidrs;
    private Set<Instance> instances;
      
    public StoredRule(Rule rule, List<String> cidrs, Set<Instance> instances) {
      this.rule = rule;
      this.cidrs = cidrs;
      this.instances = instances;
    }

    /**
     * Get the Rule object for this stored rule.
     */
    public Rule rule() {
      return rule;
    }

    /**
     * Get the CIDRs for this stored rule.
     */
    public List<String> cidrs() {
      return cidrs;
    }

    /**
     * Get the set of Instances for this stored rule.
     */
    public Set<Instance> instances() {
      return instances;
    }
  }
    
  public static class Rule {

    public static Rule create() {
      return new Rule();
    }

    private String source;
    private Set<Instance> destinations;
    private Predicate<Instance> destinationPredicate;
    private int[] ports;

    private Rule() {
    }

    /**
     * @param source The allowed source IP for traffic. If not set, this will
     * default to {@link ClusterSpec#getClientCidrs()}, or, if that is not set,
     *  to the client's originating IP.
     */
    public Rule source(String source) {
      this.source = source;
      return this;
    }

    /**
     * @param destination The allowed destination instance.
     */
    public Rule destination(Instance destination) {
      this.destinations = Collections.singleton(destination);
      return this;
    }

    /**
     * @param destinations The allowed destination instances.
     */
    public Rule destination(Set<Instance> destinations) {
      this.destinations = destinations;
      return this;
    }

    /**
     * @param destinationPredicate A predicate which is used to evaluate the
     * allowed destination instances.
     */
    public Rule destination(Predicate<Instance> destinationPredicate) {
      this.destinationPredicate = destinationPredicate;
      return this;
    }

    /**
     * @param port The port on the destination which is to be opened. Overrides
     * any previous calls to {@link #port(int)} or {@link #ports(int...)}.
     */
    public Rule port(int port) {
      this.ports = new int[] { port };
      return this;
    }

    /**
     * @param ports The ports on the destination which are to be opened.
     * Overrides
     * any previous calls to {@link #port(int)} or {@link #ports(int...)}.
     */
    public Rule ports(int... ports) {
      this.ports = ports;
      return this;
    }
  }

  private static final Logger LOG = LoggerFactory
    .getLogger(FirewallManager.class);

  private ComputeServiceContext computeServiceContext;
  private ClusterSpec clusterSpec;
  private Cluster cluster;
  private Set<StoredRule> storedRules;
    
  public FirewallManager(ComputeServiceContext computeServiceContext,
                         ClusterSpec clusterSpec, Cluster cluster) {
    this.computeServiceContext = computeServiceContext;
    this.clusterSpec = clusterSpec;
    this.cluster = cluster;
    this.storedRules = Sets.newHashSet();
  }

  public void addRules(Rule... rules) throws IOException {
    for (Rule rule : rules) {
      addRule(rule);
    }
  }

  public void addRules(Set<Rule> rules) throws IOException {
    for (Rule rule : rules) {
      addRule(rule);
    }
  }

  /**
   * Rules are additive. If no source is set then it will default
   * to {@link ClusterSpec#getClientCidrs()}, or, if that is not set,
   * to the client's originating IP. If no destinations or ports
   * are set then the rule has not effect.
   *
   * @param rule The rule to add to the firewall.
   * @throws IOException
   */
  public void addRule(Rule rule) throws IOException {
    Set<Instance> instances = Sets.newHashSet();
    if (rule.destinations != null) {
      instances.addAll(rule.destinations);
    }
    if (rule.destinationPredicate != null) {
      instances.addAll(cluster.getInstancesMatching(rule.destinationPredicate));
    }
    List<String> cidrs;
    if (rule.source == null) {
      cidrs = clusterSpec.getClientCidrs();
      if (cidrs == null || cidrs.isEmpty()) {
        cidrs = Lists.newArrayList(getOriginatingIp());
      }
    } else {
      cidrs = Lists.newArrayList(rule.source + "/32");
    }

    storedRules.add(new StoredRule(rule, cidrs, instances));
  }

  /**
   * Logs information about the StoredRule we're adding
   * @param storedRule the StoredRule we're adding
   */
  private void logInstanceRules(StoredRule storedRule) {
    Iterable<String> instanceIds =
      Iterables.transform(storedRule.instances(), new Function<Instance, String>() {
          @Override
          public String apply(@Nullable Instance instance) {
            return instance == null ? "<null>" : instance.getId();
          }
        });
      
      
      
    LOG.info("Authorizing firewall ingress to {} on ports {} for {}",
             new Object[] { instanceIds, storedRule.rule().ports, storedRule.cidrs() });
  }

  /**
   * Authorizes all rules via jclouds security groups interface.
   */
  public void authorizeAllRules() {
    authorizeIngress();
  }

  /**
   * Returns a list of Statements for executing iptables for the stored rules.
   * @return List of iptables Statements.
   */
  public List<Statement> getRulesAsStatements() {
    // in GCE there's no need to actually set the per-instance iptables rules.
    if (GoogleComputeEngineApiMetadata.CONTEXT_TOKEN.isAssignableFrom(computeServiceContext.getBackendType())) {
      return ImmutableList.<Statement>of();
    }

    List<Statement> ruleStatements = Lists.newArrayList();

    for (StoredRule storedRule : storedRules) {
      logInstanceRules(storedRule);
      for (String cidr : storedRule.cidrs()) {
        for (int port : storedRule.rule().ports) {
          ruleStatements.add(exec(String.format("iptables -I INPUT 1 -p tcp --dport %d --source %s -j ACCEPT || true",
                                                port, cidr)));
        }
      }
    }

    ruleStatements.add(exec("iptables-save || true"));

    return ruleStatements;
  }

  /**
   * @return the IP address of the client on which this code is running.
   * @throws IOException
   */
  private String getOriginatingIp() throws IOException {
    if ("stub".equals(clusterSpec.getProvider())) {
      return "62.217.232.123";
    }

    URL url = new URL("http://checkip.amazonaws.com/");
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.connect();
    return IOUtils.toString(connection.getInputStream()).trim() + "/32";
  }

  public void authorizeIngress() {

    // in order to keep changes minimal EC2 and Rackspace do one-by-one rule setting
    if (EC2ApiMetadata.CONTEXT_TOKEN.isAssignableFrom(computeServiceContext.getBackendType())) {
      // This code (or something like it) may be added to jclouds (see
      // http://code.google.com/p/jclouds/issues/detail?id=336).
      // Until then we need this temporary workaround.
      for (StoredRule rule : storedRules) {
        logInstanceRules(rule);
        String region = AWSUtils.parseHandle(Iterables.get(rule.instances(), 0).getId())[0];
        EC2Client ec2Client = computeServiceContext.unwrap(EC2ApiMetadata.CONTEXT_TOKEN).getApi();
        String groupName = "jclouds#" + clusterSpec.getClusterName();
        for (String cidr : rule.cidrs()) {
          for (int port : rule.rule().ports) {
            try {
              ec2Client.getSecurityGroupServices()
                .authorizeSecurityGroupIngressInRegion(region, groupName,
                  IpProtocol.TCP, port, port, cidr);
            } catch (IllegalStateException e) {
              LOG.warn(e.getMessage());
            /* ignore, it means that this permission was already granted */
            }
          }
        }
      }
    } else if (NovaApiMetadata.CONTEXT_TOKEN.isAssignableFrom(computeServiceContext.getBackendType())) {
      // This code (or something like it) may be added to jclouds (see
      // http://code.google.com/p/jclouds/issues/detail?id=336).
      // Until then we need this temporary workaround.
      Optional<? extends SecurityGroupApi> securityGroupApi = computeServiceContext.unwrap(NovaApiMetadata.CONTEXT_TOKEN)
        .getApi()
        .getSecurityGroupExtensionForZone(clusterSpec.getTemplate().getLocationId());

      if (securityGroupApi.isPresent()) {
        for (StoredRule rule : storedRules) {
          logInstanceRules(rule);
          final String groupName = "jclouds-" + clusterSpec.getClusterName();
          Optional<? extends SecurityGroup> group = securityGroupApi.get().list().firstMatch(new Predicate<SecurityGroup>() {
            @Override
            public boolean apply(SecurityGroup secGrp) {
              return secGrp.getName().equals(groupName);
            }
          });

        if (group.isPresent()) {
          for (String cidr : rule.cidrs()) {
            for (int port : rule.rule().ports) {
              try {
                securityGroupApi.get().createRuleAllowingCidrBlock(group.get().getId(),
                                                                   Ingress.builder()
                                                                   .ipProtocol(org.jclouds.openstack.nova.v2_0.domain.IpProtocol.TCP)
                                                                   .fromPort(port).toPort(port).build(),
                                                                   cidr);

              } catch(IllegalStateException e) {
                LOG.warn(e.getMessage());
                /* ignore, it means that this permission was already granted */
              }
            }
          }

        } else {
          LOG.warn("Expected security group " + groupName + " does not exist.");
        }
        }
      } else {
        LOG.warn("OpenStack security group extension not available for this cloud.");
      }
    } else if (GoogleComputeEngineApiMetadata.CONTEXT_TOKEN.isAssignableFrom(computeServiceContext.getBackendType())) {
      // In GCE consolidate rules to minimize api calls

      GoogleComputeEngineApi api = computeServiceContext.unwrap(GoogleComputeEngineApiMetadata.CONTEXT_TOKEN).getApi();
      Injector injector = computeServiceContext.utils().injector();
      String userProject = injector
        .getInstance(Key.get(new TypeLiteral<Supplier<String>>() {}, UserProject.class)).get();

      Network clusterNetwork = api.getNetworkApiForProject(userProject).get("jclouds-" + clusterSpec.getClusterName());
      checkNotNull(clusterNetwork, "the cluster has no network");

      FirewallApi firewallApi = api.getFirewallApiForProject(userProject);

      // create a firewall for internal communication (as gce does not do that by default)
      FirewallOptions internalAccess = new FirewallOptions()
        .name("jclouds-" + clusterSpec.getClusterName() + "-internal")
        .network(clusterNetwork.getSelfLink())
        .addSourceRange(clusterNetwork.getIPv4Range())
        .addAllowedRule(Firewall.Rule.builder()
          .IPProtocol(Firewall.Rule.IPProtocol.TCP)
          .addPortRange(1, 65535)
          .build())
        .addAllowedRule(Firewall.Rule.builder()
          .IPProtocol(Firewall.Rule.IPProtocol.UDP)
          .addPortRange(1, 65535)
          .build())
        .addAllowedRule(Firewall.Rule.builder()
          .IPProtocol(Firewall.Rule.IPProtocol.ICMP)
          .build());

      Firewall internalAccessExisting = firewallApi.get(internalAccess.getName());
      if (internalAccessExisting != null) {
        if (!same(internalAccess, internalAccessExisting)) {
          waitOperationComplete(injector, firewallApi.update(internalAccessExisting.getName(), internalAccess));
        }
      } else {
        waitOperationComplete(injector, firewallApi
          .createInNetwork(internalAccess.getName(), internalAccess.getNetwork(), internalAccess));
      }

      // create a firewall for external ssh independently of source
      FirewallOptions externalAccess = new FirewallOptions()
        .name("jclouds-" + clusterSpec.getClusterName() + "-external")
        .network(clusterNetwork.getSelfLink())
        .addSourceRange("0.0.0.0/0")
        .addAllowedRule(Firewall.Rule.builder()
          .IPProtocol(Firewall.Rule.IPProtocol.TCP)
          .addPort(22)
          .build())
        .addAllowedRule(Firewall.Rule.builder()
          .IPProtocol(Firewall.Rule.IPProtocol.ICMP)
          .build());

      Firewall externalAccessExisting = firewallApi.get(externalAccess.getName());
      if (externalAccessExisting != null) {
        if (!same(externalAccess, externalAccessExisting)) {
          waitOperationComplete(injector, firewallApi.update(externalAccessExisting.getName(), externalAccess));
        }
      } else {
        waitOperationComplete(injector, firewallApi
          .createInNetwork(externalAccess.getName(), externalAccess.getNetwork(), externalAccess));
      }

      // GCE disregards destinations, only sources are important so we group by those and merge the cidrs and ports
      // and save as a single rule
      ImmutableSet.Builder<String> cidrs = ImmutableSet.builder();
      ImmutableSet.Builder<Integer> ports = ImmutableSet.builder();
      for (StoredRule rule : storedRules) {
        cidrs.addAll(rule.cidrs());
        for (int port : rule.rule().ports) {
          ports.add(port);
        }
      }

      // if there is nothing additional to allow return
      if (cidrs.build().isEmpty() || ports.build().isEmpty())
        return;

      // update the default firewall with the provided cidr and ports for external access
      FirewallOptions additionalAccess = new FirewallOptions()
        .name("jclouds-" + clusterSpec.getClusterName() + "-additional")
        .network(clusterNetwork.getSelfLink());

      for (Integer port : ports.build()) {
        additionalAccess
          .addAllowedRule(Firewall.Rule.builder().IPProtocol(Firewall.Rule.IPProtocol.TCP).addPort(port).build());
      }
      for (String cidr : cidrs.build()) {
        additionalAccess.addSourceRange(cidr);
      }

      Firewall additionalAccessExisting = firewallApi.get(additionalAccess.getName());
      if (additionalAccessExisting != null) {
        if (!same(additionalAccess, additionalAccessExisting)) {
          // if there's already an existing firewall merge the source ranges and the allowed rules
          additionalAccess.sourceRanges(union(additionalAccess.getSourceRanges(), additionalAccessExisting.getSourceRanges()));
          additionalAccess.allowedRules(union(additionalAccess.getAllowed(), additionalAccessExisting.getAllowed()));
          waitOperationComplete(injector, firewallApi.update(additionalAccessExisting.getName(), additionalAccess));
        }
      } else {
        waitOperationComplete(injector, firewallApi
          .createInNetwork(additionalAccess.getName(), additionalAccess.getNetwork(), additionalAccess));
      }
    }
  }

  private static boolean same(FirewallOptions options, Firewall firewall) {
    return options.getSourceRanges().equals(firewall.getSourceRanges())
      && options.getAllowed().equals(firewall.getAllowed())
      && options.getName().equals(firewall.getName())
      && options.getNetwork().equals(firewall.getNetwork())
      && options.getSourceTags().equals(firewall.getSourceTags())
      && options.getTargetTags().equals(firewall.getTargetTags());
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
