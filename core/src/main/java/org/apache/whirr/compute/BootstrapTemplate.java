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

package org.apache.whirr.compute;

import static org.jclouds.compute.options.TemplateOptions.Builder.runScript;

import org.apache.whirr.ClusterSpec;
import org.apache.whirr.InstanceTemplate;
import org.apache.whirr.service.jclouds.StatementBuilder;
import org.apache.whirr.service.jclouds.TemplateBuilderStrategy;
import org.jclouds.aws.ec2.AWSEC2Client;
import org.jclouds.aws.ec2.compute.AWSEC2TemplateOptions;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.domain.Template;
import org.jclouds.compute.domain.TemplateBuilder;
import org.jclouds.scriptbuilder.domain.OsFamily;
import org.jclouds.scriptbuilder.domain.Statement;
import org.jclouds.scriptbuilder.statements.login.AdminAccess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

public class BootstrapTemplate {

  private static final Logger LOG =
    LoggerFactory.getLogger(BootstrapTemplate.class);

  public static Template build(
    final ClusterSpec clusterSpec,
    ComputeService computeService,
    StatementBuilder statementBuilder,
    TemplateBuilderStrategy strategy,
    InstanceTemplate instanceTemplate
  ) {
    String name = "bootstrap-" + Joiner.on('_').join(instanceTemplate.getRoles());

    LOG.info("Configuring template for {}", name);
    
    statementBuilder.name(name);

    AdminAccess adminAccess = AdminAccess.builder()
      .adminUsername(clusterSpec.getClusterUser())
      .adminPublicKey(clusterSpec.getPublicKey())
      .adminPrivateKey(clusterSpec.getPrivateKey()).build();
    
    statementBuilder.addStatement(0, adminAccess);
    
    Statement bootstrap = statementBuilder.build(clusterSpec);
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Running script {}:\n{}", name, bootstrap.render(OsFamily.UNIX));
    }
    
    TemplateBuilder templateBuilder = computeService.templateBuilder()
      .options(runScript(bootstrap));
    strategy.configureTemplateBuilder(clusterSpec, templateBuilder, instanceTemplate);

    return setSpotInstancePriceIfSpecified(
      computeService.getContext(), clusterSpec, templateBuilder.build(), instanceTemplate
    );
  }

  /**
   * Set maximum spot instance price based on the configuration
   */
  private static Template setSpotInstancePriceIfSpecified(
      ComputeServiceContext context, ClusterSpec spec, Template template, InstanceTemplate instanceTemplate
  ) {

    if (context != null && context.getProviderSpecificContext().getApi() instanceof AWSEC2Client) {
      float spotPrice = firstPositiveOrDefault(
        0,  /* by default use regular instances */
        instanceTemplate.getAwsEc2SpotPrice(),
        spec.getAwsEc2SpotPrice()
      );
      if (spotPrice > 0) {
        template.getOptions().as(AWSEC2TemplateOptions.class).spotPrice(spotPrice);
      }
    }

    return template;
  }

  private static float firstPositiveOrDefault(float defaultValue, float... listOfValues) {
    for(float value : listOfValues) {
      if (value > 0) return value;
    }
    return defaultValue;
  }

}
