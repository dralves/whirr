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

package org.apache.whirr.state.integration;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * Base class that loads up local test definitions, i.e. sets up itests to run
 * against jclouds-vbox when the system property: <code>whirr.itest.local</code>
 * is set to true.
 * 
 * @author dralves
 * 
 */
public class BaseRemoteOrLocalITest {

  public static final String LOCAL_TEST = "whirr.itest.local";

  protected static CompositeConfiguration config = new CompositeConfiguration();

  public static void loadRemoteOrLocalConfig() throws ConfigurationException {
    if (System.getProperty(LOCAL_TEST) != null
        && Boolean.parseBoolean(System.getProperty(LOCAL_TEST))) {
      System.setProperty("whirr.test.provider", "virtualbox");
      System.setProperty("whirr.test.identity", "admin");
      System.setProperty("whirr.test.credential", "12345");
      config.addConfiguration(new PropertiesConfiguration(
          "whirr-local-itest-vbox.properties"));
    }
  }

}
