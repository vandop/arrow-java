/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.driver.jdbc.utils;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Tests for {@link UserAgentUtils}. */
public class UserAgentUtilsTest {

  @Test
  public void testGetUserAgentString() {
    String userAgent = UserAgentUtils.getUserAgentString();

    // Verify the user agent string is not null
    assertNotNull(userAgent);

    // Verify it contains the expected format elements
    assertTrue(userAgent.startsWith("ArrowFlightJDBC/"), "Should start with ArrowFlightJDBC/");

    // Verify it contains OS information
    String osName = System.getProperty("os.name");
    assertTrue(userAgent.contains(osName), "Should contain OS name: " + osName);

    // Verify it contains Java version
    assertTrue(userAgent.contains("Java "), "Should contain Java version");
  }

  @Test
  public void testSetApplicationName() {
    // Reset cached driver version to ensure a clean test
    UserAgentUtils.resetCachedDriverVersion();

    // Set application name
    String testAppName = "TestApplication";
    UserAgentUtils.setApplicationName(testAppName);

    // Get user agent string and verify it contains the application name
    String userAgent = UserAgentUtils.getUserAgentString();
    assertTrue(userAgent.contains(testAppName), "Should contain application name: " + testAppName);

    // Reset application name for other tests
    UserAgentUtils.setApplicationName(null);
  }
}
