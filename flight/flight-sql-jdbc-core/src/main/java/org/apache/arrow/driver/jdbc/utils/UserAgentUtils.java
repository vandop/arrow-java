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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import org.apache.arrow.util.VisibleForTesting;

/** Utility class for generating UserAgent header information. */
public final class UserAgentUtils {
  private static final String UNKNOWN = "unknown";
  private static final String USER_AGENT_FORMAT = "ArrowFlightJDBC/%s (%s; %s; %s)";

  // Cache the driver version to avoid creating a new driver instance each time
  private static String cachedDriverVersion = null;

  // Application name that can be set by client applications
  private static String applicationName = null;

  private UserAgentUtils() {
    // Utility class should not be instantiated
  }

  /**
   * Builds a UserAgent string containing client, OS, OSVersion, and Driver version information.
   *
   * @return A formatted UserAgent string
   */
  public static String getUserAgentString() {
    String driverVersion = getDriverVersion();
    String osInfo = getOsInfo();
    String clientInfo = getClientInfo();

    return String.format(USER_AGENT_FORMAT, driverVersion, osInfo, getJavaVersion(), clientInfo);
  }

  /**
   * Sets the application name to be included in the UserAgent string. This can be called by client
   * applications to identify themselves.
   *
   * @param appName The application name
   */
  public static void setApplicationName(String appName) {
    applicationName = appName;
  }

  /**
   * Gets the client application name if set, otherwise returns "unknown".
   *
   * @return The client application name
   */
  private static String getClientInfo() {
    return applicationName != null ? applicationName : UNKNOWN;
  }

  /**
   * Gets the driver version from flight.properties.
   *
   * @return The driver version string or "unknown" if not available
   */
  private static String getDriverVersion() {
    if (cachedDriverVersion != null) {
      return cachedDriverVersion;
    }

    try {
      // Load version directly from properties file to avoid creating a driver instance
      try (InputStream flightPropertiesStream =
          UserAgentUtils.class.getResourceAsStream("/properties/flight.properties")) {
        if (flightPropertiesStream != null) {
          Properties properties = new Properties();
          try (Reader reader =
              new BufferedReader(
                  new InputStreamReader(flightPropertiesStream, StandardCharsets.UTF_8))) {
            properties.load(reader);
            cachedDriverVersion =
                properties.getProperty("org.apache.arrow.flight.jdbc-driver.version", UNKNOWN);
            return cachedDriverVersion;
          }
        }
      }
      return UNKNOWN;
    } catch (IOException e) {
      return UNKNOWN;
    }
  }

  /**
   * Gets the OS name, version and architecture.
   *
   * @return A string containing OS name, version and architecture
   */
  private static String getOsInfo() {
    String osName = System.getProperty("os.name", UNKNOWN);
    String osVersion = System.getProperty("os.version", UNKNOWN);
    String osArch = System.getProperty("os.arch", UNKNOWN);
    return osName + " " + osVersion + " (" + osArch + ")";
  }

  /**
   * Gets the Java version.
   *
   * @return The Java version string
   */
  private static String getJavaVersion() {
    return "Java " + System.getProperty("java.version", UNKNOWN);
  }

  /** Resets the cached driver version. Used for testing. */
  @VisibleForTesting
  static void resetCachedDriverVersion() {
    cachedDriverVersion = null;
  }
}
