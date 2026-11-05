/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cxf.io;

public final class CachedConstants {

    /**
     * The delay (in ms) for cleaning up unclosed {@code CachedOutputStream} instances. 30 minutes
     * is specified by default, the minimum value is 2 seconds. If the value of the delay is set to
     * 0 (or is negative), the cleaner will be deactivated.
     */
    public static final String CLEANER_DELAY_BUS_PROP =
        "bus.io.CachedOutputStreamCleaner.Delay";

    /**
     * Forces cleaning of the unclosed {@code CachedOutputStream} instances on {@link org.apache.cxf.Bus}
     * shutdown. The default value is "true", if the cleaner is deactivated or the value is set to "false",
     * no cleanup attempt on shutdown will be performed.
     */
    public static final String CLEANER_CLEAN_ON_SHUTDOWN_BUS_PROP =
        "bus.io.CachedOutputStreamCleaner.CleanOnShutdown";

    /**
     * The strategy to be used for cleaning up unclosed {@code CachedOutputStream} instances. By default,
     * the cleaner implementation creates a timer per each {@link org.apache.cxf.Bus} instance. However,
     * in certain deployments it could lead to excessive number of timers being created, so there is an
     * alternative strategy that uses single timer instance. The supported strategies are: default,
     * single-timer.
     */
    public static final String CLEANER_STRATEGY_BUS_PROP =
        "bus.io.CachedOutputStreamCleaner.Strategy";

    private CachedConstants() {
        // complete
    }
}
