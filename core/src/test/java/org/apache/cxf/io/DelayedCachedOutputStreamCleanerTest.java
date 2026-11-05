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

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cxf.Bus;
import org.apache.cxf.BusFactory;
import org.apache.cxf.bus.extension.ExtensionManagerBus;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(Parameterized.class)
public class DelayedCachedOutputStreamCleanerTest {
    private static final String CONTENT = "Hello Secret World! This content is written into a temporary file";
    private static final String TIMER_NAME = "DelayedCachedOutputStreamCleaner";

    private final String strategy;
    private Bus bus;

    public DelayedCachedOutputStreamCleanerTest(final String strategy) {
        this.strategy = strategy;
    }

    @Parameterized.Parameters(name = CachedConstants.CLEANER_STRATEGY_BUS_PROP + "= {0}")
    public static Collection<Object[]> strategies() {
        return Arrays.asList(new Object[][] {{"default"}, {"single-timer"}, {null}});
    }

    @After
    public void tearDown() {
        if (bus != null) {
            bus.shutdown(true);
            bus = null;
        }
    }

    @Test
    public void testNoop() {
        final Map<String, Object> properties = Collections.<String, Object>singletonMap(
            CachedConstants.CLEANER_DELAY_BUS_PROP, 0);
        bus = createBus(properties);

        final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
        assertThat(cleaner, instanceOf(DelayedCachedOutputStreamCleaner.class)); /* noop */

        assertNoopCleaner(cleaner);
    }

    @Test
    public void testNoTimer() {
        final Set<Thread> before = cleanerThreads();

        final Map<String, Object> properties = Collections.<String, Object>singletonMap(
            CachedConstants.CLEANER_DELAY_BUS_PROP, 2500);
        bus = createBus(properties);

        final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
        assertThat(cleaner, instanceOf(DelayedCachedOutputStreamCleaner.class));

        // No timer is started until the first stream is registered
        assertThat(newCleanerThreads(before), equalTo(0));

        cleaner.register(flagOnClose(new AtomicBoolean(false)));
        if (isSingleTimerStrategy()) {
            // The single (shared) timer is started, unless it is running already
            assertThat(newCleanerThreads(before) <= 1, is(true));
        } else {
            assertThat(newCleanerThreads(before), equalTo(1));
        }
    }

    @Test
    public void testNoTimerAfterShutdown() {
        final Map<String, Object> properties = Collections.<String, Object>singletonMap(
            CachedConstants.CLEANER_DELAY_BUS_PROP, 2500);
        bus = createBus(properties);

        final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
        assertThat(cleaner, instanceOf(DelayedCachedOutputStreamCleaner.class));

        // Closes the bus (and so the cleaner) before any stream is registered
        bus.shutdown(true);

        final Set<Thread> before = cleanerThreads();
        cleaner.register(flagOnClose(new AtomicBoolean(false)));
        if (isSingleTimerStrategy()) {
            // The single (shared) timer is started, unless it is running already
            assertThat(newCleanerThreads(before) <= 1, is(true));
        } else {
            // The closed cleaner must not start (and leak) a timer of its own
            assertThat(newCleanerThreads(before), equalTo(0));
        }
    }

    @Test
    public void testTimers() {
        final Map<String, Object> properties = Collections.<String, Object>singletonMap(
            CachedConstants.CLEANER_DELAY_BUS_PROP, 2500);
        final Set<Thread> before = cleanerThreads();
        final Bus[] buses = new Bus[3];
        try {
            for (int i = 0; i < buses.length; ++i) {
                buses[i] = createBus(properties);
                final CachedOutputStreamCleaner cleaner = buses[i].getExtension(CachedOutputStreamCleaner.class);
                cleaner.register(flagOnClose(new AtomicBoolean(false)));
            }

            if (isSingleTimerStrategy()) {
                // All buses share the single timer
                assertThat(newCleanerThreads(before) <= 1, is(true));
            } else {
                // Every bus has a timer of its own
                assertThat(newCleanerThreads(before), equalTo(buses.length));
            }
        } finally {
            for (Bus b : buses) {
                if (b != null) {
                    b.shutdown(true);
                }
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidStrategy() {
        bus = createBus();
        bus.setProperty(CachedConstants.CLEANER_STRATEGY_BUS_PROP, "unknown");
        new DelayedCachedOutputStreamCleaner().setBus(bus);
    }

    @Test
    public void testForceClean() throws InterruptedException {
        bus = createBus();

        final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
        assertThat(cleaner, instanceOf(DelayedCachedOutputStreamCleaner.class));

        final AtomicBoolean latch = new AtomicBoolean(false);
        final Closeable closeable = flagOnClose(latch);
        cleaner.register(closeable);

        final DelayedCachedOutputStreamCleaner delayedCleaner = (DelayedCachedOutputStreamCleaner) cleaner;
        delayedCleaner.forceClean();

        // Await for Closeable::close to be called
        assertThat(latch.get(), is(true));
    }

    @Test
    public void testClean() throws InterruptedException {
        final AtomicInteger latch = new AtomicInteger();
        final Closeable closeable1 = countOnClose(latch);
        final Closeable closeable2 = countOnClose(latch);

        /* Delay of 2.5 seconds */
        final Map<String, Object> properties = Collections.<String, Object>singletonMap(
            CachedConstants.CLEANER_DELAY_BUS_PROP, 2500);
        bus = createBus(properties);

        final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
        cleaner.register(closeable1);
        cleaner.register(closeable2);

        // Await for Closeable::close to be called on schedule
        awaitAtMost(5, TimeUnit.SECONDS, latch, 2);
        assertThat(cleaner, instanceOf(DelayedCachedOutputStreamCleaner.class));
    }

    @Test
    public void testForceCleanForEmpty() throws InterruptedException {
        bus = createBus();

        final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
        assertThat(cleaner, instanceOf(DelayedCachedOutputStreamCleaner.class));

        final AtomicBoolean latch = new AtomicBoolean(false);
        final Closeable closeable = flagOnClose(latch);

        cleaner.register(closeable);
        cleaner.unregister(closeable);

        final DelayedCachedOutputStreamCleaner delayedCleaner = (DelayedCachedOutputStreamCleaner) cleaner;
        delayedCleaner.forceClean();

        // Closeable::close should not be called
        assertThat(latch.get(), is(false));
    }

    @Test
    public void testForceCleanException() throws InterruptedException {
        bus = createBus();

        final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
        assertThat(cleaner, instanceOf(DelayedCachedOutputStreamCleaner.class));

        final AtomicInteger latch = new AtomicInteger();
        final Closeable closeable2 = countOnClose(latch);
        final Closeable closeable1 = new Closeable() {
            @Override
            public void close() throws IOException {
                latch.incrementAndGet();
                throw new IOException("Simulated");
            }
        };
        cleaner.register(closeable1);
        cleaner.register(closeable2);

        final DelayedCachedOutputStreamCleaner delayedCleaner = (DelayedCachedOutputStreamCleaner) cleaner;
        delayedCleaner.forceClean();

        // Try to call force clean one more time
        delayedCleaner.forceClean();

        // Await for Closeable::close to be called
        assertThat(latch.get(), equalTo(2));
    }

    @Test
    public void testCleanOnShutdown() throws InterruptedException {
        /* Delay of 2.5 seconds */
        final Map<String, Object> properties = Collections.<String, Object>singletonMap(
            CachedConstants.CLEANER_DELAY_BUS_PROP, 2500);
        bus = createBus(properties);

        final AtomicBoolean latch = new AtomicBoolean();
        final Closeable closeable = flagOnClose(latch);

        bus.setProperty(CachedConstants.CLEANER_DELAY_BUS_PROP, 2500); /* 2.5 seconds */
        final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
        cleaner.register(closeable);

        // Closes the bus, the cleaner should cancel the internal timer(s)
        bus.shutdown(true);

        // The Closeable::close should be called on shutdown
        assertThat(latch.get(), is(true));
    }

    @Test
    public void testCleanOnShutdownDisabled() throws InterruptedException {
        /* Delay of 2.5 seconds */
        final Map<String, Object> properties = new HashMap<String, Object>();
        properties.put(CachedConstants.CLEANER_DELAY_BUS_PROP, 2500);
        properties.put(CachedConstants.CLEANER_CLEAN_ON_SHUTDOWN_BUS_PROP, false);
        bus = createBus(properties);

        final AtomicBoolean latch = new AtomicBoolean();
        final Closeable closeable = flagOnClose(latch);

        bus.setProperty(CachedConstants.CLEANER_DELAY_BUS_PROP, 2500); /* 2.5 seconds */
        final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
        cleaner.register(closeable);

        // Closes the bus, the cleaner should cancel the internal timer(s)
        bus.shutdown(true);

        // The Closeable::close should not be called since timer(s) is cancelled
        assertDuring(3, TimeUnit.SECONDS, latch, false);
    }

    @Test
    public void testNegativeDelay() throws InterruptedException {
        final Map<String, Object> properties = Collections.<String, Object>singletonMap(
            CachedConstants.CLEANER_DELAY_BUS_PROP, -1);
        bus = createBus(properties);

        final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
        assertThat(cleaner, instanceOf(DelayedCachedOutputStreamCleaner.class)); /* noop */

        assertNoopCleaner(cleaner);
    }

    @Test
    public void testTooSmallDelay() throws InterruptedException {
        final Map<String, Object> properties = Collections.<String, Object>singletonMap(
            CachedConstants.CLEANER_DELAY_BUS_PROP, 1500);
        bus = createBus(properties);

        final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
        assertThat(cleaner, instanceOf(DelayedCachedOutputStreamCleaner.class)); /* noop */

        assertNoopCleaner(cleaner);
    }

    @Test
    public void testDelayedClean() throws InterruptedException {
        final AtomicInteger latch = new AtomicInteger();
        final Closeable closeable1 = countOnClose(latch);
        final Closeable closeable2 = countOnClose(latch);

        /* Delay of 5 seconds */
        final Map<String, Object> properties = Collections.<String, Object>singletonMap(
            CachedConstants.CLEANER_DELAY_BUS_PROP, 2500);
        bus = createBus(properties);

        final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
        cleaner.register(closeable1);

        Thread.sleep(2000);
        cleaner.register(closeable2);

        // Await for Closeable::close to be called on schedule
        awaitAtMost(3, TimeUnit.SECONDS, latch, 1);

        // Await for Closeable::close to be called on schedule
        awaitAtMost(5, TimeUnit.SECONDS, latch, 2);

        assertThat(cleaner, instanceOf(DelayedCachedOutputStreamCleaner.class));
    }

    @Test
    public void testDelayIsInMilliseconds() throws InterruptedException {
        final AtomicBoolean latch = new AtomicBoolean(false);
        final Closeable closeable = flagOnClose(latch);

        /* Delay of 2.5 seconds, the stream must not be considered as leaked before that */
        final Map<String, Object> properties = Collections.<String, Object>singletonMap(
            CachedConstants.CLEANER_DELAY_BUS_PROP, 2500);
        bus = createBus(properties);

        final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
        cleaner.register(closeable);

        Thread.sleep(500);
        cleaner.clean();

        // The delay has not expired yet, Closeable::close should not be called
        assertThat(latch.get(), is(false));
        assertThat(cleaner.size(), equalTo(1));
    }

    @Test
    public void testAbandonedCachedOutputStreamIsCleanedOnSchedule() throws Exception {
        /* Delay of 2 seconds (the minimum) */
        final Map<String, Object> properties = Collections.<String, Object>singletonMap(
            CachedConstants.CLEANER_DELAY_BUS_PROP, 2000);
        bus = createBus(properties);

        final Bus oldBus = BusFactory.getThreadDefaultBus(false);
        BusFactory.setThreadDefaultBus(bus);
        try {
            final CachedOutputStream cos = new CachedOutputStream(4);
            final File tempFile = writeToTempFile(cos);

            // The stream is never closed (leaked), the cleaner has to close it and remove the file
            awaitDeleted(10, TimeUnit.SECONDS, tempFile);
            assertThat(bus.getExtension(CachedOutputStreamCleaner.class).size(), equalTo(0));
        } finally {
            BusFactory.setThreadDefaultBus(oldBus);
        }
    }

    @Test
    public void testAbandonedInputStreamIsCleaned() throws Exception {
        bus = createBus();

        final Bus oldBus = BusFactory.getThreadDefaultBus(false);
        BusFactory.setThreadDefaultBus(bus);
        try {
            final CachedOutputStream cos = new CachedOutputStream(4);
            final File tempFile = writeToTempFile(cos);

            // The output stream is properly closed but the input stream is leaked
            final InputStream in = cos.getInputStream();
            assertThat(in, notNullValue());
            cos.close();
            assertThat("temporary file is still in use", tempFile.exists(), is(true));

            final DelayedCachedOutputStreamCleaner cleaner =
                (DelayedCachedOutputStreamCleaner) bus.getExtension(CachedOutputStreamCleaner.class);
            cleaner.forceClean();

            assertThat("temporary file is not deleted", tempFile.exists(), is(false));
            assertThat(cleaner.size(), equalTo(0));
        } finally {
            BusFactory.setThreadDefaultBus(oldBus);
        }
    }

    @Test
    public void testAbandonedCachedOutputStreamIsCleanedOnShutdown() throws Exception {
        bus = createBus();

        final Bus oldBus = BusFactory.getThreadDefaultBus(false);
        BusFactory.setThreadDefaultBus(bus);
        try {
            final CachedOutputStream cos = new CachedOutputStream(4);
            final File tempFile = writeToTempFile(cos);
            final InputStream in = cos.getInputStream();
            assertThat(in, notNullValue());

            // Both streams are leaked, the cleaner has to close them on shutdown
            bus.shutdown(true);

            assertThat("temporary file is not deleted", tempFile.exists(), is(false));
        } finally {
            BusFactory.setThreadDefaultBus(oldBus);
        }
    }

    @Test
    public void testClosedStreamIsUnregistered() throws Exception {
        bus = createBus();

        final Bus oldBus = BusFactory.getThreadDefaultBus(false);
        BusFactory.setThreadDefaultBus(bus);
        try {
            final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
            assertThat(cleaner, instanceOf(DelayedCachedOutputStreamCleaner.class));

            // close() of the file backed stream
            final CachedOutputStream cos = new CachedOutputStream(4);
            final File tempFile = writeToTempFile(cos);
            assertThat(cleaner.size(), equalTo(1));
            cos.close();
            assertThat(tempFile.exists(), is(false));
            assertThat(cleaner.size(), equalTo(0));
        } finally {
            BusFactory.setThreadDefaultBus(oldBus);
        }
    }

    @Test
    public void testResetOutStreamIsUnregistered() throws Exception {
        bus = createBus();

        final Bus oldBus = BusFactory.getThreadDefaultBus(false);
        BusFactory.setThreadDefaultBus(bus);
        try {
            final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
            assertThat(cleaner, instanceOf(DelayedCachedOutputStreamCleaner.class));

            // resetOut() of the file backed stream (see please https://issues.apache.org/jira/browse/CXF-9110)
            final CachedOutputStream cos = new CachedOutputStream(4);
            final File tempFile = writeToTempFile(cos);
            assertThat(cleaner.size(), equalTo(1));
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            cos.resetOut(out, true);
            assertThat(out.toString("UTF-8"), equalTo(CONTENT));
            assertThat(tempFile.exists(), is(false));
            assertThat(cleaner.size(), equalTo(0));
            cos.close();
            assertThat(cleaner.size(), equalTo(0));
        } finally {
            BusFactory.setThreadDefaultBus(oldBus);
        }
    }

    @Test
    public void testClosedInputStreamIsUnregistered() throws Exception {
        bus = createBus();

        final Bus oldBus = BusFactory.getThreadDefaultBus(false);
        BusFactory.setThreadDefaultBus(bus);
        try {
            final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
            assertThat(cleaner, instanceOf(DelayedCachedOutputStreamCleaner.class));

            // getInputStream() of the file backed stream, both streams are closed
            final CachedOutputStream cos = new CachedOutputStream(4);
            final File tempFile = writeToTempFile(cos);
            final InputStream in = cos.getInputStream();
            assertThat(cleaner.size(), equalTo(2));
            cos.close();
            in.close();
            assertThat(tempFile.exists(), is(false));
            assertThat(cleaner.size(), equalTo(0));
        } finally {
            BusFactory.setThreadDefaultBus(oldBus);
        }
    }

    @Test
    public void testLeakedStreamContentIsNotRead() throws InterruptedException {
        bus = createBus();

        final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
        assertThat(cleaner, instanceOf(DelayedCachedOutputStreamCleaner.class));

        // CachedOutputStream::toString() reads (and so the log would expose) the whole cached content
        final AtomicBoolean closed = new AtomicBoolean(false);
        final AtomicBoolean toStringCalled = new AtomicBoolean(false);
        cleaner.register(new Closeable() {
            @Override
            public void close() {
                closed.set(true);
            }

            @Override
            public String toString() {
                toStringCalled.set(true);
                return CONTENT;
            }
        });

        ((DelayedCachedOutputStreamCleaner) cleaner).forceClean();

        assertThat(closed.get(), is(true));
        assertThat(toStringCalled.get(), is(false));
    }

    private void assertNoopCleaner(final CachedOutputStreamCleaner cleaner) {
        final AtomicBoolean latch = new AtomicBoolean(false);
        final Closeable closeable = flagOnClose(latch);
        cleaner.register(closeable);

        final DelayedCachedOutputStreamCleaner delayedCleaner = (DelayedCachedOutputStreamCleaner) cleaner;
        delayedCleaner.forceClean();

        // Noop, Closeable::close should not be called
        assertThat(latch.get(), is(false));
    }

    private boolean isSingleTimerStrategy() {
        return "single-timer".equals(strategy);
    }

    private Bus createBus() {
        return createBus(Collections.<String, Object>emptyMap());
    }

    /**
     * The {@link ExtensionManagerBus} does not populate the bus properties from the map passed to
     * its constructor, so the properties are set explicitly (before the cleaner extension is loaded).
     */
    private Bus createBus(final Map<String, Object> properties) {
        final Bus b = new ExtensionManagerBus();
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            b.setProperty(entry.getKey(), entry.getValue());
        }
        if (strategy != null) {
            b.setProperty(CachedConstants.CLEANER_STRATEGY_BUS_PROP, strategy);
        }
        return b;
    }

    /**
     * The (alive) timer threads started by the cleaner(s)
     */
    private static Set<Thread> cleanerThreads() {
        final Set<Thread> threads = new HashSet<Thread>();
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (thread.isAlive() && TIMER_NAME.equals(thread.getName())) {
                threads.add(thread);
            }
        }
        return threads;
    }

    /**
     * The number of the cleaner timer threads started since the {@code before} snapshot was taken
     */
    private static int newCleanerThreads(final Set<Thread> before) {
        final Set<Thread> after = cleanerThreads();
        after.removeAll(before);
        return after.size();
    }

    private static File writeToTempFile(final CachedOutputStream cos) throws IOException {
        cos.write(CONTENT.getBytes("UTF-8"));
        cos.flush();

        final File tempFile = cos.getTempFile();
        assertThat("expects a tmp file", tempFile, notNullValue());
        assertThat("expects a tmp file", tempFile.exists(), is(true));
        return tempFile;
    }

    private static Closeable flagOnClose(final AtomicBoolean latch) {
        return new Closeable() {
            @Override
            public void close() {
                latch.compareAndSet(false, true);
            }
        };
    }

    private static Closeable countOnClose(final AtomicInteger latch) {
        return new Closeable() {
            @Override
            public void close() {
                latch.incrementAndGet();
            }
        };
    }

    private static void awaitAtMost(final long timeout, final TimeUnit unit, final AtomicInteger latch,
                                    final int expected) throws InterruptedException {
        final long deadline = System.nanoTime() + unit.toNanos(timeout);
        while (latch.get() != expected && System.nanoTime() - deadline < 0) {
            Thread.sleep(50);
        }
        assertThat(latch.get(), equalTo(expected));
    }

    private static void assertDuring(final long duration, final TimeUnit unit, final AtomicBoolean latch,
                                     final boolean expected) throws InterruptedException {
        final long deadline = System.nanoTime() + unit.toNanos(duration);
        while (System.nanoTime() - deadline < 0) {
            assertThat(latch.get(), is(expected));
            Thread.sleep(100);
        }
        assertThat(latch.get(), is(expected));
    }

    private static void awaitDeleted(final long timeout, final TimeUnit unit, final File file)
        throws InterruptedException {
        final long deadline = System.nanoTime() + unit.toNanos(timeout);
        while (file.exists() && System.nanoTime() - deadline < 0) {
            Thread.sleep(100);
        }
        assertThat("temporary file is not deleted", file.exists(), is(false));
    }
}
