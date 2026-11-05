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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.annotation.Resource;

import org.apache.cxf.Bus;
import org.apache.cxf.buslifecycle.BusLifeCycleListener;
import org.apache.cxf.buslifecycle.BusLifeCycleManager;
import org.apache.cxf.common.logging.LogUtils;

public final class DelayedCachedOutputStreamCleaner implements CachedOutputStreamCleaner, BusLifeCycleListener {
    private static final Logger LOG = LogUtils.getL7dLogger(DelayedCachedOutputStreamCleaner.class);
    private static final long MIN_DELAY = 2000; /* 2 seconds */
    private static final String DEFAULT_STRATEGY = "default";
    private static final String SINGLE_TIMER_STRATEGY = "single-timer";
    private static final String TIMER_NAME = "DelayedCachedOutputStreamCleaner";
    private static final DelayedCleaner NOOP_CLEANER = new DelayedCleaner() {
        // NOOP
        @Override
        public void register(Closeable closeable) {
        }

        @Override
        public void unregister(Closeable closeable) {
        }

        @Override
        public void close() {
        }

        @Override
        public void clean() {
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public void forceClean() {
        }
    };

    private DelayedCleaner cleaner = NOOP_CLEANER;
    private boolean cleanupOnShutdown = true;

    private interface DelayedCleaner extends CachedOutputStreamCleaner, Closeable {
        /**
         * Same as {@link Closeable#close()} but without checked exceptions
         */
        void close();

        void forceClean();
    }

    /**
     * Cleaner strategy which schedules the clean up of every {@link org.apache.cxf.Bus} on a single
     * (shared) timer, the clean up task is only scheduled while there are streams registered.
     */
    private static final class SingleTimerDelayedCleaner implements DelayedCleaner {
        private static final Object TIMER_LOCK = new Object();
        private static Timer timer; /* shared by all instances, guarded by TIMER_LOCK */

        private final long delay; /* default is 30 minutes, in milliseconds */
        private final DelayQueue<DelayedCloseable> queue = new DelayQueue<DelayedCloseable>();
        private final Object lock = new Object();
        private TimerTask timerTask;

        SingleTimerDelayedCleaner(final long delay) {
            this.delay = delay;
        }

        @Override
        public void register(final Closeable closeable) {
            synchronized (lock) {
                queue.put(new DelayedCloseable(closeable, delay));
                if (timerTask == null) {
                    timerTask = new TimerTask() {
                        @Override
                        public void run() {
                            clean();
                        }
                    };
                    // Schedule while holding the lock, so the task cannot be cancelled before being scheduled
                    schedule(timerTask, Math.max(MIN_DELAY, delay >> 1));
                }
            }
        }

        @Override
        public void unregister(final Closeable closeable) {
            TimerTask oldTimerTask = null;
            synchronized (lock) {
                queue.remove(new DelayedCloseable(closeable, delay));
                if (queue.isEmpty() && timerTask != null) {
                    oldTimerTask = timerTask;
                    timerTask = null;
                }
            }
            if (oldTimerTask != null) {
                oldTimerTask.cancel();
            }
        }

        @Override
        public void clean() {
            final Collection<DelayedCloseable> closeables = new ArrayList<DelayedCloseable>();
            TimerTask oldTimerTask = null;
            synchronized (lock) {
                queue.drainTo(closeables);
                if (queue.isEmpty() && timerTask != null) {
                    oldTimerTask = timerTask;
                    timerTask = null;
                }
            }
            if (oldTimerTask != null) {
                oldTimerTask.cancel();
            }
            clean(closeables);
        }

        @Override
        public void forceClean() {
            TimerTask oldTimerTask = null;
            synchronized (lock) {
                clean(queue);
                if (timerTask != null) {
                    oldTimerTask = timerTask;
                    timerTask = null;
                }
            }
            if (oldTimerTask != null) {
                oldTimerTask.cancel();
            }
        }

        @Override
        public void close()  {
            TimerTask oldTimerTask = null;
            synchronized (lock) {
                queue.clear();
                if (timerTask != null) {
                    oldTimerTask = timerTask;
                    timerTask = null;
                }
            }
            if (oldTimerTask != null) {
                oldTimerTask.cancel();
            }
        }

        @Override
        public int size() {
            return queue.size();
        }

        private static void schedule(final TimerTask task, final long period) {
            synchronized (TIMER_LOCK) {
                if (timer == null) {
                    timer = new Timer(TIMER_NAME, true);
                }
                try {
                    timer.scheduleAtFixedRate(task, 0, period);
                } catch (final IllegalStateException ex) {
                    // The shared timer has been terminated (its thread is gone), replace it
                    timer = new Timer(TIMER_NAME, true);
                    timer.scheduleAtFixedRate(task, 0, period);
                }
            }
        }

        private void clean(Collection<DelayedCloseable> closeables) {
            final Iterator<DelayedCloseable> iterator = closeables.iterator();
            while (iterator.hasNext()) {
                final DelayedCloseable next = iterator.next();
                try {
                    iterator.remove();
                    LOG.warning("Unclosed (leaked?) stream detected: " + next.closeable.hashCode());
                    next.closeable.close();
                } catch (final IOException ex) {
                    LOG.warning("Unable to close (leaked?) stream: " + ex.getMessage());
                } catch (final RuntimeException ex) {
                    LOG.warning("Unable to close (leaked?) stream: " + ex.getMessage());
                }
            }
        }
    }

    /**
     * Default cleaner strategy which uses a timer per {@link org.apache.cxf.Bus}, the timer is only
     * started once the first stream is registered.
     */
    private static final class DefaultDelayedCleaner implements DelayedCleaner {
        private final long delay; /* default is 30 minutes, in milliseconds */
        private final DelayQueue<DelayedCloseable> queue = new DelayQueue<DelayedCloseable>();
        private final Object lock = new Object();
        private volatile boolean initialized;
        private Timer timer; /* guarded by lock */

        DefaultDelayedCleaner(final long delay) {
            this.delay = delay;
        }

        @Override
        public void register(Closeable closeable) {
            queue.put(new DelayedCloseable(closeable, delay));
            // Initialize timer lazily only when at least one closeable is registered
            if (!initialized) {
                startTimer();
            }
        }

        @Override
        public void unregister(Closeable closeable) {
            queue.remove(new DelayedCloseable(closeable, delay));
        }

        @Override
        public void clean() {
            final Collection<DelayedCloseable> closeables = new ArrayList<DelayedCloseable>();
            queue.drainTo(closeables);
            clean(closeables);
        }

        @Override
        public void forceClean() {
            clean(queue);
        }

        @Override
        public void close()  {
            final Timer t;
            synchronized (lock) {
                // The cleaner is closed, no timer should be started by any subsequent registration
                initialized = true;
                t = timer;
            }
            if (t != null) {
                t.cancel();
            }
            queue.clear();
        }

        @Override
        public int size() {
            return queue.size();
        }

        private void startTimer() {
            synchronized (lock) {
                if (!initialized) {
                    initialized = true;
                    timer = new Timer(TIMER_NAME, true);
                    timer.scheduleAtFixedRate(new TimerTask() {
                        @Override
                        public void run() {
                            clean();
                        }
                    }, 0, Math.max(MIN_DELAY, delay >> 1));
                }
            }
        }

        private void clean(Collection<DelayedCloseable> closeables) {
            final Iterator<DelayedCloseable> iterator = closeables.iterator();
            while (iterator.hasNext()) {
                final DelayedCloseable next = iterator.next();
                try {
                    iterator.remove();
                    LOG.warning("Unclosed (leaked?) stream detected: " + next.closeable.hashCode());
                    next.closeable.close();
                } catch (final IOException ex) {
                    LOG.warning("Unable to close (leaked?) stream: " + ex.getMessage());
                } catch (final RuntimeException ex) {
                    LOG.warning("Unable to close (leaked?) stream: " + ex.getMessage());
                }
            }
        }
    }

    private static final class DelayedCloseable implements Delayed {
        private final Closeable closeable;
        private final long expireAt;

        DelayedCloseable(final Closeable closeable, final long delay) {
            this.closeable = closeable;
            this.expireAt = System.nanoTime() + TimeUnit.NANOSECONDS.convert(delay, TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            final long other = ((DelayedCloseable) o).expireAt;
            return (expireAt < other) ? -1 : ((expireAt == other) ? 0 : 1);
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(expireAt - System.nanoTime(), TimeUnit.NANOSECONDS);
        }

        @Override
        public int hashCode() {
            return 31 + (closeable == null ? 0 : closeable.hashCode());
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }

            if (obj == null) {
                return false;
            }

            if (getClass() != obj.getClass()) {
                return false;
            }

            final DelayedCloseable other = (DelayedCloseable) obj;
            return closeable == null ? other.closeable == null : closeable.equals(other.closeable);
        }
    }

    @Resource
    public void setBus(Bus bus) {
        Number delayValue = null;
        BusLifeCycleManager busLifeCycleManager = null;
        Boolean cleanupOnShutdownValue = null;
        boolean singleTimerStrategy = false;

        if (bus != null) {
            delayValue = (Number) bus.getProperty(CachedConstants.CLEANER_DELAY_BUS_PROP);
            cleanupOnShutdownValue = (Boolean) bus.getProperty(CachedConstants.CLEANER_CLEAN_ON_SHUTDOWN_BUS_PROP);
            busLifeCycleManager = bus.getExtension(BusLifeCycleManager.class);
            final String strategy = (String) bus.getProperty(CachedConstants.CLEANER_STRATEGY_BUS_PROP);
            if (SINGLE_TIMER_STRATEGY.equalsIgnoreCase(strategy)) {
                singleTimerStrategy = true;
            } else if (strategy != null && !DEFAULT_STRATEGY.equalsIgnoreCase(strategy)) {
                throw new IllegalArgumentException("The value of " + CachedConstants.CLEANER_STRATEGY_BUS_PROP
                    + " property is invalid: " + strategy + " (should be " + DEFAULT_STRATEGY + " or "
                    + SINGLE_TIMER_STRATEGY + ")");
            }
        }

        if (cleaner != null) {
            cleaner.close();
        }

        if (cleanupOnShutdownValue != null) {
            cleanupOnShutdown = cleanupOnShutdownValue;
        } else {
            cleanupOnShutdown = true;
        }

        if (delayValue == null) {
            // Default delay is set to 30 mins
            cleaner = createCleaner(singleTimerStrategy, TimeUnit.MILLISECONDS.convert(30, TimeUnit.MINUTES));
        } else {
            final long value = delayValue.longValue();
            if (value > 0 && value >= MIN_DELAY) {
                cleaner = createCleaner(singleTimerStrategy, value); /* already in milliseconds */
            } else {
                cleaner = NOOP_CLEANER;
                if (value != 0) {
                    throw new IllegalArgumentException("The value of " + CachedConstants.CLEANER_DELAY_BUS_PROP
                        + " property is invalid: " + value + " (should be >= " + MIN_DELAY + ", 0 to deactivate)");
                }
            }
        }

        if (busLifeCycleManager != null) {
            busLifeCycleManager.registerLifeCycleListener(this);
        }
    }

    private static DelayedCleaner createCleaner(final boolean singleTimerStrategy, final long delay) {
        if (singleTimerStrategy) {
            return new SingleTimerDelayedCleaner(delay);
        }
        return new DefaultDelayedCleaner(delay);
    }

    @Override
    public void register(Closeable closeable) {
        cleaner.register(closeable);
    }

    @Override
    public void unregister(Closeable closeable) {
        cleaner.unregister(closeable);
    }

    @Override
    public int size() {
        return cleaner.size();
    }

    @Override
    public void clean() {
        cleaner.clean();
    }

    @Override
    public void initComplete() {
    }

    @Override
    public void postShutdown() {
        // If cleanup on shutdown is asked, force cleaning all cached output streams
        if (cleanupOnShutdown) {
            forceClean();
            cleaner.close();
        }
    }

    @Override
    public void preShutdown() {
        // If cleanup on shutdown is asked, defer closing till postShutdown hook
        if (!cleanupOnShutdown) {
            cleaner.close();
        }
    }

    public void forceClean() {
        cleaner.forceClean();
    }
}
