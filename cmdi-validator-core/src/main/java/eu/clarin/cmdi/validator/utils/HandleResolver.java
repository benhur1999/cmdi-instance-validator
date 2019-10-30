/**
 * This software is copyright (c) 2014-2019 by
 *  - Institut fuer Deutsche Sprache (http://www.ids-mannheim.de)
 * This is free software. You can redistribute it
 * and/or modify it under the terms described in
 * the GNU General Public License v3 of which you
 * should have received a copy. Otherwise you can download
 * it from
 *
 *   http://www.gnu.org/licenses/gpl-3.0.txt
 *
 * @copyright Institut fuer Deutsche Sprache (http://www.ids-mannheim.de)
 *
 * @license http://www.gnu.org/licenses/gpl-3.0.txt
 *  GNU General Public License v3
 */
package eu.clarin.cmdi.validator.utils;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarin.cmdi.validator.Version;

public class HandleResolver {
    public static final class Statistics {
        private final long cacheHitCount;
        private final long cacheMissCount;
        private final long timeoutCount;
        private final long unknownHostCount;
        private final long errorCount;
        private final long totalRequestsCount;
        private final int currentRequestsCount;
        private final int currentCacheSize;

        private Statistics(long cacheHitCount,
                long cacheMissCount,
                long timeoutCount,
                long unknownHostCount,
                long errorCount,
                long totalRequestsCount,
                int currentRequestsCount,
                int currentCacheSize) {
            this.cacheHitCount        = cacheHitCount;
            this.cacheMissCount       = cacheMissCount;
            this.timeoutCount         = timeoutCount;
            this.unknownHostCount     = unknownHostCount;
            this.errorCount           = errorCount;
            this.totalRequestsCount   = totalRequestsCount;
            this.currentRequestsCount = currentRequestsCount;
            this.currentCacheSize     = currentCacheSize;
        }


        public long getCacheHitCount() {
            return cacheHitCount;
        }


        public long getCacheMissCount() {
            return cacheMissCount;
        }


        public long getTimeoutCount() {
            return timeoutCount;
        }


        public long getUnknownHostCount() {
            return unknownHostCount;
        }


        public long getErrorCount() {
            return errorCount;
        }


        public long getTotalRequestsCount() {
            return totalRequestsCount;
        }


        public int getCurrentRequestsCount() {
            return currentRequestsCount;
        }

        public int getCurrentCacheSize() {
            return currentCacheSize;
        }
    }
    private static final Logger logger =
            LoggerFactory.getLogger(HandleResolver.class);
    public static final int TIMEOUT      = -1;
    public static final int UNKNOWN_HOST = -2;
    public static final int ERROR        = -3;
    private static final String USER_AGENT =
            "CMDI-Validator-HandleResolver/" + Version.getVersion();
    private static final int DEFAULT_MAX_CONCURRENT_REQUESTS = 8;
    private static final int DEFAULT_CONNECT_TIMEOUT = 5000;
    private static final int DEFAULT_SOCKET_TIMEOUT = 10000;
    private final LRUCache<URI, Integer> cache =
            new LRUCache<URI, Integer>(16 * 1024);
    private final Set<URI> pending;
    private final int maxConcurrentRequestsCount;
    private final Semaphore maxConcurrentRequests;
    private final CloseableHttpClient client;
    private long cacheHitCount                = 0;
    private long cacheMissCount               = 0;
    private AtomicLong timeoutCount           = new AtomicLong();
    private AtomicLong unknownHostCount       = new AtomicLong();
    private AtomicLong errorCount             = new AtomicLong();
    private AtomicLong totalRequestsCount     = new AtomicLong();
    private AtomicInteger currentRequestCount = new AtomicInteger();
    private final Object waiter = new Object();


    public HandleResolver(int maxConcurrentRequests) {
        if (maxConcurrentRequests < 1) {
            throw new IllegalArgumentException("maxConcurrentRequests < 1");
        }
        this.pending = new HashSet<URI>(maxConcurrentRequests * 4);
        this.client = createHttpClient(DEFAULT_CONNECT_TIMEOUT,
                DEFAULT_SOCKET_TIMEOUT);
        this.maxConcurrentRequestsCount = maxConcurrentRequests;
        this.maxConcurrentRequests = new Semaphore(maxConcurrentRequests, true);
    }


    public HandleResolver() {
        this(DEFAULT_MAX_CONCURRENT_REQUESTS);
    }


    public int resolve(final URI handle) throws IOException {
        if (handle == null) {
            throw new NullPointerException("handle == null");
        }
        logger.debug("resolving '{}'", handle);
        totalRequestsCount.incrementAndGet();
        for (;;) {
            boolean doResolve = false;
            synchronized (cache) {
                final Integer cached = cache.get(handle);
                if (cached != null) {
                    logger.trace("got cached result for '{}': {}",
                            handle, cached);
                    cacheHitCount++;
                    return cached.intValue();
                }
                cacheMissCount++;

                synchronized (pending) {
                    if (!pending.contains(handle)) {
                        doResolve = true;
                        pending.add(handle);
                    }
                } // synchronized (pending)
            } // synchronized (cache)

            // either resolve in this thread of wait for pending resolve result
            if (doResolve) {
                int result = ERROR;
                try {
                    result = doResolve(handle);
                } finally {
                    // cache result and notify other threads
                    synchronized (cache) {
                        logger.trace("caching result {} for '{}'",
                                result, handle);
                        cache.put(handle, Integer.valueOf(result));
                        synchronized (pending) {
                            pending.remove(handle);
                            synchronized (waiter) {
                                waiter.notifyAll();
                            } // synchronized (waiter)
                        } // synchronized (pending)
                    } // synchronized (cache)
                    if (result == ERROR) {
                        errorCount.incrementAndGet();
                    }
                }
                return result;
            } else {
                try {
                    synchronized (waiter) {
                        waiter.wait();
                    } // synchronized (waiter)
                } catch (InterruptedException e) {
                    errorCount.incrementAndGet();
                    return ERROR;
                }
            }
        } // for
    }


    public Statistics getStatistics() {
        synchronized (cache) {
            return new Statistics(cacheHitCount,
                    cacheMissCount,
                    timeoutCount.get(),
                    unknownHostCount.get(),
                    errorCount.get(),
                    totalRequestsCount.get(),
                    currentRequestCount.get(),
                    cache.size());
        } // synchronized (cache)
    }


    public void clear() {
        try {
            try {
                // acquire all permits to deny any requests from happening ...
                maxConcurrentRequests.acquire(maxConcurrentRequestsCount);

                // clear data
                synchronized (cache) {
                    cache.clear();
                    cacheHitCount  = 0;
                    cacheMissCount = 0;
                    timeoutCount.set(0);
                    unknownHostCount.set(0);
                    errorCount.set(0);
                    totalRequestsCount.set(0);
                } // synchronized (cache)
            } finally {
                maxConcurrentRequests.release();
            }
        } catch (InterruptedException e) {
            /* IGNORE */
        }
    }


    private int doResolve(final URI handle) throws IOException {
        logger.trace("performing HTTP request for '{}'", handle);

        try {
            maxConcurrentRequests.acquire();
        } catch (InterruptedException e) {
            throw new InterruptedIOException();
        }

        currentRequestCount.incrementAndGet();
        final HttpHead request = new HttpHead(handle);
        try {
            final CloseableHttpResponse response =
                    client.execute(request, new BasicHttpContext());
            try {
                final StatusLine status = response.getStatusLine();
                return status.getStatusCode();
            } finally {
                response.close();
            }
        } catch (ConnectTimeoutException e) {
            timeoutCount.incrementAndGet();
            return TIMEOUT;
        } catch (SocketTimeoutException e) {
            timeoutCount.incrementAndGet();
            return TIMEOUT;
        } catch (UnknownHostException e) {
            unknownHostCount.incrementAndGet();
            return UNKNOWN_HOST;
        } finally {
            request.reset();
            currentRequestCount.decrementAndGet();
            maxConcurrentRequests.release();
        }
    }


    private CloseableHttpClient createHttpClient(int connectTimeout,
            int socketTimeout) {
        final PoolingHttpClientConnectionManager manager =
                new PoolingHttpClientConnectionManager();
        manager.setDefaultMaxPerRoute(8);
        manager.setMaxTotal(128);
        manager.setValidateAfterInactivity(-1);

        final SocketConfig socketConfig = SocketConfig.custom()
                .setSoReuseAddress(true)
                .setSoLinger(0)
                .build();

        final ConnectionConfig connectionConfig = ConnectionConfig.custom()
                .setBufferSize(1024)
                .build();

        final RequestConfig requestConfig = RequestConfig.custom()
                .setAuthenticationEnabled(false)
                .setRedirectsEnabled(true)
                .setMaxRedirects(4)
                .setCircularRedirectsAllowed(false)
                .setCookieSpec(CookieSpecs.IGNORE_COOKIES)
                .setConnectTimeout(connectTimeout)
                .setSocketTimeout(socketTimeout)
                .setConnectionRequestTimeout(0) /* infinite */
                .build();

        final ConnectionKeepAliveStrategy keepAliveStrategy =
                new ConnectionKeepAliveStrategy() {
            @Override
            public long getKeepAliveDuration(final HttpResponse response,
                    final HttpContext context) {
                return 15000;
            }
        };

        return HttpClients.custom()
                .setUserAgent(USER_AGENT)
                .setConnectionManager(manager)
                .setDefaultSocketConfig(socketConfig)
                .setDefaultConnectionConfig(connectionConfig)
                .setDefaultRequestConfig(requestConfig)
                .setKeepAliveStrategy(keepAliveStrategy)
                .build();
    }


    @Override
    protected void finalize() throws Throwable {
        client.close();
    }

} // class HandleResolver
