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
package eu.clarin.cmdi.validator;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.FileLock;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.xml.XMLConstants;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CMDISchemaLoader {
    public static final long DISABLE_CACHE_AGING = -1;
    private static final Logger logger =
            LoggerFactory.getLogger(CMDISchemaLoader.class);
    private static final String USER_AGENT =
            "CMDI-Validator-SchemaLoader/" + Version.getVersion();
    private static final String XML_XSD_RESSOURCE = "/xml.xsd";
    private static final String CMDI_ENEVLOPE_NS_URI =
            "http://www.clarin.eu/cmd/1";
    private static final String CMDI_ENVELOPE_XSD_RESSOURCE =
            "/cmd-envelop.xsd"; 
    private static final String EXTENSION_XSD   = "xsd";
    private static final String EXTENSION_ERROR = "error";
    private final File cacheDirectory;
    private final long maxCacheAge;
    private final long maxNegativeCacheAge;
    private final CloseableHttpClient httpClient;
    private final Set<String> pending = new HashSet<String>(128);
    private final Object guard = new Object();
    private final Object waiter = new Object();


    public CMDISchemaLoader(File cacheDirectory, long maxCacheAge,
            long maxNegativeCacheAge, int connectTimeout,
            int socketTimeout) {
        if (cacheDirectory == null) {
            throw new NullPointerException("cacheDirectory == null");
        }
        if (maxCacheAge < -1) {
            throw new IllegalArgumentException("maxCacheAge < -1");
        }
        if (maxNegativeCacheAge < -1) {
            throw new IllegalArgumentException("maxNegativeCacheAge < -1");
        }
        this.cacheDirectory      = cacheDirectory;
        this.maxCacheAge         = maxCacheAge;
        this.maxNegativeCacheAge = maxNegativeCacheAge;
        this.httpClient          = createHttpClient(connectTimeout, socketTimeout);
    }


    public CMDISchemaLoader(File cacheDirectory, long maxCacheAge, int connectTimeout,
            int socketTimeout) {
        this(cacheDirectory, maxCacheAge, TimeUnit.HOURS.toMillis(1), 
                connectTimeout, socketTimeout);
    }


    public CMDISchemaLoader(File cacheDirectory, long maxCacheAge) {
        this(cacheDirectory, maxCacheAge, TimeUnit.HOURS.toMillis(1), 60000, 60000);
    }


    public CMDISchemaLoader(File cacheDirectory) {
        this(cacheDirectory, DISABLE_CACHE_AGING);
    }


    public InputStream loadSchemaFile(String targetNamespace,
            String schemaLocation) throws IOException {
        if (targetNamespace == null) {
            throw new NullPointerException("targetNamespace == null");
        }
        if (schemaLocation == null) {
            throw new NullPointerException("schemaLocation == null");
        }

        logger.trace("loading schema: targetNamespace={}, location={}",
                targetNamespace, schemaLocation);
        InputStream stream = null;
        if (XMLConstants.XML_NS_URI.equals(targetNamespace)) {
            stream = loadBundledSchema(XML_XSD_RESSOURCE, schemaLocation);
        } else if (CMDI_ENEVLOPE_NS_URI.equals(targetNamespace)) {
            stream = loadBundledSchema(CMDI_ENVELOPE_XSD_RESSOURCE,
                    schemaLocation);
        }
        if (stream != null) {
            return stream;
        }

        // fall back to file cache ...
        final File cacheDataFile =
                makeFile(schemaLocation, EXTENSION_XSD);
        final File cacheErrorFile =
                makeFile(schemaLocation, EXTENSION_ERROR);

        for (;;) {
            boolean doDownload = false;

            synchronized (guard) {
                /*
                 * check, if an earlier attempt to download the schema failed.
                 */
                if (cacheErrorFile.exists()) {
                    if (isExpired(cacheErrorFile, maxNegativeCacheAge)) {
                        logger.trace("-> error file '{}' expired",
                                cacheErrorFile);
                        cacheErrorFile.delete();
                    } else {
                        throw new IOException("cached error condition detected");
                    }
                }

                if (cacheDataFile.exists()) {
                    if (isExpired(cacheDataFile, maxCacheAge)) {
                        logger.debug("cached entry for '{}' has expired",
                                schemaLocation);
                        cacheDataFile.delete();
                    } else {
                        synchronized (pending) {
                            if (!pending.contains(schemaLocation)) {
                                logger.trace("-> '{}' from file cache", schemaLocation);
                                return new FileInputStream(cacheDataFile);
                            }
                        }
                    }
                }

                synchronized (pending) {
                    if (!pending.contains(schemaLocation)) {
                        doDownload = true;
                        pending.add(schemaLocation);
                        logger.trace("pending + '{}'", schemaLocation);
                    }
                } // synchronized (pending)
            } // synchronized (guard)

            // either download in this thread of wait for pending download
            if (doDownload) {
                boolean failed = false;
                try {
                    download(cacheDataFile, schemaLocation);
                    logger.trace("downloaded schema from '{}' succesfully", schemaLocation);
                    return new FileInputStream(cacheDataFile);
                } catch (IOException e) {
                    logger.error("downloading schema from '{}' failed", schemaLocation);
                    logger.error("cause:", e);
                    failed = true;
                    throw e;
                } finally {
                    synchronized (guard) {
                        if (failed) {
                            if (cacheErrorFile.exists()) {
                                cacheErrorFile.setLastModified(
                                        System.currentTimeMillis());
                            } else {
                                cacheErrorFile.createNewFile();
                            }
                        }
                        synchronized (pending) {
                            logger.trace("pending - '{}'", schemaLocation);
                            pending.remove(schemaLocation);
                            synchronized (waiter) {
                                logger.trace("notify all waiters for downloading schema from '{}'", schemaLocation);
                                waiter.notifyAll();
                            } // synchronized (waiter)
                        }// synchronized (pending)
                    } // synchronized (guard)
                }
            } else {
                try {
                    synchronized (waiter) {
                        logger.trace("waiting for download schema from '{}'", schemaLocation);
                        waiter.wait();
                    } // synchronized (waiter)
                } catch (InterruptedException e) {
                    throw new InterruptedIOException(
                            "interrupted while waiting for download");
                }
            }
        } // for
    }


    private InputStream loadBundledSchema(String ressource,
            String schemaLocation) {
        InputStream stream = this.getClass().getResourceAsStream(ressource);
        if (stream != null) {
            logger.trace("using bundled schema for '{}'", schemaLocation);
        } else {
            logger.warn("unable to load bundled schema for '{}', " +
                    "falling back to download.", schemaLocation);
        }
        return stream;
    }
    
    
    private void download(File cacheFile, String schemaLocation)
            throws IOException {
        try {
            logger.debug("downloading schema from '{}'", schemaLocation);
            final URI uri = new URI(schemaLocation);
            final HttpGet request = new HttpGet(uri);
            try {
                logger.trace("submitting HTTP request: {}", uri.toString());
                final CloseableHttpResponse response =
                        httpClient.execute(request, new BasicHttpContext());
                try {
                    final StatusLine status = response.getStatusLine();
                    if (status.getStatusCode() == HttpStatus.SC_OK) {
                        final HttpEntity entity = response.getEntity();
                        if (entity == null) {
                            throw new IOException(
                                    "request returned no message body");
                        }

                        FileOutputStream out = null;
                        try {
                            out = new FileOutputStream(cacheFile);
                            // use exclusive lock
                            final FileLock lock = out.getChannel().lock();
                            try {
                                entity.writeTo(out);
                                out.flush();
                                out.getFD().sync();
                            } finally {
                                lock.release();
                            }
                        } finally {
                            if (out != null) {
                                out.close();
                            }
                        }
                    } else {
                        switch (status.getStatusCode()) {
                        case HttpStatus.SC_NOT_FOUND:
                            throw new IOException("not found: " + uri);
                        default:
                            throw new IOException("unexpected status: " +
                                    status.getStatusCode());
                        } // switch
                    }
                } catch (IOException e) {
                    /* delete broken cache file */
                    if (cacheFile != null) {
                        cacheFile.delete();
                    }
                    throw e;
                } finally {
                    /* make sure to release allocated resources */
                    response.close();
                }
            } finally {
                request.reset();
            }
        } catch (URISyntaxException e) {
            throw new IOException("schemaLocation uri is invalid: " +
                    schemaLocation, e);
        }
    }


    private File makeFile(String schemaLocation, String extension) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < schemaLocation.length(); i++) {
            final char c = schemaLocation.charAt(i);
            switch (c) {
            case '.':
                /* FALL-THROUGH */
            case ':':
                /* FALL-THROUGH */
            case ';':
                /* FALL-THROUGH */
            case '?':
                /* FALL-THROUGH */
            case '&':
                /* FALL-THROUGH */
            case '=':
                /* FALL-THROUGH */
            case '"':
                /* FALL-THROUGH */
            case '\'':
                /* FALL-THROUGH */
            case '/':
                /* FALL-THROUGH */
            case '\\':
                sb.append('_');
                break;
            default:
                sb.append(c);
            }
        } // for
        sb.append(".").append(extension);
        return new File(cacheDirectory, sb.toString());
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
                        return 60000;
                    }
                };

        return HttpClients.custom()
                .setUserAgent(USER_AGENT)
                .setConnectionManager(manager)
                .setDefaultSocketConfig(socketConfig)
                .setDefaultRequestConfig(requestConfig)
                .setKeepAliveStrategy(keepAliveStrategy)
                .build();
    }


    private static boolean isExpired(File file, long maxAge) {
        if (maxAge != DISABLE_CACHE_AGING) {
            return (System.currentTimeMillis() - file.lastModified()) >= maxAge;
        } else {
            return false;
        }
    }


    @Override
    protected void finalize() throws Throwable {
        httpClient.close();
    }

} // class CMDISchemaLoader
