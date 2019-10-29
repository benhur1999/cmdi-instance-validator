/**
 * This software is copyright (c) 2014 by
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
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.XMLConstants;
import javax.xml.transform.ErrorListener;
import javax.xml.transform.TransformerException;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamSource;

import net.java.truevfs.access.TFile;
import net.java.truevfs.access.TFileInputStream;
import net.sf.saxon.Configuration;
import net.sf.saxon.s9api.DocumentBuilder;
import net.sf.saxon.s9api.Processor;
import net.sf.saxon.s9api.QName;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.WhitespaceStrippingPolicy;
import net.sf.saxon.s9api.XPathCompiler;
import net.sf.saxon.s9api.XPathSelector;
import net.sf.saxon.s9api.XQueryCompiler;
import net.sf.saxon.s9api.XQueryEvaluator;
import net.sf.saxon.s9api.XQueryExecutable;
import net.sf.saxon.s9api.XdmDestination;
import net.sf.saxon.s9api.XdmItem;
import net.sf.saxon.s9api.XdmNode;
import net.sf.saxon.s9api.XsltCompiler;
import net.sf.saxon.s9api.XsltExecutable;
import net.sf.saxon.s9api.XsltTransformer;
import net.sf.saxon.trans.UncheckedXPathException;

import org.apache.commons.lang3.SystemUtils;
import org.apache.xerces.impl.xs.XMLSchemaLoader;
import org.apache.xerces.impl.xs.XSDDescription;
import org.apache.xerces.parsers.SAXParser;
import org.apache.xerces.parsers.XML11Configuration;
import org.apache.xerces.util.SymbolTable;
import org.apache.xerces.xni.XMLResourceIdentifier;
import org.apache.xerces.xni.XNIException;
import org.apache.xerces.xni.grammars.Grammar;
import org.apache.xerces.xni.grammars.XMLGrammarDescription;
import org.apache.xerces.xni.grammars.XMLGrammarPool;
import org.apache.xerces.xni.parser.XMLEntityResolver;
import org.apache.xerces.xni.parser.XMLErrorHandler;
import org.apache.xerces.xni.parser.XMLInputSource;
import org.apache.xerces.xni.parser.XMLParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import eu.clarin.cmdi.validator.utils.LRUCache;
import eu.clarin.cmdi.validator.utils.LocationUtils;


public final class CMDIValidator {
    public enum Result {
        OK, ABORTED, ERROR
    }
    private static final Logger logger =
            LoggerFactory.getLogger(CMDIValidator.class);
    private static final String SCHEMATATRON_STAGE_1 =
            "/schematron/iso_dsdl_include.xsl";
    private static final String SCHEMATATRON_STAGE_2 =
            "/schematron/iso_abstract_expand.xsl";
    private static final String SCHEMATATRON_STAGE_3 =
            "/schematron/iso_svrl_for_xslt2.xsl";
    private static final String ANALYZE_SVRL =
            "/analyze-svrl.xq";
    private static final String DEFAULT_SCHEMATRON_SCHEMA =
            "/default.sch";
    private static final String XML_SCHEMA_LOCATION =
            "http://www.w3.org/2001/xml.xsd";
    private static final String XML_SCHEMA_GRAMMAR_TYPE =
            "http://www.w3.org/2001/XMLSchema";
    private static final String GRAMMAR_POOL =
            "http://apache.org/xml/properties/internal/grammar-pool";
    private static final String NAMESPACES_FEATURE_ID =
            "http://xml.org/sax/features/namespaces";
    private static final String VALIDATION_FEATURE_ID =
            "http://xml.org/sax/features/validation";
    private static final String SCHEMA_VALIDATION_FEATURE_ID =
            "http://apache.org/xml/features/validation/schema";
    private static final String SCHEMA_FULL_CHECKING_FEATURE_ID =
            "http://apache.org/xml/features/validation/schema-full-checking";
    private static final String HONOUR_ALL_SCHEMA_LOCATIONS_ID =
            "http://apache.org/xml/features/honour-all-schemaLocations";
    private static final int INITAL_SYMBOL_TABLE_SIZE = 16141;
    private static final QName SVRL_S = new QName("s");
    private static final QName SVRL_L = new QName("l");
    private final Processor processor;
    private final CMDISchemaLoader schemaLoader;
    private final XsltExecutable schematronValidatorExecutable;
    private final XQueryExecutable analyzeSchematronReport;
    private final List<CMDIValidatorExtension> extensions;
    private final FileEnumerator files;
    private final CMDIValidationHandler handler;
    private final Map<Thread, ThreadContext> contexts =
            new ConcurrentHashMap<Thread, ThreadContext>();
    private final AtomicInteger threadsProcessing = new AtomicInteger();
    private State state = State.INIT;
    private Result result = null;

    
    public CMDIValidator(final CMDIValidatorConfig config)
            throws CMDIValidatorInitException {
        this(config, config.getRoot(), config.getHandler());
    }

    public CMDIValidator(final CMDIValidatorConfig config, final File src, CMDIValidationHandler handler)
            throws CMDIValidatorInitException {
        if (config == null) {
            throw new NullPointerException("config == null");
        }

        /*
         * initialize custom schema loader
         */
        if (config.getSchemaLoader() != null) {
            logger.debug("using supplied schema loader ...");
            this.schemaLoader = config.getSchemaLoader();
        } else {
            logger.debug("initializing schema loader ...");
            this.schemaLoader = initSchemaLoader(config);
        }

        /*
         * initialize Saxon processor
         */
        logger.debug("initializing Saxon ...");
        this.processor = new Processor(true);
        final Configuration saxonConfig =
                this.processor.getUnderlyingConfiguration();
        saxonConfig.setErrorListener(new ErrorListener() {
            @Override
            public void warning(TransformerException exception)
                    throws TransformerException {
                throw exception;
            }


            @Override
            public void fatalError(TransformerException exception)
                    throws TransformerException {
                throw exception;
            }


            @Override
            public void error(TransformerException exception)
                    throws TransformerException {
                throw exception;
            }
        });


        /*
         * initialize Schematron validator
         */
        if (!config.isSchematronDisabled()) {
            this.schematronValidatorExecutable =
                    initSchematronValidator(config, processor);
            InputStream stream = null;
            try {
                stream = getClass().getResourceAsStream(ANALYZE_SVRL);
                final XQueryCompiler compiler = processor.newXQueryCompiler();
                this.analyzeSchematronReport  = compiler.compile(stream);
            } catch (SaxonApiException e) {
                throw new CMDIValidatorInitException(
                        "error initializing schematron validator", e);
            } finally {
                if (stream != null) {
                    try {
                        stream.close();
                    } catch (IOException e) {
                        /* IGNORE */
                    }
                }
            }
            logger.debug("Schematron validator successfully initialized");
        } else {
            this.schematronValidatorExecutable = null;
            this.analyzeSchematronReport       = null;
        }

        /*
         * initialize extensions
         */
        final List<CMDIValidatorExtension> exts = config.getExtensions();
        if (exts != null) {
            this.extensions =
                    new ArrayList<CMDIValidatorExtension>(exts.size());
            for (CMDIValidatorExtension extension : exts) {
                extension.initalize(processor);
                extensions.add(extension);
            }
        } else {
            this.extensions = null;
        }

        /*
         * other stuff
         */
        final TFile root = new TFile(src);
        this.files       = new FileEnumerator(root, config.getFileFilter());
        if (config.getHandler() == null) {
            throw new NullPointerException("handler == null");
        }
        this.handler = handler;
    }


    public CMDIValidationHandler getHandler() {
        return handler;
    }


    public void abort() {
        synchronized (this) {
            if ((state == State.INIT) || (state == State.RUN)) {
                state = State.DONE;
                files.flush();
                if (result == null) {
                    result = Result.ABORTED;
                }
            }
        } // synchronized (this)
    }


    boolean processOneFile() throws CMDIValidatorException {
        try {
            TFile file = null;
            boolean done;

            threadsProcessing.incrementAndGet();

            synchronized (this) {
                switch (state) {
                case INIT:
                    try {
                        state = State.RUN;
                        handler.onJobStarted();
                    } catch (CMDIValidatorException e) {
                        state = State.DONE;
                        throw e;
                    }
                    /* FALL-THROUGH */
                case RUN:
                    file = files.nextFile();
                    if (files.isEmpty() && (state == State.RUN)) {
                        state = State.DONE;
                    }
                    break;
                default:
                    // ignore
                }

                done = (state == State.DONE);
            } // synchronized (this)

            if (file != null) {
                ThreadContext context = contexts.get(Thread.currentThread());
                if (context == null) {
                    context = new ThreadContext();
                    contexts.put(Thread.currentThread(), context);
                }
                context.validate(file);
            }

            return done;
        } catch (Throwable e) {
            synchronized (this) {
                state = State.DONE;
                if (result == null) {
                    result = Result.ERROR;
                }
            } // synchronized (this)
            if (e instanceof CMDIValidatorException) {
                throw (CMDIValidatorException) e;
            } else {
                throw new CMDIValidatorException(
                        "an unexpected error occurred", e);
            }
        } finally {
            if (threadsProcessing.decrementAndGet() <= 0) {
                synchronized (this) {
                    if (state == State.DONE) {
                        state = State.FINI;
                        if (result == null) {
                            result = Result.OK;
                        }

                        // notify handler
                        handler.onJobFinished(result);
                    }
                } // synchronized (this)
            }
        }
    }


    private static CMDISchemaLoader initSchemaLoader(
            final CMDIValidatorConfig config) throws CMDIValidatorInitException {
        int connectTimeout = config.getConnectTimeout();
        int socketTimeout = config.getSocketTimeout();
        File cacheDirectory = config.getSchemaCacheDirectory();
        if (cacheDirectory == null) {
            if (SystemUtils.IS_OS_WINDOWS &&
                    (SystemUtils.JAVA_IO_TMPDIR != null)) {
                cacheDirectory =
                        new File(SystemUtils.JAVA_IO_TMPDIR, "cmdi-validator");
            } else if (SystemUtils.IS_OS_UNIX &&
                    (SystemUtils.USER_HOME != null)) {
                cacheDirectory =
                        new File(SystemUtils.USER_HOME, ".cmdi-validator");
            }
            if (cacheDirectory != null) {
                if (!cacheDirectory.exists()) {
                    if (!cacheDirectory.mkdir()) {
                        throw new CMDIValidatorInitException(
                                "cannot create cache directory: " +
                                        cacheDirectory);
                    }
                }
            } else {
                if (SystemUtils.JAVA_IO_TMPDIR == null) {
                    throw new CMDIValidatorInitException(
                            "cannot determine temporary directory");
                }
                cacheDirectory = new File(SystemUtils.JAVA_IO_TMPDIR);
            }
        } else {
            if (!cacheDirectory.isDirectory()) {
                throw new CMDIValidatorInitException(
                        "supplied cache dircetory '" +
                                cacheDirectory.getAbsolutePath() +
                                "' is not a directory");
            }
            if (!cacheDirectory.canWrite()) {
                throw new CMDIValidatorInitException("cache dircetory '" +
                        cacheDirectory.getAbsolutePath() + "' is not writable");
            }
        }
        return new CMDISchemaLoader(cacheDirectory, CMDISchemaLoader.DISABLE_CACHE_AGING, connectTimeout, socketTimeout);
    }


    private static XsltExecutable initSchematronValidator(
            final CMDIValidatorConfig config, final Processor processor)
            throws CMDIValidatorInitException {
        URL schema = null;
        File schematronSchemaFile = config.getSchematronSchemaFile();
        if (schematronSchemaFile != null) {
            if (!schematronSchemaFile.exists()) {
                throw new CMDIValidatorInitException("file '" +
                        schematronSchemaFile.getAbsolutePath() +
                        "' does not exist");
            }
            if (!schematronSchemaFile.isFile()) {
                throw new CMDIValidatorInitException("file '" +
                        schematronSchemaFile.getAbsolutePath() +
                        "' is not a regular file");
            }
            if (!schematronSchemaFile.canRead()) {
                throw new CMDIValidatorInitException("file '" +
                        schematronSchemaFile.getAbsolutePath() +
                        "' cannot be read");
            }
            try {
                schema = schematronSchemaFile.toURI().toURL();
            } catch (MalformedURLException e) {
                throw new CMDIValidatorInitException("internal error", e);
            }
        } else {
            schema = CMDIValidator.class.getResource(DEFAULT_SCHEMATRON_SCHEMA);
            if (schema == null) {
                throw new CMDIValidatorInitException(
                        "cannot locate bundled Schematron schema: " +
                                DEFAULT_SCHEMATRON_SCHEMA);
            }
        }
        final XsltCompiler compiler = processor.newXsltCompiler();
        XsltTransformer stage1 =
                loadStylesheet(processor, compiler, SCHEMATATRON_STAGE_1);
        XsltTransformer stage2 =
                loadStylesheet(processor, compiler, SCHEMATATRON_STAGE_2);
        XsltTransformer stage3 =
                loadStylesheet(processor, compiler, SCHEMATATRON_STAGE_3);
        try {
            XdmDestination destination = new XdmDestination();
            stage1.setSource(new StreamSource(schema.toExternalForm()));
            stage1.setDestination(stage2);
            stage2.setDestination(stage3);
            stage3.setDestination(destination);
            stage1.transform();
            return compiler.compile(destination.getXdmNode().asSource());
        } catch (SaxonApiException e) {
            throw new CMDIValidatorInitException(
                    "error compiling schematron rules", e);
        }
    }


    private static XsltTransformer loadStylesheet(final Processor processor,
            final XsltCompiler compiler, final String name)
            throws CMDIValidatorInitException {
        try {
            logger.debug("loading stylesheet '{}'", name);
            final URL uri = CMDIValidator.class.getResource(name);
            if (uri != null) {
                DocumentBuilder builder = processor.newDocumentBuilder();
                XdmNode source =
                        builder.build(new StreamSource(uri.toExternalForm()));
                XsltExecutable stylesheet = compiler.compile(source.asSource());
                return stylesheet.load();
            } else {
                throw new CMDIValidatorInitException("cannot find resource '" +
                        name + "'");
            }
        } catch (SaxonApiException e) {
            throw new CMDIValidatorInitException(
                    "error loading schematron stylesheet '" + name + "'", e);
        }
    }


    private enum State {
        INIT, RUN, DONE, FINI;
    }


    private final class ThreadContext {
        private final SAXParser parser;
        private final XsltTransformer schematronValidator;
        private final DocumentBuilder builder;
        private CMDIWriteableValidationReport report;


        private ThreadContext() {
            /*
             * initialize Xerces
             */
            XMLEntityResolver resolver = new XMLEntityResolver() {
                @Override
                public XMLInputSource resolveEntity(
                        XMLResourceIdentifier identifier) throws XNIException,
                        IOException {
                    final String uri = identifier.getExpandedSystemId();
                    if (uri == null) {
                        throw new IOException(
                                "bad schema location for namespace '" +
                                        identifier.getNamespace() + "': " +
                                        identifier.getLiteralSystemId());
                    }
                    InputStream stream = schemaLoader.loadSchemaFile(
                            identifier.getNamespace(), uri);
                    return new XMLInputSource(null, null, null, stream, null);
                }
            };

            SymbolTable symbols = new SymbolTable(INITAL_SYMBOL_TABLE_SIZE);
            ShadowCacheXMLGrammarPool pool = new ShadowCacheXMLGrammarPool(8);

            XMLSchemaLoader xsdLoader = new XMLSchemaLoader(symbols);
            xsdLoader.setParameter(GRAMMAR_POOL, pool);
            xsdLoader.setEntityResolver(resolver);
            xsdLoader.setErrorHandler(new XMLErrorHandler() {
                @Override
                public void warning(String domain, String key,
                        XMLParseException e) throws XNIException {
                    /* ignore warnings */
                }


                @Override
                public void error(String domain, String key, XMLParseException e)
                        throws XNIException {
                    throw e;
                }


                @Override
                public void fatalError(String domain, String key,
                        XMLParseException e) throws XNIException {
                    throw e;
                }
            });

            try {
                InputStream stream = null;
                try {
                    stream = schemaLoader.loadSchemaFile(
                            XMLConstants.XML_NS_URI, XML_SCHEMA_LOCATION);
                    Grammar grammar = xsdLoader.loadGrammar(new XMLInputSource(
                            XMLConstants.XML_NS_URI, XML_SCHEMA_LOCATION, null,
                            stream, null));
                    if (grammar != null) {
                        pool.cacheGrammars(XML_SCHEMA_GRAMMAR_TYPE,
                                new Grammar[] { grammar });
                    }
                    pool.lockPool();
                } finally {
                    if (stream != null) {
                        stream.close();
                    }
                }
            } catch (IOException e) {
                /*
                 * Should never happen
                 */
                logger.error("error initaliting thread context", e);
            }

            XML11Configuration xercesConfig =
                    new XML11Configuration(symbols, pool);
            xercesConfig.setFeature(NAMESPACES_FEATURE_ID, true);
            xercesConfig.setFeature(VALIDATION_FEATURE_ID, true);
            xercesConfig.setFeature(SCHEMA_VALIDATION_FEATURE_ID, true);
            xercesConfig.setFeature(SCHEMA_FULL_CHECKING_FEATURE_ID, true);
            xercesConfig.setFeature(HONOUR_ALL_SCHEMA_LOCATIONS_ID, true);
            xercesConfig.setEntityResolver(resolver);

            /*
             * create a reusable parser and also add an error handler.
             * We cannot use a global error handler in xerces config, because
             * Saxon ignores and overwrites it ...
             */
            this.parser = new SAXParser(xercesConfig);
            this.parser.setErrorHandler(new ErrorHandler() {
                @Override
                public void warning(SAXParseException e) throws SAXException {
                    reportWarning(e.getLineNumber(),
                            e.getColumnNumber(),
                            e.getMessage(),
                            e);
                }

                @Override
                public void error(SAXParseException e) throws SAXException {
                    reportError(e.getLineNumber(),
                            e.getColumnNumber(),
                            e.getMessage(),
                            e);
                    throw e;
                }

                @Override
                public void fatalError(SAXParseException e) throws SAXException {
                    reportError(e.getLineNumber(),
                            e.getColumnNumber(),
                            e.getMessage(),
                            e);
                    throw e;
                }
            });

            /*
             * initialize and configure Saxon document builder
             */
            this.builder = processor.newDocumentBuilder();
            this.builder.setWhitespaceStrippingPolicy(
                    WhitespaceStrippingPolicy.IGNORABLE);
            this.builder.setLineNumbering(true);
            /*
             * even though, we need to perform Schema validation, tell
             * Saxon to enable DTD validation. Otherwise, it will
             * not validate at all ... :/
             */
            this.builder.setDTDValidation(true);

            /*
             * initialize Schematron validator
             */
            if (schematronValidatorExecutable != null) {
                this.schematronValidator = schematronValidatorExecutable.load();
            } else {
                this.schematronValidator = null;
            }
        }


        private void validate(final TFile file) throws CMDIValidatorException {
            TFileInputStream stream = null;
            try {

                /*
                 * step 0: prepare
                 */
                logger.debug("validating file '{}' ({} bytes)",
                        file, file.length());
                report = new CMDIWriteableValidatonReportImpl();
                report.setFile(file);
                stream = new TFileInputStream(file);

                /*
                 * step 1: parse document and perform schema validation
                 */
                final XdmNode document = parseInstance(stream);

                if (document != null) {
                    /*
                     * step 2: perform Schematron validation
                     */
                    if (schematronValidator != null) {
                        validateSchematron(document);
                    }

                    /*
                     * step 3: run extensions, if any
                     */
                    if (extensions != null) {
                        for (CMDIValidatorExtension extension : extensions) {
                            extension.validate(document, report);
                        }
                    }
                }
            } catch (IOException e) {
                throw new CMDIValidatorException(
                        "error reading file '" + file + "'", e);
            } catch (CMDIValidatorException e) {
                throw e;
            } finally {
                try {
                    if (stream != null) {
                        stream.close();
                    }
                } catch (IOException e) {
                    throw new CMDIValidatorException(
                            "error closing file '" + file + "'", e);
                } finally {
                    if ((report != null) && (handler != null)) {
                        try {
                            handler.onValidationReport(report);
                        } finally {
                            report = null;
                        }
                    }
                }
            }
        }


        private XdmNode parseInstance(InputStream stream)
                throws CMDIValidatorException {
            try {
                try {
                    final SAXSource source =
                            new SAXSource(parser, new InputSource(stream));
                    return builder.build(source);
                } finally {
                    /* recycle parser */
                    try {
                        parser.reset();
                    } catch (XNIException e) {
                        throw new CMDIValidatorException(
                                "error resetting parser", e);
                    } finally {
                        /* really make sure, stream is closed */
                        stream.close();
                    }
                }
            } catch (SaxonApiException e) {
                logger.trace("error parsing instance", e);
                return null;
            } catch (UncheckedXPathException e) {
                logger.trace("error parsing instance", e);
                return null;
            } catch (IOException e) {
                final String message = (e.getMessage() != null)
                        ? e.getMessage()
                        : "input/output error";
                throw new CMDIValidatorException(message, e);
            }
        }


        private void validateSchematron(XdmNode document)
                throws CMDIValidatorException {
            try {
                logger.trace("performing schematron validation ...");
                schematronValidator.setSource(document.asSource());
                final XdmDestination destination = new XdmDestination();
                schematronValidator.setDestination(destination);
                schematronValidator.transform();

                final XdmNode svrlDocument = destination.getXdmNode();
                if (svrlDocument != null) {
                    XPathCompiler xpathCompiler = null;
                    final XQueryEvaluator evaluator =
                            analyzeSchematronReport.load();
                    evaluator.setContextItem(svrlDocument);
                    for (final XdmItem item : evaluator) {
                        /* lazy initialize XPath compiler */
                        if (xpathCompiler == null) {
                            xpathCompiler = processor.newXPathCompiler();
                            xpathCompiler.setCaching(true);
                        }
                        final XdmNode node = (XdmNode) item;
                        final String s =
                                nullSafeTrim(node.getAttributeValue(SVRL_S));
                        final String l =
                                nullSafeTrim(node.getAttributeValue(SVRL_L));
                        final String m =
                                nullSafeTrim(node.getStringValue());
                        int line   = -1;
                        int column = -1;
                        if (l != null) {
                            XPathSelector xs = xpathCompiler.compile(l).load();
                            xs.setContextItem(document);
                            XdmItem n = xs.evaluateSingle();
                            line = LocationUtils.getLineNumber(n);
                            column = LocationUtils.getColumnNumber(n);
                        }
                        if ("I".equals(s)) {
                            report.reportInfo(line, column, m);
                        } else if ("W".equals(s)) {
                            report.reportWarning(line, column, m);
                        } else {
                            report.reportError(line, column, m);
                        }
                    } // for
                    if (xpathCompiler != null) {
                        xpathCompiler.setCaching(false);
                        xpathCompiler = null;
                    }
                }
            } catch (SaxonApiException e) {
                throw new CMDIValidatorException(
                        "error performing schematron validation", e);
            }
        }


        private String nullSafeTrim(String s) {
            if (s != null) {
                s = s.trim();
                if (s.isEmpty()) {
                    s = null;
                }
            }
            return s;
        }


        private void reportWarning(int line, int col, String message,
                Throwable cause) {
            logger.debug("reporting warning: [{}:{}]: {}", line, col, message);
            if (report != null) {
                report.reportWarning(line, col, message, cause);
            }
        }


        private void reportError(int line, int col, String message,
                Throwable cause) {
            logger.debug("reporting error: [{}:{}]: {}", line, col, message);
            if (report != null) {
                report.reportError(line, col, message, cause);
            }
        }
    }


    private static final class FileEnumerator {
        private final class FileList {
            private final TFile[] fileList;
            private int idx = 0;


            private FileList(TFile[] fileList) {
                this.fileList = fileList;
            }


            private TFile nextFile() {
                while (idx < fileList.length) {
                    final TFile file = fileList[idx++];
                    if (file.isDirectory()) {
                        return file;
                    }
                    if ((filter != null) && !filter.accept(file)) {
                        continue;
                    }
                    return file;
                } // while
                return null;
            }

            private int size() {
                return (fileList.length - idx);
            }
        }
        private final FileFilter filter;
        private final LinkedList<FileList> stack =
                new LinkedList<FileList>();


        FileEnumerator(TFile root, FileFilter filter) {
            if (root == null) {
                throw new NullPointerException("root == null");
            }
            if (root.isDirectory()) {
                pushDirectory(root);
            } else {
                stack.add(new FileList(new TFile[] { root }));
            }
            this.filter = filter;
        }


        boolean isEmpty() {
            return stack.isEmpty();
        }


        TFile nextFile() {
            for (;;) {
                if (stack.isEmpty()) {
                    break;
                }
                final FileList list = stack.peek();
                final TFile file = list.nextFile();
                if ((list.size() == 0) || (file == null)) {
                    stack.pop();
                    if (file == null) {
                        continue;
                    }
                }
                if (file.isDirectory()) {
                    pushDirectory(file);
                    continue;
                }
                return file;
            }
            return null;
        }


        void flush() {
            stack.clear();
        }


        private void pushDirectory(TFile directory) {
            final TFile[] files = directory.listFiles();
            if ((files != null) && (files.length > 0)) {
                stack.push(new FileList(files));
            }
        }

    } // class FileEnumerator


    private static final class ShadowCacheXMLGrammarPool implements
            XMLGrammarPool {
        private final Set<Grammar> cache =
                new LinkedHashSet<Grammar>();
        private final Map<String, Grammar> shadowCache;
        private boolean locked = false;


        private ShadowCacheXMLGrammarPool(int shadowCacheSize) {
            this.shadowCache = new LRUCache<String, Grammar>(shadowCacheSize);
        }


        @Override
        public Grammar[] retrieveInitialGrammarSet(String grammarType) {
            if (XML_SCHEMA_GRAMMAR_TYPE.equals(grammarType) &&
                    !cache.isEmpty()) {
                final Grammar[] result = new Grammar[cache.size()];
                return cache.toArray(result);
            } else {
                return null;
            }
        }


        @Override
        public Grammar retrieveGrammar(XMLGrammarDescription d) {
            logger.trace("search for grammar: {} / {} / {} / {}",
                    d.getNamespace(),
                    d.getLiteralSystemId(),
                    d.getExpandedSystemId(),
                    d.getBaseSystemId());
            if ((d.getNamespace() == null) || !(d instanceof XSDDescription)) {
                logger.trace("-> miss (invalid arguments supplied by caller)");
                return null;
            }

            final XSDDescription desc = (XSDDescription) d;
            final String namespace = desc.getNamespace();
            Grammar result = findGrammerFromCache(desc);
            if (result != null) {
                logger.trace("-> match from cache: {} -> {} / {}",
                        namespace,
                        desc.getNamespace(),
                        desc.getLiteralSystemId());
                return result;
            }

            String locationHint = null;
            if (desc.getLocationHints() != null) {
                String[] h = desc.getLocationHints();
                if (h.length > 0) {
                    locationHint = h[0];
                }
                logger.trace("-> hint: {}", locationHint);
            } else if (desc.getLiteralSystemId() != null) {
                locationHint = desc.getLiteralSystemId();
            }

            if (locationHint != null) {
                Grammar grammar = shadowCache.get(locationHint);
                if (grammar != null) {
                    logger.trace("-> match from shadow cache: {} -> {}",
                            grammar.getGrammarDescription().getNamespace(),
                            locationHint);
                    return grammar;
                }
            }
            logger.trace("-> miss");
            return null;
        }


        @Override
        public void lockPool() {
            locked = true;
        }


        @Override
        public void unlockPool() {
            locked = false;
        }


        @Override
        public void clear() {
            if (!locked) {
                cache.clear();
            }
        }


        @Override
        public void cacheGrammars(String grammarType, Grammar[] grammars) {
            if (XML_SCHEMA_GRAMMAR_TYPE.equals(grammarType) &&
                    (grammars != null) &&
                    (grammars.length > 0)) {
                for (Grammar grammar : grammars) {
                    final XMLGrammarDescription gd =
                            grammar.getGrammarDescription();
                    if (findGrammerFromCache(gd) == null) {
                        if (!locked) {
                            logger.trace("cached grammar: {} / {}",
                                    gd.getNamespace(),
                                    gd.getLiteralSystemId());
                            cache.add(grammar);
                        } else {
                            final String literalSystemId =
                                        gd.getLiteralSystemId();
                            if (!shadowCache.containsKey(literalSystemId)) {
                                logger.trace("shadow cached grammar: {} / {}",
                                        gd.getNamespace(),
                                        gd.getLiteralSystemId());
                                shadowCache.put(literalSystemId, grammar);
                            }
                        }
                    }
                } // for
            }
        }


        private Grammar findGrammerFromCache(XMLGrammarDescription desc) {
            if (!cache.isEmpty()) {
                for (Grammar grammar : cache) {
                    final XMLGrammarDescription gd =
                            grammar.getGrammarDescription();
                    if (gd.getNamespace().equals(desc.getNamespace())) {
                        return grammar;
                    }
                }
            }
            return null;
        }
    }  // class ShadowCacheGrammarPool

} // class CMDIValidator
