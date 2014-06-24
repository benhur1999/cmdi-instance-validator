package eu.clarin.cmdi.validator;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.XMLConstants;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamSource;

import net.java.truevfs.access.TFile;
import net.java.truevfs.access.TFileInputStream;
import net.sf.saxon.s9api.Axis;
import net.sf.saxon.s9api.DocumentBuilder;
import net.sf.saxon.s9api.Processor;
import net.sf.saxon.s9api.QName;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.WhitespaceStrippingPolicy;
import net.sf.saxon.s9api.XPathCompiler;
import net.sf.saxon.s9api.XPathExecutable;
import net.sf.saxon.s9api.XPathSelector;
import net.sf.saxon.s9api.XdmDestination;
import net.sf.saxon.s9api.XdmItem;
import net.sf.saxon.s9api.XdmNode;
import net.sf.saxon.s9api.XdmSequenceIterator;
import net.sf.saxon.s9api.XsltCompiler;
import net.sf.saxon.s9api.XsltExecutable;
import net.sf.saxon.s9api.XsltTransformer;

import org.apache.commons.lang3.SystemUtils;
import org.apache.xerces.impl.xs.XMLSchemaLoader;
import org.apache.xerces.impl.xs.XSDDescription;
import org.apache.xerces.parsers.DOMParser;
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
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import eu.clarin.cmdi.validator.CMDIValidatorResult.Severity;
import eu.clarin.cmdi.validator.utils.LRUCache;


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
    private static final String SVRL_NAMESPACE_URI =
            "http://purl.oclc.org/dsdl/svrl";
    private static final String SVRL_NAMESPACE_PREFIX =
            "svrl";
    private static final QName SVRL_TEXT =
            new QName(SVRL_NAMESPACE_URI, "text");
    private static final QName SVRL_FAILED_ASSERT =
            new QName(SVRL_NAMESPACE_URI, "failed-assert");
    private final Processor processor;
    private final CMDISchemaLoader schemaLoader;
    private final XsltExecutable schematronValidatorExecutable;
    private final XPathExecutable xpath1;
    private final XPathExecutable xpath2;
    private final List<CMDIValidatorExtension> extensions;
    private final FileEnumerator files;
    private final CMDIValidatorHandler handler;
    private final Map<Thread, ThreadContext> contexts =
            new ConcurrentHashMap<Thread, ThreadContext>();
    private final AtomicInteger threadsProcessing = new AtomicInteger();
    private State state = State.INIT;
    private Result result = null;


    public CMDIValidator(final CMDIValidatorConfig config)
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

        /*
         * initialize Schematron validator
         */
        if (!config.isSchematronDisabled()) {
            this.schematronValidatorExecutable =
                    initSchematronValidator(config, processor);
            try {
                final XPathCompiler compiler = processor.newXPathCompiler();
                compiler.declareNamespace(SVRL_NAMESPACE_PREFIX, SVRL_NAMESPACE_URI);
                this.xpath1 = compiler.compile("//(svrl:failed-assert|svrl:successful-report)");
                this.xpath2 = compiler.compile("preceding-sibling::svrl:fired-rule/@role");
            } catch (SaxonApiException e) {
                throw new CMDIValidatorInitException(
                        "error initializing schematron validator", e);
            }
            logger.debug("Schematron validator successfully initialized");
        } else {
            this.schematronValidatorExecutable = null;
            this.xpath1                        = null;
            this.xpath2                        = null;
        }

        /*
         * initialize extensions
         */
        final List<CMDIValidatorExtension> exts = config.getExtensions();
        if (exts != null) {
            this.extensions = new LinkedList<CMDIValidatorExtension>();
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
        final TFile root = new TFile(config.getRoot());
        this.files       = new FileEnumerator(root, config.getFileFilter());
        if (config.getHandler() == null) {
            throw new NullPointerException("handler == null");
        }
        this.handler = config.getHandler();
    }


    public CMDIValidatorHandler getHandler() {
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
        } catch (CMDIValidatorException e) {
            synchronized (this) {
                state = State.DONE;
                if (result == null) {
                    result = Result.ERROR;
                }
            } // synchronized (this)
            throw e;
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
        return new CMDISchemaLoader(cacheDirectory, CMDISchemaLoader.DISABLE_CACHE_AGING);
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
        private final XML11Configuration xercesConfig;
        private final XsltTransformer schematronValidator;
        private final DocumentBuilder builder;
        private final ResultImpl result = new ResultImpl();


        private ThreadContext() {
            /*
             * initialize Xerces
             */
            final XMLEntityResolver resolver = new XMLEntityResolver() {
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

            xercesConfig = new XML11Configuration(symbols, pool);
            xercesConfig.setFeature(NAMESPACES_FEATURE_ID, true);
            xercesConfig.setFeature(VALIDATION_FEATURE_ID, true);
            xercesConfig.setFeature(SCHEMA_VALIDATION_FEATURE_ID, true);
            xercesConfig.setFeature(SCHEMA_FULL_CHECKING_FEATURE_ID, true);
            xercesConfig.setFeature(HONOUR_ALL_SCHEMA_LOCATIONS_ID, true);
            xercesConfig.setEntityResolver(resolver);
            xercesConfig.setErrorHandler(new XMLErrorHandler() {
                @Override
                public void warning(String domain, String key,
                        XMLParseException e) throws XNIException {
                    reportWarning(e.getLineNumber(),
                            e.getColumnNumber(),
                            e.getMessage(), e);
                }


                @Override
                public void error(String domain, String key,
                        XMLParseException e) throws XNIException {
                    reportError(e.getLineNumber(),
                            e.getColumnNumber(),
                            e.getMessage(), e);
                    throw e;
                }


                @Override
                public void fatalError(String domain, String key,
                        XMLParseException e) throws XNIException {
                    reportError(e.getLineNumber(),
                            e.getColumnNumber(),
                            e.getMessage(), e);
                    throw e;
                }
            });

            /*
             * initialize Saxon document builder
             */
            this.builder = processor.newDocumentBuilder();
            this.builder.setWhitespaceStrippingPolicy(
                    WhitespaceStrippingPolicy.IGNORABLE);

            /*
             * initialize schematron
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
                result.setFile(file);
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
                            extension.validate(document, result);
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
                    if (handler != null) {
                        if (result.isHighestSeverity(Severity.ERROR)) {
                            handler.onValidationFailure(result);
                        } else {
                            handler.onValidationSuccess(result);
                        }
                    }
                    result.reset();
                }
            }
        }


        private XdmNode parseInstance(InputStream stream)
                throws CMDIValidatorException {
            try {
                final DOMParser parser = new DOMParser(xercesConfig);
                try {
                    parser.parse(new InputSource(stream));
                    stream.close();

                    final Document dom = parser.getDocument();
                    if (dom == null) {
                        throw new CMDIValidatorException(
                                "parser returned no return result");
                    }
                    return builder.build(new DOMSource(dom));
                } finally {
                    parser.reset();
                }
            } catch (SAXException e) {
                logger.trace("error parsing instance", e);
                return null;
            } catch (SaxonApiException e) {
                logger.trace("error parsing instance", e);
                return null;
            } catch (IOException e) {
                final String message = (e.getMessage() != null)
                        ? e.getMessage()
                        : "input/output error";
                throw new CMDIValidatorException(message, e);
            } finally {
                /* really make sure, stream is closed */
                try {
                    stream.close();
                } catch (IOException e) {
                    /* IGNORE */
                }
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

                final XPathSelector selector = xpath1.load();
                selector.setContextItem(destination.getXdmNode());
                for (XdmItem item : selector) {
                    final XdmNode node = (XdmNode) item;
                    final XdmNode text = getFirstChild(node, SVRL_TEXT);
                    String msg = (text != null)
                            ? text.getStringValue().trim()
                            : null;
                    if (SVRL_FAILED_ASSERT.equals(node.getNodeName())) {
                        final XPathSelector selector2 = xpath2.load();
                        String role = null;
                        selector2.setContextItem(node);
                        XdmItem evaluateSingle = selector2.evaluateSingle();
                        if (evaluateSingle != null) {
                            role = evaluateSingle.getStringValue().trim();
                        }
                        if ("warning".equalsIgnoreCase(role)) {
                            result.reportWarning(-1, -1, msg);
                        } else {
                            result.reportError(-1, -1, msg);
                        }
                    } else {
                        result.reportInfo(-1, -1, msg);
                    }
                }
            } catch (SaxonApiException e) {
                throw new CMDIValidatorException(
                        "error performing schematron validation", e);
            }
        }


        private XdmNode getFirstChild(XdmNode parent, QName name) {
            XdmSequenceIterator i = parent.axisIterator(Axis.CHILD, name);
            if (i.hasNext()) {
                return (XdmNode) i.next();
            } else {
                return null;
            }
        }


        private void reportWarning(int line, int col, String message,
                Throwable cause) {
            logger.debug("reporting warning: [{}:{}]: {}", line, col, message);
            if (result != null) {
                result.reportWarning(line, col, message, cause);
            }
        }


        private void reportError(int line, int col, String message,
                Throwable cause) {
            logger.debug("reporting error: [{}:{}]: {}", line, col, message);
            if (result != null) {
                result.reportError(line, col, message, cause);
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


    private static final class ResultImpl implements
            CMDIValidatorWriteableResult {
        private final List<Message> messages = new LinkedList<Message>();
        private File file;
        private Severity highestSeverity;


        private ResultImpl() {
            reset();
        }


        @Override
        public File getFile() {
            return file;
        }


        @Override
        public Severity getHighestSeverity() {
            return highestSeverity;
        }


        @Override
        public boolean isHighestSeverity(Severity severity) {
            if (severity == null) {
                throw new NullPointerException("severity == null");
            }
            return this.highestSeverity.equals(severity);
        }


        @Override
        public List<Message> getMessages() {
            if (messages.isEmpty()) {
                return Collections.emptyList();
            } else {
                return Collections.unmodifiableList(messages);
            }
        }


        @Override
        public Message getFirstMessage() {
            return messages.isEmpty() ? null : messages.get(0);
        }


        @Override
        public Message getFirstMessage(Severity severity) {
            if (severity == null) {
                throw new NullPointerException("severity == null");
            }

            if (!messages.isEmpty()) {
                for (Message msg : messages) {
                    if (severity.equals(msg.getSeverity())) {
                        return msg;
                    }
                }
            }
            return null;
        }


        @Override
        public int getMessageCount() {
            return messages.size();
        }


        @Override
        public int getMessageCount(Severity severity) {
            if (severity == null) {
                throw new NullPointerException("severity == null");
            }

            int count = 0;
            if (!messages.isEmpty()) {
                for (Message msg : messages) {
                    if (severity.equals(msg.getSeverity())) {
                        count++;
                    }
                }
            }
            return count;
        }


        @Override
        public void reportInfo(int line, int col, String message) {
            reportInfo(line, col, message, null);
        }


        @Override
        public void reportInfo(int line, int col, String message,
                Throwable cause) {
            messages.add(new MessageImpl(Severity.INFO, line, col, message,
                    cause));
        }


        @Override
        public void reportWarning(int line, int col, String message) {
            reportWarning(line, col, message, null);
        }


        @Override
        public void reportWarning(int line, int col, String message,
                Throwable cause) {
            switch (highestSeverity) {
            case WARNING:
                /* FALL-THROUGH */
            case ERROR:
                break;
            default:
                highestSeverity = Severity.WARNING;
            }
            messages.add(new MessageImpl(Severity.WARNING, line, col, message,
                    cause));
        }


        @Override
        public void reportError(int line, int col, String message) {
            reportError(line, col, message, null);
        }


        @Override
        public void reportError(int line, int col, String message,
                Throwable cause) {
            switch (highestSeverity) {
            case ERROR:
                break;
            default:
                highestSeverity = Severity.ERROR;
            }
            messages.add(new MessageImpl(Severity.ERROR, line, col, message,
                    cause));
        }


        private void setFile(File file) {
            this.file = file;
        }


        private void reset() {
            this.messages.clear();
            this.file = null;
            this.highestSeverity = Severity.INFO;
        }
    } // class ResultImpl


    private static final class MessageImpl implements
            CMDIValidatorResult.Message {
        private final Severity severity;
        private final int line;
        private final int col;
        private final String message;
        private final Throwable cause;


        private MessageImpl(Severity severity, int line, int col,
                String message, Throwable cause) {
            this.severity = severity;
            this.line     = line;
            this.col      = col;
            this.message  = message;
            this.cause    = cause;
        }


        @Override
        public Severity getSeverity() {
            return severity;
        }


        @Override
        public int getLineNumber() {
            return line;
        }


        @Override
        public int getColumnNumber() {
            return col;
        }


        @Override
        public String getMessage() {
            return message;
        }


        @Override
        public Throwable getCause() {
            return cause;
        }
    } // class MessageImpl

} // class CMDIValidator
