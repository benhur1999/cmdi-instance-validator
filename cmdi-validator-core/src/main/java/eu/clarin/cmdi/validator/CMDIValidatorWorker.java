package eu.clarin.cmdi.validator;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.XMLConstants;
import javax.xml.transform.sax.SAXSource;

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
import net.java.truevfs.access.TFile;
import net.java.truevfs.access.TFileInputStream;
import net.sf.saxon.s9api.DocumentBuilder;
import net.sf.saxon.s9api.Processor;
import net.sf.saxon.s9api.QName;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.WhitespaceStrippingPolicy;
import net.sf.saxon.s9api.XPathCompiler;
import net.sf.saxon.s9api.XPathSelector;
import net.sf.saxon.s9api.XQueryEvaluator;
import net.sf.saxon.s9api.XQueryExecutable;
import net.sf.saxon.s9api.XdmDestination;
import net.sf.saxon.s9api.XdmItem;
import net.sf.saxon.s9api.XdmNode;
import net.sf.saxon.s9api.XsltExecutable;
import net.sf.saxon.s9api.XsltTransformer;
import net.sf.saxon.trans.UncheckedXPathException;


public class CMDIValidatorWorker {
    private static final Logger logger =
            LoggerFactory.getLogger(CMDIValidatorWorker.class);
    private static final int INITAL_SYMBOL_TABLE_SIZE = 16141;
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
    private static final QName SVRL_S = new QName("s");
    private static final QName SVRL_L = new QName("l");
    private final Processor processor;
    private final XQueryExecutable analyzeSchematronReport;
    private final List<CMDIValidatorExtension> extensions;
    private final long maxFileSize;
    private final SAXParser parser;
    private final DocumentBuilder builder;
    private final XsltTransformer schematronValidator;
    private final CMDIValidatonReportBuilder reportBuilder =
            new CMDIValidatonReportBuilder();


    public CMDIValidatorWorker(final CMDIValidatorConfig config,
            final CMDISchemaLoader schemaLoader,
            final Processor processor,
            final XsltExecutable schematronValidatorExecutable,
            final XQueryExecutable analyzeSchematronReport,
            final List<CMDIValidatorExtension> extensions,
            final long maxFileSize) {
        this.processor = processor;
        this.analyzeSchematronReport = analyzeSchematronReport;
        this.extensions = extensions;
        this.maxFileSize = maxFileSize;

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
    
    
    public void validate(final TFile file,
            final CMDIValidationReportSink reportSink)
            throws CMDIValidatorException {
        try {
            reportBuilder.setFile(file);
            if ((maxFileSize > 0) && (file.length() > maxFileSize)) {
                logger.debug("skipping file '{}' ({} bytes)",
                        file, file.length());
                reportBuilder.setFileSkipped();
            } else {
                TFileInputStream stream = null;
                try {
                    logger.debug("validating file '{}' ({} bytes)", file,
                            file.length());

                    /*
                     * step 0: prepare
                     */
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
                                extension.validate(document, reportBuilder);
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
                    }
                }
            }
        } finally {
            if (reportSink != null) {
                try {
                    reportSink.postReport(reportBuilder.build());
                } catch (CMDIValidatorException e) {
                    throw e;
                } catch (Throwable e) {
                    throw new CMDIValidatorException(
                            "error posting validation report", e);
                }
            }
            reportBuilder.reset();
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
                        reportBuilder.reportInfo(line, column, m);
                    } else if ("W".equals(s)) {
                        reportBuilder.reportWarning(line, column, m);
                    } else {
                        reportBuilder.reportError(line, column, m);
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
        reportBuilder.reportWarning(line, col, message, cause);
    }


    private void reportError(int line, int col, String message,
            Throwable cause) {
        logger.debug("reporting error: [{}:{}]: {}", line, col, message);
        reportBuilder.reportError(line, col, message, cause);
    }


    private static final class ShadowCacheXMLGrammarPool
            implements XMLGrammarPool {
        private final Set<Grammar> cache = new LinkedHashSet<Grammar>();
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
                    d.getNamespace(), d.getLiteralSystemId(),
                    d.getExpandedSystemId(), d.getBaseSystemId());
            if ((d.getNamespace() == null) || !(d instanceof XSDDescription)) {
                logger.trace("-> miss (invalid arguments supplied by caller)");
                return null;
            }

            final XSDDescription desc = (XSDDescription) d;
            final String namespace = desc.getNamespace();
            Grammar result = findGrammerFromCache(desc);
            if (result != null) {
                logger.trace("-> match from cache: {} -> {} / {}", namespace,
                        desc.getNamespace(), desc.getLiteralSystemId());
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
                    (grammars != null) && (grammars.length > 0)) {
                for (Grammar grammar : grammars) {
                    final XMLGrammarDescription gd = grammar
                            .getGrammarDescription();
                    if (findGrammerFromCache(gd) == null) {
                        if (!locked) {
                            logger.trace("cached grammar: {} / {}",
                                    gd.getNamespace(), gd.getLiteralSystemId());
                            cache.add(grammar);
                        } else {
                            final String literalSystemId = gd
                                    .getLiteralSystemId();
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
                    final XMLGrammarDescription gd = grammar
                            .getGrammarDescription();
                    if (gd.getNamespace().equals(desc.getNamespace())) {
                        return grammar;
                    }
                }
            }
            return null;
        }
    } // class ShadowCacheGrammarPool

} // class CMDIValidatorWorker
