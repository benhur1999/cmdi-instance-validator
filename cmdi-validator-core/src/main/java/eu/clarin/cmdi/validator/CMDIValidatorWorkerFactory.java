package eu.clarin.cmdi.validator;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import javax.xml.transform.ErrorListener;
import javax.xml.transform.TransformerException;
import javax.xml.transform.stream.StreamSource;

import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.saxon.Configuration;
import net.sf.saxon.s9api.DocumentBuilder;
import net.sf.saxon.s9api.Processor;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.XQueryCompiler;
import net.sf.saxon.s9api.XQueryExecutable;
import net.sf.saxon.s9api.XdmDestination;
import net.sf.saxon.s9api.XdmNode;
import net.sf.saxon.s9api.XsltCompiler;
import net.sf.saxon.s9api.XsltExecutable;
import net.sf.saxon.s9api.XsltTransformer;

public class CMDIValidatorWorkerFactory {
    private static final Logger logger =
            LoggerFactory.getLogger(CMDIValidatorWorkerFactory.class);
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
    private final CMDIValidatorConfig config;
    private final CMDISchemaLoader schemaLoader;
    private final Processor processor;
    private final XsltExecutable schematronValidatorExecutable;
    private final XQueryExecutable analyzeSchematronReport;
    private final List<CMDIValidatorExtension> extensions;
    

    public CMDIValidatorWorkerFactory(CMDIValidatorConfig config)
            throws CMDIValidatorInitException {
        if (config == null) {
            throw new NullPointerException("config == null");
        }
        this.config = config;
        
        logger.debug("initializing ...");
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

    }
    
    
    public CMDIValidatorWorker createWorker() {
        return new CMDIValidatorWorker(config,
                schemaLoader,
                processor,
                schematronValidatorExecutable,
                analyzeSchematronReport,
                extensions,
                config.getMaxFileSize());
    }


    private static CMDISchemaLoader initSchemaLoader(
            final CMDIValidatorConfig config)
            throws CMDIValidatorInitException {
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
        return new CMDISchemaLoader(cacheDirectory,
                CMDISchemaLoader.DISABLE_CACHE_AGING,
                connectTimeout,
                socketTimeout);
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

} // class CMDIValidatorWorkerFactory
