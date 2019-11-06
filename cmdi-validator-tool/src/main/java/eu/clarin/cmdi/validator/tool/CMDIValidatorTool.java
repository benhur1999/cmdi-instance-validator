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
package eu.clarin.cmdi.validator.tool;

import java.io.File;
import java.io.FileFilter;
import java.io.PrintWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.java.truevfs.access.TFile;
import net.java.truevfs.access.TVFS;
import net.java.truevfs.kernel.spec.FsSyncException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOCase;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarin.cmdi.validator.CMDIValidatorConfig;
import eu.clarin.cmdi.validator.CMDIValidatorException;
import eu.clarin.cmdi.validator.CMDIValidatorInitException;
import eu.clarin.cmdi.validator.LegacyCMDIThreadedValidator;
import eu.clarin.cmdi.validator.CMDIThreadedValidator;
import eu.clarin.cmdi.validator.CMDIValidator;
import eu.clarin.cmdi.validator.CMDIValidationHandlerAdapter;
import eu.clarin.cmdi.validator.CMDIValidationReport;
import eu.clarin.cmdi.validator.CMDIValidationReport.Message;
import eu.clarin.cmdi.validator.CMDIValidationReport.Severity;
import eu.clarin.cmdi.validator.extensions.CheckHandlesExtension;
import eu.clarin.cmdi.validator.utils.HandleResolver;
import eu.clarin.cmdi.validator.utils.CMDIValidationReportXMLWriter;


public class CMDIValidatorTool {
    private static final String PRG_NAME                   = "cmdi-validator";
    private static final long DEFAULT_MAX_FILE_SIZE        = 10 * 1024 * 1024; 
    private static final long DEFAULT_PROGRESS_INTERVAL    = 15000;
    private static final Locale LOCALE                     = Locale.ENGLISH;
    private static final String OPT_DEBUG                  = "d";
    private static final String OPT_DEBUG_TRACE            = "D";
    private static final String OPT_QUIET                  = "q";
    private static final String OPT_VERBOSE                = "v";
    private static final String OPT_THREAD_COUNT           = "t";
    private static final String OPT_NO_THREADS             = "T";
    private static final String OPT_NO_ESTIMATE            = "E";
    private static final String OPT_MAX_FILESIZE           = "l";
    private static final String OPT_NO_MAX_FILESIZE        = "L";
    private static final String OPT_SCHEMA_CACHE_DIR       = "c";
    private static final String OPT_NO_SCHEMATRON          = "S";
    private static final String OPT_SCHEMATRON_FILE        = "s";
    private static final String OPT_FILENAME_FILTER        = "F";
    private static final String OPT_REPORT_FILE            = "R";
    private static final String OPT_CHECK_PIDS             = "p";
    private static final String OPT_CHECK_AND_RESOLVE_PIDS = "P";
    private static final String OPT_USE_LEGACY             = "Xlegacy";
    private static final Logger logger =
            LoggerFactory.getLogger(CMDIValidatorTool.class);
    private static final org.apache.log4j.ConsoleAppender appender;


    public static void main(String[] args) {
        /*
         * application defaults
         */
        int debugging               = 0;
        boolean quiet               = false;
        boolean verbose             = false;
        int threadCount             = Runtime.getRuntime().availableProcessors();
        boolean estimate            = true;
        long maxFileSize            = DEFAULT_MAX_FILE_SIZE;
        long progressInterval       = DEFAULT_PROGRESS_INTERVAL;
        File schemaCacheDir         = null;
        boolean disableSchematron   = false;
        File schematronFile         = null;
        FileFilter fileFilter       = null;
        File reportFile             = null;
        boolean checkPids           = false;
        boolean checkAndResolvePids = false;
        boolean useLegacy           = false;
        
        /*
         * setup command line parser
         */
        final Options options = createCommandLineOptions();
        try {
            final CommandLineParser parser = new DefaultParser();
            final CommandLine line = parser.parse(options, args);
            // check incompatible combinations
            if (line.hasOption(OPT_MAX_FILESIZE) && line.hasOption(OPT_NO_MAX_FILESIZE)) {
                throw new ParseException(String.format(
                        "The %s and %s options are mutually exclusive",
                        OPT_MAX_FILESIZE, OPT_NO_MAX_FILESIZE));
            }
            if (line.hasOption(OPT_THREAD_COUNT) && line.hasOption(OPT_NO_THREADS)) {
                throw new ParseException(String.format(
                        "The %s and %s options are mutually exclusive",
                        OPT_THREAD_COUNT, OPT_NO_THREADS));
            }
            if (line.hasOption(OPT_DEBUG) && line.hasOption(OPT_QUIET)) {
                throw new ParseException(String.format(
                        "The %s and %s options are mutually exclusive",
                        OPT_DEBUG, OPT_QUIET));
            }
            if (line.hasOption(OPT_VERBOSE) && line.hasOption(OPT_QUIET)) {
                throw new ParseException(String.format(
                        "The %s and %s options are mutually exclusive",
                        OPT_VERBOSE, OPT_QUIET));
            }
            if (line.hasOption(OPT_NO_SCHEMATRON) && line.hasOption(OPT_SCHEMATRON_FILE)) {
                throw new ParseException(String.format(
                        "The %s and %s options are mutually exclusive",
                        OPT_NO_SCHEMATRON, OPT_SCHEMATRON_FILE));
            }
            if (line.hasOption(OPT_CHECK_PIDS) && line.hasOption(OPT_CHECK_AND_RESOLVE_PIDS)) {
                throw new ParseException(String.format(
                        "The %s and %s options are mutually exclusive",
                        OPT_CHECK_PIDS, OPT_CHECK_AND_RESOLVE_PIDS));
            }

            // extract options
            if (line.hasOption(OPT_DEBUG)) {
                debugging = 1;
            }
            if (line.hasOption(OPT_DEBUG_TRACE)) {
                debugging = 2;
            }
            if (line.hasOption(OPT_QUIET)) {
                quiet = true;
            }
            if (line.hasOption(OPT_VERBOSE)) {
                verbose = true;
            }
            if (quiet) {
                progressInterval = -1;
            }
            if (line.hasOption(OPT_THREAD_COUNT)) {
                try {
                    threadCount = Integer.parseInt(
                            line.getOptionValue(OPT_THREAD_COUNT));
                    if (threadCount < 1) {
                        throw new ParseException(
                                "thread count must be larger then 0");
                    }
                } catch (NumberFormatException e) {
                    throw new ParseException("invalid number");
                }
            }
            if (line.hasOption(OPT_NO_THREADS)) {
                threadCount = 1;
            }
            if (line.hasOption(OPT_NO_ESTIMATE) || (progressInterval < 0)) {
                estimate = false;
            }
            if (line.hasOption(OPT_MAX_FILESIZE)) {
                maxFileSize = parseMaxFileOption(
                        line.getOptionValue(OPT_MAX_FILESIZE));
            }
            if (line.hasOption(OPT_NO_MAX_FILESIZE)) {
                maxFileSize = 0;
            }
            if (line.hasOption(OPT_SCHEMA_CACHE_DIR)) {
                String dir = line.getOptionValue(OPT_SCHEMA_CACHE_DIR);
                if ((dir == null) || dir.isEmpty()) {
                    throw new ParseException("invalid argument for -" +
                            OPT_SCHEMA_CACHE_DIR);
                }
                schemaCacheDir = new File(dir);
            }
            if (line.hasOption(OPT_NO_SCHEMATRON)) {
                disableSchematron = true;
            }
            if (line.hasOption(OPT_SCHEMATRON_FILE)) {
                String name = line.getOptionValue(OPT_SCHEMATRON_FILE);
                if ((name == null) || name.isEmpty()) {
                    throw new ParseException("invalid argument for -" +
                            OPT_SCHEMATRON_FILE);
                }
                schematronFile = new File(name);
            }
            if (line.hasOption(OPT_FILENAME_FILTER)) {
                String[] wildcards = line.getOptionValues(OPT_FILENAME_FILTER);
                try {
                    fileFilter = new WildcardFileFilter(wildcards, IOCase.SYSTEM);
                } catch (IllegalArgumentException e) {
                    throw new ParseException("invalid argument for -" +
                            OPT_FILENAME_FILTER);
                }
            }

            if (line.hasOption(OPT_REPORT_FILE)) {
                String filename = line.getOptionValue(OPT_REPORT_FILE);
                if ((filename == null) || filename.isEmpty()) {
                    throw new ParseException("invalid argument for -" +
                            OPT_REPORT_FILE);
                }
                reportFile = new File(filename);
            }

            if (line.hasOption(OPT_CHECK_PIDS)) {
                checkPids = true;
            }
            if (line.hasOption(OPT_CHECK_AND_RESOLVE_PIDS)) {
                checkAndResolvePids = true;
            }

            if (line.hasOption(OPT_USE_LEGACY)) {
                useLegacy = true;
            }

            final String[] remainingArgs = line.getArgs();
            if ((remainingArgs == null) || (remainingArgs.length == 0)) {
                throw new ParseException("require at leat one <DIRECTORY> or " +
                        "<FILE> as additional command line parameter");
            }

            final org.apache.log4j.Logger log =
                    org.apache.log4j.Logger.getLogger(
                            CMDIValidator.class.getPackage().getName());
            if (debugging > 0) {
                appender.setLayout(
                        new org.apache.log4j.PatternLayout("[%p] %t: %m%n"));
                if (debugging > 1) {
                    log.setLevel(org.apache.log4j.Level.TRACE);
                } else {
                    log.setLevel(org.apache.log4j.Level.DEBUG);
                }
            } else {
                if (quiet) {
                    log.setLevel(org.apache.log4j.Level.ERROR);
                } else {
                    log.setLevel(org.apache.log4j.Level.INFO);
                }
            }

            // prepare files list ...
            List<TFile> files = new ArrayList<>(remainingArgs.length);
            for (int i = 0; i < remainingArgs.length; i++) {
                TFile file = new TFile(remainingArgs[i]);
                if (file.exists()) {
                    files.add(file);
                } else {
                    logger.error("file or directory not found: {}", file);
                    /* bail early */
                    System.exit(66); /* EX_NOINPUT */
                }
            }

            try {
                /*
                 * process file(s) ...
                 */
                int totalFileCount = -1;
                if (estimate && logger.isInfoEnabled()) {
                    logger.debug("counting files ...");
                    totalFileCount = countFiles(files, fileFilter);
                }

                if (threadCount > 1) {
                    logger.debug("using {} threads", threadCount);
                }

                final Handler handler = new Handler(verbose, reportFile);

                final CMDIValidatorConfig.Builder builder =
                            new CMDIValidatorConfig.Builder(handler);
                logger.debug("skipping files larger than {} bytes",
                        maxFileSize);
                builder.maxFileSize(maxFileSize);
                if (schemaCacheDir != null) {
                    logger.info("using schema cache directory: {}",
                            schemaCacheDir);
                    builder.schemaCacheDirectory(schemaCacheDir);
                }
                if (schematronFile != null) {
                    logger.info("using Schematron schema from file: {}",
                            schematronFile);
                    builder.schematronSchemaFile(schematronFile);
                }
                if (disableSchematron) {
                    builder.disableSchematron();
                }
                if (fileFilter != null) {
                    builder.fileFilter(fileFilter);
                }

                CheckHandlesExtension checkHandleExtension = null;
                if (checkPids || checkAndResolvePids) {
                    if (checkAndResolvePids) {
                        logger.info("enabling PID validation (syntax and resolving)");
                    } else {
                        logger.info("enabling PID validation (syntax only)");
                    }
                    checkHandleExtension =
                            new CheckHandlesExtension(checkAndResolvePids);
                    builder.extension(checkHandleExtension);
                }

                CMDIValidatorConfig config = builder.build();
                CMDIValidator validator;
                if (!useLegacy) {
                    validator = new CMDIThreadedValidator(config,
                            handler, files, threadCount);
                } else {
                    logger.warn("using legacy validator");
                    validator = new LegacyCMDIThreadedValidator(config,
                            handler, files, threadCount);
                }

                try {
                    validator.start();

                    /*
                     * Wait until validation is done and report about
                     * progress every now and then ...
                     */
                    for (;;) {
                        try {
                            if (handler.await(progressInterval)) {
                                break;
                            }
                        } catch (InterruptedException e) {
                            /* IGNORE */
                        }
                        if ((progressInterval > 0) && logger.isInfoEnabled()) {
                            final CMDIValidator.Statistics s = validator.getStatistics();
                            int fps = s.getFilesProcessedPerSecond();
                            long bps = s.getBytesProcessedPerSecond();
                            if (totalFileCount > 0) {
                                float complete = (s.getTotalFilesCount() / (float)  totalFileCount) * 100f;
                                logger.info("processed {} files ({}%) in {} ({} files/second, {}/second) ...",
                                        s.getTotalFilesCount(),
                                        String.format(LOCALE, "%.2f", complete),
                                        formatDuration(s.getElapsedTime()),
                                        ((fps != -1) ? fps : "N/A"),
                                        ((bps != -1) ? humananizeBytes(bps) : "N/A MB"));
                            } else {
                                logger.info("processed {} files in {} ({} files/second, {}/second) ...",
                                        s.getTotalFilesCount(),
                                        formatDuration(s.getElapsedTime()),
                                        ((fps != -1) ? fps : "N/A"),
                                        ((bps != -1) ? humananizeBytes(bps) : "N/A MB"));
                            }
                            if (logger.isDebugEnabled()) {
                                if ((checkHandleExtension != null) &&
                                        checkHandleExtension.isResolvingHandles()) {
                                    final HandleResolver.Statistics stats =
                                            checkHandleExtension.getStatistics();
                                    logger.debug("[handle resolver stats] total requests: {}, running requests: {}, cache hits: {}, cache misses: {}, current cache size: {}",
                                            stats.getTotalRequestsCount(),
                                            stats.getCurrentRequestsCount(),
                                            stats.getCacheHitCount(),
                                            stats.getCacheMissCount(),
                                            stats.getCurrentCacheSize());
                                }
                            }
                        }
                    } // for (;;)
                } finally {
                    validator.shutdown();
                }

                CMDIValidator.Statistics s = handler.finalStats;
                int fps = s.getFilesProcessedPerSecond();
                long bps = s.getBytesProcessedPerSecond();

                logger.info("time elapsed: {}, validation result: {}% failure rate (files: {} total, {} passed, {} failed, {} skipped; {} total, {} files/second, {}/second)",
                        formatDuration(s.getElapsedTime()),
                        String.format(LOCALE, "%.2f", s.getFailureRate() * 100f),
                        s.getTotalFilesCount(),
                        s.getValidFilesCount(),
                        s.getFilesInvalidCount(),
                        s.getSkippedFilesCount(),
                        humananizeBytes(s.getTotalBytesCount()),
                        ((fps != -1) ? fps : "N/A"),
                        ((bps != -1) ? humananizeBytes(bps) : "N/A MB"));
                logger.debug("... done");
            } finally {
                for (TFile file : files) {
                    if (file.isArchive()) {
                        try {   
                            TVFS.umount(file);
                        } catch (FsSyncException e) {
                            logger.error("error unmounting archive '{}'",
                                    file, e);
                        }
                    }
                }
            }
        } catch (CMDIValidatorException e) {
            logger.error("error initalizing job: {}", e.getMessage());
            if (debugging > 0) {
                logger.error(e.getMessage(), e);
            }
            System.exit(1);
        } catch (CMDIValidatorInitException e) {
            logger.error("error initializing validator: {}", e.getMessage());
            if (debugging > 0) {
                logger.error(e.getMessage(), e);
            }
            System.exit(2);
        } catch (ParseException e) {
            PrintWriter writer = new PrintWriter(System.err);
            if (e.getMessage() != null) {
                writer.print("ERROR: ");
                writer.println(e.getMessage());
            }
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(writer, HelpFormatter.DEFAULT_WIDTH, PRG_NAME,
                    null, options, HelpFormatter.DEFAULT_LEFT_PAD,
                    HelpFormatter.DEFAULT_DESC_PAD, null, true);
            writer.flush();
            writer.close();
            System.exit(64); /* EX_USAGE */
        }
    }


    private static Options createCommandLineOptions() {
        final Options options = new Options();
        OptionGroup g1 = new OptionGroup();
        g1.addOption(Option.builder(OPT_DEBUG)
                .longOpt("debug")
                .desc("enable debugging output")
                .build());
        g1.addOption(Option.builder(OPT_DEBUG_TRACE)
                .longOpt("trace")
                .desc("enable full debugging output")
                .build());
        g1.addOption(Option.builder(OPT_QUIET)
                .longOpt("quiet")
                .desc("be quiet")
                .build());
        options.addOptionGroup(g1);
        options.addOption(Option.builder(OPT_VERBOSE)
                .longOpt("verbose")
                .desc("be verbose")
                .build());
        OptionGroup g2 = new OptionGroup();
        g2.addOption(Option.builder(OPT_THREAD_COUNT)
                .hasArg()
                .argName("COUNT")
                .longOpt("threads")
                .desc("number of validator threads")
                .build());
        g2.addOption(Option.builder(OPT_NO_THREADS)
                .longOpt("no-threads")
                .desc("disable threading")
                .build());
        options.addOptionGroup(g2);
        options.addOption(Option.builder(OPT_NO_ESTIMATE)
                .longOpt("no-estimate")
                .desc("disable gathering of total file count for progress reporting")
                .build());
        OptionGroup g3 = new OptionGroup();
        g3.addOption(Option.builder(OPT_MAX_FILESIZE)
                .hasArg()
                .argName("SIZE")
                .longOpt("max-file-size")
                .desc(String.format("maximum file size to process (default: %s)",
                        humananizeBytes(DEFAULT_MAX_FILE_SIZE)))
                .build());
        g3.addOption(Option.builder(OPT_NO_MAX_FILESIZE)
                 .longOpt("process-all")
                 .desc("process all files regardless of size")
                 .build());
        options.addOptionGroup(g3);
        options.addOption(Option.builder(OPT_SCHEMA_CACHE_DIR)
                .hasArg()
                .argName("DIRECTORY")
                .longOpt("schema-cache-dir")
                .desc("schema caching directory")
                .build());
        OptionGroup g4 = new OptionGroup();
        g4.addOption(Option.builder(OPT_NO_SCHEMATRON)
                .longOpt("no-schematron")
                .desc("disable Schematron validator")
                .build());
        g4.addOption(Option.builder(OPT_SCHEMATRON_FILE)
                .hasArg()
                .argName("FILE")
                .longOpt("schematron-file")
                .desc("load Schematron schema from file")
                .build());
        options.addOptionGroup(g4);
        options.addOption(Option.builder(OPT_FILENAME_FILTER)
                .hasArg()
                .argName("WILDCARD")
                .longOpt("file-filter")
                .desc("only process filenames matching a wildcard")
                .build());
        options.addOption(Option.builder(OPT_REPORT_FILE)
                .hasArg()
                .argName("FILENAME")
                .longOpt("report-file")
                .desc("write validation report")
                .build());
        OptionGroup g5 = new OptionGroup();
        g5.addOption(Option.builder(OPT_CHECK_PIDS)
                .longOpt("check-pids")
                .desc("check persistent identifiers syntax")
                .build());
        g5.addOption(Option.builder(OPT_CHECK_AND_RESOLVE_PIDS)
                .longOpt("check-and-resolve-pids")
                .desc("check persistent identifiers syntax and if they resolve properly")
                .build());
        options.addOptionGroup(g5);
        options.addOption(Option.builder(OPT_USE_LEGACY)
                .desc("use legacy validator (development only)")
                .build());
        return options;
    }


    private static long parseMaxFileOption(String s) throws ParseException {
        final Matcher m = Pattern
                .compile("^(\\d+)\\s*(?:([KMGT])B?)?$", Pattern.CASE_INSENSITIVE)
                .matcher(s);
        if (m.matches()) {
            long size = Long.parseLong(m.group(1));
            String unit = m.group(2);
            if (unit != null) {
                unit = unit.toUpperCase();
                if ("K".equals(unit)) {
                    size *= 1024l;
                } else if ("M".equals(unit)) {
                    size *= 1048576l;
                } else if ("G".equals(unit)) {
                    size *= 1073741824l;
                } else if ("T".equals(unit)) {
                    size *= 1099511627776l;
                }
            }
            return size;
        } else {
            throw new ParseException(String.format("invalid size: %s", s));
        }
    }


    private static int countFiles(List<TFile> files, FileFilter fileFilter) {
        int count = 0;
        for (TFile file : files) {
            if (file.isDirectory()) {
                count += countFilesInDir(file, fileFilter);
            } else {
                if ((fileFilter != null) && !fileFilter.accept(file)) {
                    count++;
                }
            }
        }
        return count;
    }
    
    
    private static int countFilesInDir(TFile directory, FileFilter fileFilter) {
        int count = 0;
        final TFile[] entries = directory.listFiles();
        if ((entries != null) && (entries.length > 0)) {
            for (TFile entry : entries) {
                if (entry.isDirectory()) {
                    if ((fileFilter != null) &&
                            (entry.isArchive() && !fileFilter.accept(entry))) {
                        continue;
                    }
                    count += countFilesInDir(entry, fileFilter);
                } else {
                    if ((fileFilter != null) && !fileFilter.accept(entry)) {
                        continue;
                    }
                    count++;
                }
            }
        }
        return count;
    }


    private static final String formatDuration(Duration duration) {
        final long seconds = duration.getSeconds();
        return String.format("%d:%02d:%02d",
                (seconds / 3600),
                ((seconds % 3600) / 60),
                (seconds % 60));
    }

    
    private static final double LOG_1024 = Math.log(1024);


    private static String humananizeBytes(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        } else {
            int exp = (int) (Math.log(bytes) / LOG_1024);
            char pre = "KMGTPE".charAt(exp - 1);
            return String.format(LOCALE, "%.1f %sB",
                    (bytes / Math.pow(1024, exp)), pre);
        }
    }    

    
    private static class Handler extends CMDIValidationHandlerAdapter {
        private final boolean verbose;
        private final File reportFile;
        private boolean isCompleted = false;
        private final Object waiter = new Object();
        private CMDIValidationReportXMLWriter reportFileWriter;
        private CMDIValidator.Statistics finalStats;

        private Handler(boolean verbose, File reportFile) {
            this.verbose = verbose;
            this.reportFile = reportFile;
        }


        public boolean await(long timeout) throws InterruptedException {
            synchronized (waiter) {
                if (isCompleted) {
                    return true;
                }
                if (timeout > 0) {
                    waiter.wait(timeout);
                } else {
                    waiter.wait();
                }
                return isCompleted;
            }
        }


        @Override
        public void onJobStarted() throws CMDIValidatorException {
            logger.debug("validation process started");
            if (reportFile != null) {
                this.reportFileWriter =
                        new CMDIValidationReportXMLWriter(reportFile);    
            }
        }


        @Override
        public void onJobFinished(final CMDIValidator.Result result,
                final CMDIValidator.Statistics statistics)
                throws CMDIValidatorException {
            if (reportFileWriter != null) {
                reportFileWriter.close();
            }

            switch (result) {
            case OK:
                logger.debug("validation process finished successfully");
                break;
            case ABORTED:
                logger.info("processing was aborted");
                break;
            case ERROR:
                logger.debug("validation process yielded an error");
                break;
            default:
                logger.debug("unknown result: " + result);
            } // switch
            finalStats = statistics;
            
            synchronized (waiter) {
                isCompleted = true;
                waiter.notifyAll();
            } // synchronized (waiter)
        }


        @Override
        public void onValidationReport(final CMDIValidationReport report)
                throws CMDIValidatorException {

            if (reportFileWriter != null) {
                reportFileWriter.writeReport(report);
            }

            final File file = report.getFile();
            if (report.isSkipped()) {
                if (verbose) {
                    logger.info("file '{}' was skipped", file);
                }
            } else {
                switch (report.getResult()) {
                case VALID:
                    logger.debug("file '{}' is valid", file);
                    break;
                case VALID_WITH_WARINGS:
                    if (verbose) {
                        logger.warn("file '{}' is valid (with warnings):",
                                file);
                        for (Message msg : report.getMessages()) {
                            if ((msg.getLineNumber() != -1) &&
                                    (msg.getColumnNumber() != -1)) {
                                logger.warn(" ({}) {} [line={}, column={}]",
                                        msg.getSeverity().getShortcut(),
                                        msg.getMessage(), msg.getLineNumber(),
                                        msg.getColumnNumber());
                            } else {
                                logger.warn(" ({}) {}",
                                        msg.getSeverity().getShortcut(),
                                        msg.getMessage());
                            }
                        }
                    } else {
                        Message msg = report.getFirstMessage(Severity.WARNING);
                        int count = report.getMessageCount(Severity.WARNING);
                        if (count > 1) {
                            logger.warn(
                                    "file '{}' is valid (with warnings): {} ({} more warnings)",
                                    file, msg.getMessage(), (count - 1));
                        } else {
                            logger.warn(
                                    "file '{}' is valid (with warnings): {}",
                                    file, msg.getMessage());
                        }
                    }
                    break;
                case INVALID:
                    if (verbose) {
                        logger.error("file '{}' is invalid:", file);
                        for (Message msg : report.getMessages()) {
                            if ((msg.getLineNumber() != -1) &&
                                    (msg.getColumnNumber() != -1)) {
                                logger.error(" ({}) {} [line={}, column={}]",
                                        msg.getSeverity().getShortcut(),
                                        msg.getMessage(), msg.getLineNumber(),
                                        msg.getColumnNumber());
                            } else {
                                logger.error(" ({}) {}",
                                        msg.getSeverity().getShortcut(),
                                        msg.getMessage());
                            }
                        }
                    } else {
                        Message msg = report.getFirstMessage(Severity.ERROR);
                        int count = report.getMessageCount(Severity.ERROR);
                        if (count > 1) {
                            logger.error(
                                    "file '{}' is invalid: {} ({} more errors)",
                                    file, msg.getMessage(), (count - 1));
                        } else {
                            logger.error("file '{}' is invalid: {}", file,
                                    msg.getMessage());
                        }
                    }
                    break;
                default:
                    /* IGNORE */
                    break;
                } // switch
            }
        }
    } // class Handler


    static {
        appender = new org.apache.log4j.ConsoleAppender(
                new org.apache.log4j.PatternLayout("%m%n"),
                org.apache.log4j.ConsoleAppender.SYSTEM_OUT);
        org.apache.log4j.BasicConfigurator.configure(appender);
        org.apache.log4j.Logger logger =
                org.apache.log4j.Logger.getRootLogger();
        logger.setLevel(org.apache.log4j.Level.WARN);
    }

} // class CMDIValidatorTool
