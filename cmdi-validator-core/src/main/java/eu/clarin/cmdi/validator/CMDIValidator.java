package eu.clarin.cmdi.validator;

import java.util.List;
import net.java.truevfs.access.TFile;

public abstract class CMDIValidator {
    protected final CMDIValidatorConfig config;
    private final CMDIValidationHandler handler;
    protected final List<TFile> files;
    private long started               = -1;
    private long finished              = -1;
    private int filesTotal   = 0;
    private int filesValidCount  = 0;
    private int filesWarningCount = 0;
    private int filesSkippedCount = 0;
    private int filesInvalidCount = 0;
    private long totalBytes  = 0;


    protected CMDIValidator(final CMDIValidatorConfig config,
            final CMDIValidationHandler handler,
            final List<TFile> files) {
        if (config == null) {
            throw new NullPointerException("config == null");
        }
        this.config = config;
        
        if (handler == null) {
            throw new NullPointerException("handler == null");
        }
        this.handler = handler;
        
        if (files == null) {
            throw new NullPointerException("files == null");
        }
        if (files.isEmpty()) {
            throw new IllegalArgumentException("files is empty");
        }
        this.files = files;
    }
    
    
    public abstract void start() throws CMDIValidatorInitException, CMDIValidatorException;


    public abstract void abort() throws CMDIValidatorException;


    public abstract void shutdown() throws CMDIValidatorException;


    public final Statistics getStatistics() {
        return new Statistics(filesTotal,
                filesValidCount,
                filesWarningCount,
                filesSkippedCount,
                filesInvalidCount,
                totalBytes);
    }
    
    
    protected final void handleProcessingStarted()
            throws CMDIValidatorException {
        started = System.nanoTime();
        // invoke user handler
        handler.onJobStarted();
    }


    protected final void handleProcessingFinished(LegacyCMDIValidator.Result result)
            throws CMDIValidatorException {
        finished = System.nanoTime();
        // invoke user handler
        handler.onJobFinished(result);
    }


    protected final void handlePostValidationReport(CMDIValidationReport report)
            throws CMDIValidatorException {
        filesTotal++;
        if (report.isSkipped()) {
            filesSkippedCount++;
        } else {
            totalBytes += report.getFileSize();

            switch (report.getResult()) {
            case VALID:
                filesValidCount++;
                break;
            case VALID_WITH_WARINGS:
                filesWarningCount++;
                break;
            case INVALID:
                filesInvalidCount++;
                break;
            default:
                break;
            } // switch
        }
        handler.onValidationReport(report);
    }

    
    public static final class Statistics {
        private final int filesTotal;
        private final int filesValidCount;
        private final int filesWarningCount;
        private final int filesSkippedCount;
        private final int filesInvalidCount;
        private final long totalBytes;


        private Statistics(int filesTotal, int filesValidCount,
                int filesWarningCount, int filesSkippedCount,
                int filesInvalidCount, long totalBytes) {
            this.filesTotal = filesTotal;
            this.filesValidCount = filesValidCount;
            this.filesWarningCount = filesWarningCount;
            this.filesSkippedCount = filesSkippedCount;
            this.filesInvalidCount = filesInvalidCount;
            this.totalBytes = totalBytes;
        }


        public int getFilesTotal() {
            return filesTotal;
        }


        public int getFilesValidCount() {
            return filesValidCount;
        }


        public int getFilesWarningCount() {
            return filesWarningCount;
        }


        public int getFilesSkippedCount() {
            return filesSkippedCount;
        }


        public int getFilesInvalidCount() {
            return filesInvalidCount;
        }


        public long getTotalBytes() {
            return totalBytes;
        }

    } // class Statistics
    
} // class AbstractCMDIValidator
