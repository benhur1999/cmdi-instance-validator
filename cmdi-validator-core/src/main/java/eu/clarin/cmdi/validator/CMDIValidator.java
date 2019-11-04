package eu.clarin.cmdi.validator;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import net.java.truevfs.access.TFile;

public abstract class CMDIValidator {
    protected final CMDIValidatorConfig config;
    private final CMDIValidationHandler handler;
    protected final List<TFile> files;
    private LocalDateTime startedTime = null;
    private LocalDateTime finishedTime = null;
    private int filesTotalCount = 0;
    private int filesValidCount = 0;
    private int filesWarningCount = 0;
    private int filesInvalidCount = 0;
    private int filesSkippedCount = 0;
    private long totalBytesCount = 0;
    private Statistics finalStatistics;


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
        return  (finalStatistics != null)
                ? finalStatistics
                : makeStatistics();
    }
    
    
    protected final void handleProcessingStarted()
            throws CMDIValidatorException {
        startedTime = LocalDateTime.now();
        // invoke user handler
        handler.onJobStarted();
    }


    protected final void handleProcessingFinished(CMDIValidator.Result result)
            throws CMDIValidatorException {
        finishedTime = LocalDateTime.now();
        // invoke user handler
        finalStatistics = makeStatistics();
        handler.onJobFinished(result, finalStatistics);
    }


    protected final void handlePostValidationReport(CMDIValidationReport report)
            throws CMDIValidatorException {
        filesTotalCount++;
        if (report.isSkipped()) {
            filesSkippedCount++;
        } else {
            totalBytesCount += report.getFileSize();

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


    private Statistics makeStatistics() {
        return new Statistics(startedTime,
                finishedTime,
                filesTotalCount,
                filesValidCount,
                filesWarningCount,
                filesInvalidCount,
                filesSkippedCount,
                totalBytesCount);
    }

    
    public enum Result {
        OK, ABORTED, ERROR
    }

    public static final class Statistics {
        private final LocalDateTime timestamp = LocalDateTime.now();
        private final LocalDateTime startedTime;
        private final LocalDateTime finishedTime;
        private final int filesTotalCount;
        private final int filesValidCount;
        private final int filesWarningCount;
        private final int filesInvalidCount;
        private final int filesSkippedCount;
        private final long totalBytesCount;


        private Statistics(LocalDateTime startedTime,
                LocalDateTime finishedTime,
                int filesTotalCount,
                int filesValidCount,
                int filesWarningCount, 
                int filesInvalidCount,
                int filesSkippedCount,
                long totalBytesCount) {
            this.startedTime = startedTime;
            this.finishedTime = finishedTime;
            this.filesTotalCount = filesTotalCount;
            this.filesValidCount = filesValidCount;
            this.filesWarningCount = filesWarningCount;
            this.filesInvalidCount = filesInvalidCount;
            this.filesSkippedCount = filesSkippedCount;
            this.totalBytesCount = totalBytesCount;
        }


        public LocalDateTime getTimestamp() {
            return timestamp;
        }


        public LocalDateTime getStartedTime() {
            return startedTime;
        }


        public LocalDateTime getFinishedTime() {
            return finishedTime;
        }


        public Duration getElapsedTime() {
            if (finishedTime != null) {
                return Duration.between(startedTime, finishedTime);
            } else {
                return Duration.between(startedTime, timestamp);
            }
        }


        public int getTotalFilesCount() {
            return filesTotalCount;
        }


        public int getValidFilesCount() {
            return filesValidCount;
        }


        public int getWarningFilesCount() {
            return filesWarningCount;
        }


        public int getFilesInvalidCount() {
            return filesInvalidCount;
        }


        public int getSkippedFilesCount() {
            return filesSkippedCount;
        }


        public long getTotalBytesCount() {
            return totalBytesCount;
        }

        
        public float getFailureRate() {
            return (filesTotalCount > 0)
                  ? ((float) filesInvalidCount / (float) filesTotalCount)
                  : 0.0f;
        }

        
        public int getFilesProcessedPerSecond() {
            final long seconds = getElapsedTime().getSeconds();
            if (seconds > 0) {
                return ((int) (filesTotalCount / seconds));
            } else {
                return -1;
            }
        }
        

        public long getBytesProcessedPerSecond() {
            final long seconds = getElapsedTime().getSeconds();
            if (seconds > 0) {
                return (totalBytesCount / seconds);
            } else {
                return -1;
            }
        }

    } // class Statistics
    
} // class AbstractCMDIValidator
