package eu.clarin.cmdi.validator;

import java.util.List;

import eu.clarin.cmdi.validator.LegacyCMDIValidator.Result;
import net.java.truevfs.access.TFile;

public class LegacyCMDIThreadedValidator extends CMDIValidator {
    private final int threads;
    private LegacyThreadedCMDIValidatorProcessor processor;


    public LegacyCMDIThreadedValidator(CMDIValidatorConfig config,
            CMDIValidationHandler handler, List<TFile> files, int threads) {
        super(config, handler, files);
        if (threads < 1) {
            throw new IllegalArgumentException("threads < 1");
        }
        this.threads = threads;
    }


    @Override
    public void start()
            throws CMDIValidatorInitException, CMDIValidatorException {
        processor = new LegacyThreadedCMDIValidatorProcessor(threads);
        processor.start();
        final LegacyCMDIValidator validator = new LegacyCMDIValidator(config,
                new LegacyCMDIValidator.ValidationHandlerFacade() {
            
            @Override
            public void onJobStarted() throws CMDIValidatorException {
                handleProcessingStarted();
            }
            
            
            @Override
            public void onJobFinished(Result result) throws CMDIValidatorException {
                handleProcessingFinished(result);
            }

            @Override
            public void onValidationReport(CMDIValidationReport report)
                    throws CMDIValidatorException {
                synchronized (this) {
                    handlePostValidationReport(report);
                }
            }
        });
        processor.process(validator);
    }


    @Override
    public void abort() throws CMDIValidatorException {
    }


    @Override
    public void shutdown() throws CMDIValidatorException {
        processor.shutdown();
    }

} // class LegacyCMDIThreadedValidator

