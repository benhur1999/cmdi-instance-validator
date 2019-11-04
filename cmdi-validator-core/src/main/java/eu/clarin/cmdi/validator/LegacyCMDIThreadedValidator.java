package eu.clarin.cmdi.validator;

import java.util.List;

import net.java.truevfs.access.TFile;

public class LegacyCMDIThreadedValidator extends CMDIValidator {
    private final int threads;
    private LegacyThreadedCMDIValidatorProcessor processor;
    private LegacyCMDIValidator validator; 


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

        validator = new LegacyCMDIValidator(config,
                new LegacyCMDIValidator.ValidationHandlerFacade() {
            
            @Override
            public void onJobStarted() throws CMDIValidatorException {
                handleProcessingStarted();
            }
            
            
            @Override
                    public void onJobFinished(CMDIValidator.Result result)
                            throws CMDIValidatorException {
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
        if (validator != null) {
            validator.abort();
        }
    }


    @Override
    public void shutdown() throws CMDIValidatorException {
        processor.shutdown();
    }

} // class LegacyCMDIThreadedValidator

