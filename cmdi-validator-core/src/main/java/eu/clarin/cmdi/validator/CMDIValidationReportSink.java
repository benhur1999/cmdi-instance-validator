package eu.clarin.cmdi.validator;

public interface CMDIValidationReportSink {

    public void postReport(CMDIValidationReport report)
            throws CMDIValidatorException;

} // class CMDIValidationReportSink
