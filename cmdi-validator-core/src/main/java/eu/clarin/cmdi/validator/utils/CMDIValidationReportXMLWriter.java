package eu.clarin.cmdi.validator.utils;

import java.io.File;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.clarin.cmdi.validator.CMDIValidationReport;
import eu.clarin.cmdi.validator.CMDIValidatorException;
import net.sf.saxon.s9api.Processor;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.Serializer;
import net.sf.saxon.s9api.Serializer.Property;


public class CMDIValidationReportXMLWriter {
    private static final Logger logger =
            LoggerFactory.getLogger(CMDIValidationReportXMLWriter.class);
    private final File file;
    private XMLStreamWriter writer;


    public CMDIValidationReportXMLWriter(File file) throws CMDIValidatorException {
        if (file == null) {
            throw new NullPointerException("file == null");
        }
        this.file = file;
        logger.debug("open '{}'", file.getAbsoluteFile());
        try {
            Processor p = new Processor(false);
            Serializer s = p.newSerializer(file);
            s.setCloseOnCompletion(true);
            s.setOutputProperty(Property.INDENT, "yes");
            writer = s.getXMLStreamWriter();
            writer.writeStartDocument();
            writer.writeStartElement("report");
        } catch (XMLStreamException e) {
            throw new CMDIValidatorException("writer", e);
        } catch (SaxonApiException e) {
            throw new CMDIValidatorException("saxon", e);
        }
    }


    public synchronized void close() throws CMDIValidatorException {
        logger.debug("closing '{}'", file);
        try {
            writer.writeEndElement(); // "report" element
            writer.writeEndDocument();
            writer.close();
        } catch (XMLStreamException e) {
            throw new CMDIValidatorException("stream", e);
        }
    }


    public synchronized void writeReport(CMDIValidationReport report)
            throws CMDIValidatorException {
        try {
            writer.writeStartElement("file");
            String result;
            switch (report.getResult()) {
            case VALID:
                result = "valid";
                break;
            case VALID_WITH_WARINGS:
                result = "valid-with-warnings";
                break;
            case INVALID:
                result = "invalid";
                break;
            case SKIPPED:
                result = "skipped";
                break;
            default:
                result = "unknown";
                break;
            }
            writer.writeAttribute("result", result);
            writer.writeAttribute("name", report.getFile().toString());
            writer.writeAttribute("size", Long.toString(report.getFileSize()));
            if (report.getMessageCount() > 0) {
                writer.writeStartElement("messages");
                String severity;
                for (CMDIValidationReport.Message message : report.getMessages()) {
                    writer.writeStartElement("message");
                    switch (message.getSeverity()) {
                    case INFO:
                        severity = "info";
                        break;
                    case WARNING:
                        severity = "warning";
                        break;
                    case ERROR:
                        severity = "error";
                        break;
                    default:
                        severity = "unknown";
                        break;
                    } // switch
                    writer.writeAttribute("severity", severity);
                    if (message.getLineNumber() > 0) {
                        writer.writeAttribute("line",
                                Integer.toString(message.getLineNumber()));
                    }
                    if (message.getColumnNumber() > 0) {
                        writer.writeAttribute("column",
                                Integer.toString(message.getColumnNumber()));
                    }
                    writer.writeCharacters(message.getMessage());
                    writer.writeEndElement(); // "message" element
                }
                writer.writeEndElement(); // "messages" element
            }
            writer.writeEndElement(); // "file" element
        } catch (XMLStreamException e) {
            throw new CMDIValidatorException("stream", e);
        }
    }

}
