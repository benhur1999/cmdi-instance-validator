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
package eu.clarin.cmdi.validator;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import eu.clarin.cmdi.validator.CMDIValidationReport.Severity;
import eu.clarin.cmdi.validator.CMDIValidationReport.Message;
import eu.clarin.cmdi.validator.CMDIValidationReport.Result;


public final class CMDIValidatonReportBuilder {
    private static final int DEFAULT_MESSAGE_SIZE = 16;
    private static final int MAX_MESSAGE_SIZE = 64;
    private File file;
    private boolean fileSkipped;
    private List<Message> messages =
            new ArrayList<Message>(DEFAULT_MESSAGE_SIZE);
    private Severity highestSeverity = Severity.INFO;


    public void reset() {
        file = null;
        fileSkipped = false;
        if (messages.size() > MAX_MESSAGE_SIZE) {
            messages = new ArrayList<Message>(DEFAULT_MESSAGE_SIZE);
        } else {
            messages.clear();
        }
        highestSeverity = Severity.INFO;
    }


    public void setFile(File file) {
        this.file = file;
    }


    public void setFileSkipped() {
        this.fileSkipped = true;
    }


    public void reportInfo(int line, int col, String message) {
        reportInfo(line, col, message, null);
    }


    public void reportInfo(int line, int col, String message,
            Throwable cause) {
        addMessage(Severity.INFO, line, col, message, cause);
    }


    public void reportWarning(int line, int col, String message) {
        reportWarning(line, col, message, null);
    }


    public void reportWarning(int line, int col, String message,
            Throwable cause) {
        addMessage(Severity.WARNING, line, col, message, cause);
    }


    public void reportError(int line, int col, String message) {
        reportError(line, col, message, null);
    }


    public void reportError(int line, int col, String message,
            Throwable cause) {
        addMessage(Severity.ERROR, line, col, message, cause);
    }


    private void addMessage(final Severity severity,
            final int line,
            final int col,
            final String message,
            final Throwable cause) {
        if (severity.priority() > highestSeverity.priority()) {
            highestSeverity = severity;
        }
        messages.add(new Message(severity, line, col, message, cause));
    }


    public CMDIValidationReport build() {
        Result result;
        if (fileSkipped) {
            result = Result.SKIPPED;
        } else {
            switch (highestSeverity) {
            case INFO:
                result = Result.SUCCESS;
                break;
            case WARNING:
                result = Result.WARNING;
                break;
            case ERROR:
                result = Result.ERROR;
                break;
            default:
                /* NOT-REACH */
                throw new InternalError("cannot happen");
            }
        }
        return new CMDIValidationReport(file, result,
                new ArrayList<Message>(messages));
    }

} // class CMDIValidatonReportBuilder
