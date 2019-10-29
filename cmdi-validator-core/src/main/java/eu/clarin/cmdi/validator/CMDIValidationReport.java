/**
 * This software is copyright (c) 2014 by
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
import java.util.List;


public interface CMDIValidationReport {
    public enum Severity {
        INFO {
            @Override
            public String getShortcut() {
                return "I";
            }

            @Override
            public int priority() {
                return 1;
            }
        },
        WARNING {
            @Override
            public String getShortcut() {
                return "W";
            }

            @Override
            public int priority() {
                return 2;
            }
        },
        ERROR {
            @Override
            public String getShortcut() {
                return "E";
            }

            @Override
            public int priority() {
                return 3;
            }
        };

        public abstract String getShortcut();


        public abstract int priority();
    } // enum Severity

    public interface Message {
        public Severity getSeverity();


        public int getLineNumber();


        public int getColumnNumber();


        public String getMessage();


        public Throwable getCause();
    } // interface Message


    public File getFile();


    public boolean isFileSkipped();


    public boolean isSuccess();


    public boolean isWarning();


    public boolean isFailed();


    public Severity getHighestSeverity();


    public boolean isHighestSeverity(Severity severity);


    public List<Message> getMessages();


    public Message getFirstMessage();


    public Message getFirstMessage(Severity severity);


    public int getMessageCount();


    public int getMessageCount(Severity severity);

} // CMDIValidatorResult
