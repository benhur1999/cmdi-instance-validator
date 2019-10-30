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
import java.util.Collections;
import java.util.List;


public final class CMDIValidationReport {
    private final File file;
    private final Result result;
    private final List<Message> messages;

    
    CMDIValidationReport(File file,
            Result result,
            List<Message> messages) {
        this.file = file;
        this.result  = result;
        if ((messages != null) && !messages.isEmpty()) {
            this.messages = Collections.unmodifiableList(messages);
        } else {
            this.messages = null;
        }
    }


    public File getFile() {
        return file;
    }


    public Result getResult() {
        return result;
    }


    public boolean isSuccess() {
        return Result.SUCCESS.equals(result);
    }


    public boolean isWarning() {
        return Result.WARNING.equals(result);
    }


    public boolean isError() {
        return Result.ERROR.equals(result);
    }


    public boolean isSkipped() {
        return Result.SKIPPED.equals(result);
    }


    public List<Message> getMessages() {
        if (messages != null) {
          return messages;  
        } else {
            return Collections.emptyList();
        }
    }


    public Message getFirstMessage() {
        return (messages != null) ? messages.get(0) : null;
    }


    public Message getFirstMessage(Severity severity) {
        if (severity == null) {
            throw new NullPointerException("severity == null");
        }

        if (messages != null) {
            for (CMDIValidationReport.Message msg : messages) {
                if (severity.equals(msg.getSeverity())) {
                    return msg;
                }
            }
        }
        return null;
    }


    public int getMessageCount() {
        return (messages != null) ? messages.size() : 0;
    }


    public int getMessageCount(Severity severity) {
        if (severity == null) {
            throw new NullPointerException("severity == null");
        }

        int count = 0;
        if (messages != null) {
            for (Message msg : messages) {
                if (severity.equals(msg.getSeverity())) {
                    count++;
                }
            }
        }
        return count;
    }


    public enum Result {
        SUCCESS, WARNING, ERROR, SKIPPED
    } // enum Result


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
    

    public static final class Message {
        private final Severity severity;
        private final int line;
        private final int col;
        private final String message;
        private final Throwable cause;


        Message(Severity severity, int line, int col, String message,
                Throwable cause) {
            this.severity = severity;
            this.line = line;
            this.col = col;
            this.message = message;
            this.cause = cause;
        }


        public Severity getSeverity() {
            return severity;
        }


        public int getLineNumber() {
            return line;
        }


        public int getColumnNumber() {
            return col;
        }


        public String getMessage() {
            return message;
        }


        public Throwable getCause() {
            return cause;
        }
    } // inner class Message

} // class CMDIValidationReport
