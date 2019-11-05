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


    public long getFileSize() {
        return file.length();
    }


    public Result getResult() {
        return result;
    }


    public boolean isValid() {
        return Result.VALID.equals(result);
    }


    public boolean isValidWithWarnings() {
        return Result.VALID_WITH_WARINGS.equals(result);
    }


    public boolean isInvalid() {
        return Result.INVALID.equals(result);
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
        VALID {
            @Override
            public String getXMLString() {
                return "valid";
            }
        },
        VALID_WITH_WARINGS {
            @Override
            public String getXMLString() {
                return "valid-with-warnings";
            }
        },
        INVALID {

            @Override
            public String getXMLString() {
                return "invalid";
            }
        },
        SKIPPED {
            @Override
            public String getXMLString() {
                return "skipped";
            }
            
        };
        
        public abstract String getXMLString();
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

            @Override
            public String getXMLString() {
                return "info";
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

            @Override
            public String getXMLString() {
                return "warning";
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

            @Override
            public String getXMLString() {
                return "error";
            }
        };

        public abstract String getShortcut();


        public abstract int priority();
        
        public abstract String getXMLString();
    } // enum Severity
    

    public enum Source {
        XML_PARSER {
            @Override
            public String getXMLString() {
                return "xml-parser";
            }
        },
        SCHEMATRON {
            @Override
            public String getXMLString() {
                return "schematron";
            }
        },
        EXTENSION {
            @Override
            public String getXMLString() {
                return "extension";
            }
        };

        public abstract String getXMLString();
    } // enum Source

    
    public static final class Message {
        private final Severity severity;
        private final Source source;
        private final int line;
        private final int col;
        private final String message;
        private final Throwable cause;


        Message(Severity severity, Source source, int line, int col,
                String message, Throwable cause) {
            this.severity = severity;
            this.source = source;
            this.line = line;
            this.col = col;
            this.message = message;
            this.cause = cause;
        }


        public Severity getSeverity() {
            return severity;
        }

        
        public Source getSource() {
            return source;
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
