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
import java.io.FileFilter;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import net.java.truevfs.access.TFile;


public final class CMDIValidator {
    public enum Result {
        OK, ABORTED, ERROR
    }
    private final CMDIValidatorWorkerFactory workerFactory;
    private final FileEnumerator files;
    private final CMDIValidationHandler handler;
    private final Map<Thread, ThreadContext> contexts =
            new ConcurrentHashMap<Thread, ThreadContext>();
    private final AtomicInteger threadsProcessing = new AtomicInteger();
    private State state = State.INIT;
    private Result result = null;

    
    public CMDIValidator(final CMDIValidatorConfig config)
            throws CMDIValidatorInitException {
        this(config, config.getRoot(), config.getHandler());
    }


    public CMDIValidator(final CMDIValidatorConfig config, final File src,
            CMDIValidationHandler handler) throws CMDIValidatorInitException {
        if (config == null) {
            throw new NullPointerException("config == null");
        }

        this.workerFactory = new CMDIValidatorWorkerFactory(config);

        /*
         * other stuff
         */
        final TFile root = new TFile(src);
        this.files       = new FileEnumerator(root, config.getFileFilter());
        if (config.getHandler() == null) {
            throw new NullPointerException("handler == null");
        }
        this.handler = handler;
    }


    public CMDIValidationHandler getHandler() {
        return handler;
    }


    public void abort() {
        synchronized (this) {
            if ((state == State.INIT) || (state == State.RUN)) {
                state = State.DONE;
                files.flush();
                if (result == null) {
                    result = Result.ABORTED;
                }
            }
        } // synchronized (this)
    }


    boolean processOneFile() throws CMDIValidatorException {
        try {
            TFile file = null;
            boolean done;

            threadsProcessing.incrementAndGet();

            synchronized (this) {
                switch (state) {
                case INIT:
                    try {
                        state = State.RUN;
                        handler.onJobStarted();
                    } catch (CMDIValidatorException e) {
                        state = State.DONE;
                        throw e;
                    }
                    /* FALL-THROUGH */
                case RUN:
                    file = files.nextFile();
                    if (files.isEmpty() && (state == State.RUN)) {
                        state = State.DONE;
                    }
                    break;
                default:
                    // ignore
                }

                done = (state == State.DONE);
            } // synchronized (this)

            if (file != null) {
                ThreadContext context = contexts.get(Thread.currentThread());
                if (context == null) {
                    context = new ThreadContext();
                    contexts.put(Thread.currentThread(), context);
                }
                context.validate(file);
            }

            return done;
        } catch (Throwable e) {
            synchronized (this) {
                state = State.DONE;
                if (result == null) {
                    result = Result.ERROR;
                }
            } // synchronized (this)
            if (e instanceof CMDIValidatorException) {
                throw (CMDIValidatorException) e;
            } else {
                throw new CMDIValidatorException(
                        "an unexpected error occurred", e);
            }
        } finally {
            if (threadsProcessing.decrementAndGet() <= 0) {
                synchronized (this) {
                    if (state == State.DONE) {
                        state = State.FINI;
                        if (result == null) {
                            result = Result.OK;
                        }

                        // notify handler
                        handler.onJobFinished(result);
                    }
                } // synchronized (this)
            }
        }
    }




    private enum State {
        INIT, RUN, DONE, FINI;
    }


    private final class ThreadContext implements CMDIValidationReportSink {
        private final CMDIValidatorWorker worker;

        private ThreadContext() {
            this.worker = workerFactory.createWorker();
        }


        private void validate(final TFile file) throws CMDIValidatorException {
            worker.validate(file, this);
        }


        @Override
        public void postReport(CMDIValidationReport report)
                throws CMDIValidatorException {
            if (handler != null) {
                handler.onValidationReport(report);
            }
        }

    }


    private static final class FileEnumerator {
        private final class FileList {
            private final TFile[] fileList;
            private int idx = 0;


            private FileList(TFile[] fileList) {
                this.fileList = fileList;
            }


            private TFile nextFile() {
                while (idx < fileList.length) {
                    final TFile file = fileList[idx++];
                    if (file.isDirectory()) {
                        return file;
                    }
                    if ((filter != null) && !filter.accept(file)) {
                        continue;
                    }
                    return file;
                } // while
                return null;
            }

            private int size() {
                return (fileList.length - idx);
            }
        }
        private final FileFilter filter;
        private final LinkedList<FileList> stack =
                new LinkedList<FileList>();


        FileEnumerator(TFile root, FileFilter filter) {
            if (root == null) {
                throw new NullPointerException("root == null");
            }
            if (root.isDirectory()) {
                pushDirectory(root);
            } else {
                stack.add(new FileList(new TFile[] { root }));
            }
            this.filter = filter;
        }


        boolean isEmpty() {
            return stack.isEmpty();
        }


        TFile nextFile() {
            for (;;) {
                if (stack.isEmpty()) {
                    break;
                }
                final FileList list = stack.peek();
                final TFile file = list.nextFile();
                if ((list.size() == 0) || (file == null)) {
                    stack.pop();
                    if (file == null) {
                        continue;
                    }
                }
                if (file.isDirectory()) {
                    pushDirectory(file);
                    continue;
                }
                return file;
            }
            return null;
        }


        void flush() {
            stack.clear();
        }


        private void pushDirectory(TFile directory) {
            final TFile[] files = directory.listFiles();
            if ((files != null) && (files.length > 0)) {
                stack.push(new FileList(files));
            }
        }

    } // class FileEnumerator



} // class CMDIValidator
