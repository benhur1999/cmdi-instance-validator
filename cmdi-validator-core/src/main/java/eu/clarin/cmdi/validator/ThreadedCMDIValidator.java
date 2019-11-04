package eu.clarin.cmdi.validator;
import java.util.ArrayList;
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
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.java.truevfs.access.TFile;


public class ThreadedCMDIValidator {
    private static final Logger logger =
            LoggerFactory.getLogger(ThreadedCMDIValidator.class);
    private final CMDIValidatorConfig config;
    private final CMDIValidationHandler handler;
    private final List<TFile> files;
    private final int threads;
    private AtomicReference<RunState> runState =
            new AtomicReference<>(RunState.INIT);
    private ExecutorService executor;
    private CoordinatorThread coordinator;


    public ThreadedCMDIValidator(final CMDIValidatorConfig config,
            final CMDIValidationHandler handler,
            final List<TFile> files,
            final int threads) {
        if (config == null) {
            throw new NullPointerException("config == null");
        }
        if (handler == null) {
            throw new NullPointerException("handler == null");
        }
        if (files == null) {
            throw new NullPointerException("files == null");
        }
        if (files.isEmpty()) {
            throw new IllegalArgumentException("files is empty");
        }
        if (threads < 1) {
            throw new IllegalArgumentException("threads < 1");
        }
        this.config = config;
        this.handler = handler;
        this.threads = threads;
        this.files = files;
    }


    public ThreadedCMDIValidator(final CMDIValidatorConfig config,
            final CMDIValidationHandler handler,
            final List<TFile> files) {
        this(config, handler, files, Runtime.getRuntime().availableProcessors());
    }

    
    public void start() throws CMDIValidatorInitException, CMDIValidatorException {
        if (runState.compareAndSet(RunState.INIT, RunState.STARTING)) {
            final FileEnumerator fileEnumerator =
                    new FileEnumerator(files, config.getFileFilter());
            final CMDIValidatorWorkerFactory workerFactory =
                    new CMDIValidatorWorkerFactory(config);

            final CountDownLatch goLatch = new CountDownLatch(1);

            this.coordinator = new CoordinatorThread(goLatch, threads);

            ThreadGroup workers = new ThreadGroup("workers");
            final ThreadFactory threadFactory = new ThreadFactory() {
                private int id = 0;


                @Override
                public Thread newThread(Runnable target) {
                    String name = String.format("worker-%02x", id++);
                    return new Thread(workers, target, name);
                }
            };
            executor = Executors.newFixedThreadPool(threads, threadFactory);

            final AtomicInteger workersActive = new AtomicInteger(threads);
            for (int i = 0; i < threads; i++) {
                final WorkerRunnable worker = new WorkerRunnable(goLatch,
                        workersActive, workerFactory.createWorker(),
                        fileEnumerator, coordinator.pendingQueue);
                executor.submit(worker);
            }

            if (runState.compareAndSet(RunState.STARTING, RunState.RUNNING)) {
                coordinator.start();
            } else {
                throw new IllegalStateException("UNEXPECTED STATE: " + runState.get());
            }
        } else {
            throw new IllegalStateException("invalid state: " + runState.get());
        }
    }


    public void shutdown() {
        logger.debug("shutdown validation processor");
        executor.shutdownNow();
        try {
            executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            /* IGNORE */
        }
        try {
            coordinator.join();
        } catch (InterruptedException e) {
            /* IGNORE */
        }
    }



    
    private enum RunState {
        INIT, STARTING, RUNNING, STOPPING, FINISHED
    }


    private final class CoordinatorThread extends Thread {
        private final CountDownLatch goLatch;
        private final BlockingQueue<CMDIValidationReport> pendingQueue;
        private final ArrayList<CMDIValidationReport> workQueue;
        private volatile boolean run = true;


        private CoordinatorThread(final CountDownLatch goLatch, final int threads) {
            super("report-writer");
            this.goLatch = goLatch;
            this.pendingQueue = new ArrayBlockingQueue<>(threads * 16);
            this.workQueue = new ArrayList<>(threads * 16 * 8);
        }


        @Override
        public void run() {
            logger.trace("report writer started");
            try {
                // signal start
                handler.onJobStarted();

                // start other threads
                goLatch.countDown();
                
                runLoop();
            } catch (CMDIValidatorException e) {
                logger.error("error start: ", e);
            }

            try {
                handler.onJobFinished(CMDILegacyValidator.Result.OK);
            } catch (CMDIValidatorException e) {
                logger.error("error finish: ", e);
            }

            runState.set(RunState.FINISHED);
            logger.trace("report writer terminated");
        }


        private void runLoop() {
            try {
                // loop for work ...
                while (run) {
                    try {
                        workQueue.add(pendingQueue.take());
                    } catch (InterruptedException e) {
                        /* IGNORE */
                    }

                    /* drain loop:
                     * even when we are supposed to terminate, we need to flush
                     * all queued validation reports  
                     */
                    for (;;) {
                        pendingQueue.drainTo(workQueue);
                        if (workQueue.isEmpty()) {
                            break;
                        }
                        logger.trace("writing {} report(s)", workQueue.size());
                        for (int i = 0; i < workQueue.size(); i++) {
                            handler.onValidationReport(workQueue.get(i));
                        }
                        workQueue.clear();
                    } // for (inner)
                } // for (outer)
            } catch (CMDIValidatorException e) {
                logger.error("validator exception in report writer", e);
            } catch (Throwable e) {
                logger.error("unexpected exception in report writer", e);
            }
        }


        private void allWorkersFinished() {
            logger.debug("all workers are finished");
            CoordinatorThread.this.interrupt();
//            if (runState.compareAndSet(RunState.RUNNING, RunState.STOPPING) || runState.) 
//            logger.debug("triggered shutdown");
//            if (!runState.compareAndSet(RunState.RUNNING,
//                    RunState.STOPPING)) {
//                if (!runState.compareAndSet(RunState.STOPPING,
//                        RunState.STOPPING)) {
//                    throw new RuntimeException("bad state: " + runState.get());
//                }
//            }
//
            run = false;
        }


        private void handleWorkerException(Throwable exception) {
            // FIXME
            logger.error("worker exception", exception);
        }

    }

    
    private final class WorkerRunnable
            implements Runnable, CMDIValidationReportSink {
        private final CountDownLatch goLatch;
        private final AtomicInteger workersActive;
        private final CMDIValidatorWorker worker;
        private final FileEnumerator fileEnumerator;
        private final BlockingQueue<CMDIValidationReport> reportsQueue;
        private int stalledCount = 0;
        private long stalledTime = 0;
        

        private WorkerRunnable(final CountDownLatch goLatch,
                final AtomicInteger workersActive,
                final CMDIValidatorWorker validationWorker,
                final FileEnumerator files,
                final BlockingQueue<CMDIValidationReport> reportsQueue) {
            this.goLatch = goLatch;
            this.workersActive = workersActive;
            this.worker = validationWorker;
            this.fileEnumerator = files;
            this.reportsQueue = reportsQueue;
        }


        @Override
        public void run() {
            try {
                // wait for start signal
                goLatch.await();
                
                logger.trace("validation worker started");

                // loop for work ...
                for (;;) {
                    // check for stop condition
                    if (Thread.currentThread().isInterrupted()) {
                        break;
                    }

                    TFile file = fileEnumerator.nextFile();
                    if (file == null) {
                        break;
                    }
                    worker.validate(file, this);
                } // for
            } catch (CMDIValidatorException e) {
                logger.error("validator exception in worker thread", e);
                coordinator.handleWorkerException(e);
            } catch (Throwable e) {
                logger.error("unexpected exception in worker thread", e);
                coordinator.handleWorkerException(e);
            } finally {
                logger.trace("validation worker terminated");
                logger.info("stalled count = {}, stalled time = {}",
                        stalledCount, stalledTime);
                if (workersActive.decrementAndGet() == 0) {
                    coordinator.allWorkersFinished();
                }
            }
        }


        @Override
        public void postReport(CMDIValidationReport report)
                throws CMDIValidatorException {
            if (!reportsQueue.offer(report)) {
                final long now = System.nanoTime();
                /*
                 * loop to make sure we'll put the report in the pending queue,
                 * even if interrupted
                 */
                for (;;) {
                    stalledCount++;
                    try {
                        reportsQueue.put(report);
                        break;
                    } catch (InterruptedException e) {
                        /* IGNORE */
                    }
                } // for
                final long delta = System.nanoTime() - now;
                stalledTime += TimeUnit.NANOSECONDS.toMillis(delta);
            }
        }
    }

} // ThreadedCMDIValidator
