package eu.clarin.cmdi.validator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ThreadedCMDIValidatorProcessor implements CMDIValidatorProcessor {
    private static final Logger logger =
            LoggerFactory.getLogger(ThreadedCMDIValidatorProcessor.class);
    private final int threads;
    private final List<CMDIValidator> validators =
            new ArrayList<CMDIValidator>();
    private int idx = 0;
    private boolean running = false;
    private ThreadGroup workers;
    private ExecutorService executor;


    public ThreadedCMDIValidatorProcessor() {
        this(Runtime.getRuntime().availableProcessors());
    }


    public ThreadedCMDIValidatorProcessor(final int threads) {
        if (threads < 1) {
            throw new IllegalArgumentException("threads < 1");
        }
        this.threads = threads;
    }


    public synchronized void start() {
        if (running) {
            throw new IllegalStateException("engine already started");
        }
        workers = new ThreadGroup("Validation-Workers");
        final ThreadFactory threadFactory = new ThreadFactory() {
            private int id = 0;
            @Override
            public Thread newThread(Runnable target) {
                final String name = String.format("worker-%02x", id++);
                return new Thread(workers, target, name);
            }
        };
        final CountDownLatch goLatch = new CountDownLatch(1);
        executor = Executors.newFixedThreadPool(threads, threadFactory);
        for (int i = 0; i < threads; i++) {
            final Worker worker = new Worker(goLatch);
            executor.submit(worker);
        }
        goLatch.countDown();
        running = true;
    }


    public synchronized void shutdown() {
        logger.debug("shutdown validation processor");
        executor.shutdownNow();
        try {
            executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            /* IGNORE */
        }
    }


    @Override
    public void process(final CMDIValidator validator)
            throws CMDIValidatorException {
        if (validator == null) {
            throw new NullPointerException("validator == null");
        }
        synchronized (validators) {
            if (validators.contains(validator)) {
                throw new CMDIValidatorException("already processing this validator");
            }

            /* enqueue work ... */
            validators.add(validator);

            /* ... and let the waiting herd stampede. */
            validators.notifyAll();
        } // synchronized (validators)
    }



    private class Worker implements Runnable {
        private final CountDownLatch goLatch;

        private Worker(final CountDownLatch goLatch) {
            this.goLatch = goLatch;
        }


        @Override
        public void run() {
            try {
                // wait for start signal
                goLatch.await();

                // loop for work ...
                boolean done = false;
                CMDIValidator validator = null;
                for (;;) {
                    if (Thread.currentThread().isInterrupted()) {
                        break;
                    }
                    synchronized (validators) {
                        if (done && (validator != null)) {
                            validators.remove(validator);
                        }
                        if (!validators.isEmpty()) {
                            if (idx >= validators.size()) {
                                idx = 0;
                            }
                            validator = validators.get(idx++);
                        } else {
                            validator = null;
                            validators.wait();
                        }
                    } // synchronized (validators)

                    // got something to do ... proceed
                    if (validator != null) {
                        try {
                            done = validator.processOneFile();
                        } catch (CMDIValidatorException e) {
                            logger.error("error processing validator: {}",
                                    e.getMessage(), e);
                            synchronized (validators) {
                                validators.remove(validator);
                                validator = null;
                            } // synchronized (validators)
                        }
                    }
                } // for
            } catch (InterruptedException e) {
                /* IGNORE */
            } catch (Throwable e) {
                logger.error("unexpected error in worker thread: {}",
                        e.getMessage(), e);
            }
        }
    }

} // class ThreadedCMDIValidatorProcessor

