package org.scassandra;

import uk.co.scassandra.ServerStubRunner;

class ScassandraRunner implements Scassandra {

    private final int binaryPort;
    private final int adminPort;
    private final ServerStubRunner serverStubRunner;

    public ScassandraRunner(int binaryPort, int adminPort) {
        this.binaryPort = binaryPort;
        this.adminPort = adminPort;
        serverStubRunner = new ServerStubRunner(binaryPort, adminPort);
    }

    @Override
    public void start() {
        new Thread() {
            public void run() {
                serverStubRunner.start();
            }
        }.start();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
    }

    @Override
    public void stop() {
        serverStubRunner.shutdown();
    }

}


