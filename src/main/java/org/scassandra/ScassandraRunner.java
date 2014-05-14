package org.scassandra;

import uk.co.scassandra.ServerStubRunner;

class ScassandraRunner implements Scassandra {

    private final ServerStubRunner serverStubRunner;

    public ScassandraRunner(int binaryPort, int adminPort) {
        serverStubRunner = new ServerStubRunner(binaryPort, adminPort);
    }

    @Override
    public void start() {
        serverStubRunner.start();
        // The above start is async. Once scassandra offers a way to block until it is ready
        // we can remove this sleep.
        // See https://github.com/scassandra/scassandra-server/issues/10
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
        }
    }

    @Override
    public void stop() {
        serverStubRunner.shutdown();
    }

}


