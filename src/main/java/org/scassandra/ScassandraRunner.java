package org.scassandra;

import org.scassandra.http.client.ActivityClient;
import org.scassandra.http.client.PrimingClient;
import uk.co.scassandra.ServerStubRunner;

class ScassandraRunner implements Scassandra {

    private final ServerStubRunner serverStubRunner;
    private final PrimingClient primingClient;
    private final ActivityClient activityClient;

    ScassandraRunner(int binaryPort, int adminPort) {
        serverStubRunner = new ServerStubRunner(binaryPort, adminPort);
        primingClient = PrimingClient.builder().withPort(adminPort).build();
        activityClient = ActivityClient.builder().withPort(adminPort).build();

    }

    @Override
    public PrimingClient primingClient() {
        return this.primingClient;
    }

    @Override
    public ActivityClient activityClient() {
        return this.activityClient;
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


