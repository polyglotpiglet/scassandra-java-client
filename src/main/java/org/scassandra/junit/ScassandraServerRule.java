package org.scassandra.junit;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.scassandra.Scassandra;
import org.scassandra.ScassandraFactory;
import org.scassandra.http.client.ActivityClient;
import org.scassandra.http.client.PrimingClient;

/**
 * ClassRule: Starts scassandra before the tests run and stops scassandra when all tests have finished.
 *
 * Rule: Clears all primes and recorded activity between each test.
 */
public class ScassandraServerRule implements TestRule {

    private Scassandra scassandra;
    private boolean started = false;

    public ScassandraServerRule(){
        scassandra = ScassandraFactory.createServer();
    }

    public ScassandraServerRule(int binaryPort, int adminPort){
        scassandra = ScassandraFactory.createServer(binaryPort, adminPort);
    }

    @Override
    public Statement apply(final Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                if (!started) {
                    started = true;
                    scassandra.start();
                    try {
                        base.evaluate();
                    } finally {
                        scassandra.stop();
                    }
                } else {
                    primingClient().clearAllPrimes();
                    activityClient().clearAllRecordedActivity();
                    base.evaluate();
                }
            }
        };
    }

    public PrimingClient primingClient() {
        return scassandra.primingClient();
    }

    public ActivityClient activityClient() {
        return scassandra.activityClient();
    }
}
