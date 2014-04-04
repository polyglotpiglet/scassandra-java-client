package org.scassandra;

public class ScassandraFactory {
    public static final Scassandra createServer(int binaryPort, int adminPort) {
        return new ScassandraRunner(binaryPort, adminPort);
    }
}
