package org.scassandra;

/**
 * Factory for creating Scassandra instances.
 */
public class ScassandraFactory {
    /**
     * @param binaryPort Cassandra binary port
     * @param adminPort Scassandra admin port
     * @return
     */
    public static final Scassandra createServer(int binaryPort, int adminPort) {
        return new ScassandraRunner(binaryPort, adminPort);
    }

    /**
     * Creates a Scassandra instance with 8042 as the binary port and 8043 as the admin port.
     * @return Scassandra
     */
    public static final Scassandra createServer() {
        return new ScassandraRunner(8042, 8043);
    }
}
