package org.scassandra;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class VersionIntegrationTest {
    private static Scassandra scassandra = ScassandraFactory.createServer();

    @Test
    public void getVersion() {
        scassandra.start();
        String version = scassandra.serverVersion();
        assertEquals("0.6.0", version);
    }

}
