package org.scassandra.http.client;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;

public class ConnectionTest {

    @Test
    public void testEqualsContract() {
        EqualsVerifier.forClass(Connection.class).verify();
    }
}
