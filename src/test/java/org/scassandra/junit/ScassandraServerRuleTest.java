package org.scassandra.junit;

import com.datastax.driver.core.Cluster;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.scassandra.http.client.PrimingRequest;

import static org.junit.Assert.assertEquals;

public class ScassandraServerRuleTest {

    private static int binaryPort = 8042;
    private static int adminPort = 8043;

    @ClassRule
    public static ScassandraServerRule rule = new ScassandraServerRule(binaryPort, adminPort);

    @Rule
    public ScassandraServerRule clearRule = rule;

    @BeforeClass
    public static void primeAQueryToMakeSureItIsCleared() {
        rule.primingClient().primeQuery(PrimingRequest.queryBuilder().withQuery("").build());
        rule.primingClient().primePreparedStatement(
                PrimingRequest.preparedStatementBuilder().withQuery("").build()
        );

        Cluster cluster = Cluster.builder().addContactPoint("localhost").withPort(binaryPort).build();
        cluster.connect();
    }

    @Test
    public void ruleStartsScassandaBeforeAnyTestRuns() {
        // basically any of these should not throw an exception
        rule.activityClient().clearPreparedStatementExecutions();
    }

    @Test
    public void primingClientClearedBeforeEachTest() {
        assertEquals("Primed queries haven't been cleared between tests",
                0, rule.primingClient().retrieveQueryPrimes().size());

        assertEquals("Primed prepared statements haven't been cleared between tests",
                0, rule.primingClient().retrievePreparedPrimes().size());
    }

    @Test
    public void clearsConnectionsBetweenTests() {
        assertEquals("Connections haven't been cleared between tests",
                0, rule.activityClient().retrieveConnections().size());
    }

}