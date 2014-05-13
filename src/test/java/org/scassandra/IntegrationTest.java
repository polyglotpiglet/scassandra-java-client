package org.scassandra;

import org.junit.*;
import org.scassandra.http.client.ActivityClient;
import org.scassandra.http.client.PrimeFailedException;
import org.scassandra.http.client.PrimingClient;
import org.scassandra.http.client.PrimingRequest;

import java.util.*;

import static org.junit.Assert.assertEquals;

/*
TODO: Break up into multiple integration tests.
 */
public class IntegrationTest {

    private static int binaryPort = 2345;
    private static int adminPort = 3456;
    public static final Scassandra SERVER = ScassandraFactory.createServer(binaryPort, adminPort);
    private static ActivityClient activityClient;
    private static PrimingClient primingClient;

    @BeforeClass
    public static void startScassandra() {
        SERVER.start();

        activityClient = ActivityClient.builder()
                .withHost("localhost")
                .withPort(adminPort).build();

        primingClient = PrimingClient.builder()
                .withHost("localhost")
                .withPort(adminPort).build();
    }

    @AfterClass
    public static void stopScassandra() {
        SERVER.stop();
    }

    @Before
    public void setup() {
        activityClient.clearConnections();
        activityClient.clearQueries();
        primingClient.clearPrimes();
    }

    @Test
    public void clientsShouldBeAbleToConnect() {
        //given
        //when
        PrimingRequest pr = PrimingRequest.queryBuilder()
                .withQuery("select * from people")
                .withResult(PrimingRequest.Result.read_request_timeout)
                .build();

        //then
        activityClient.clearConnections();
        primingClient.primeQuery(pr);
    }

    @Test(expected = PrimeFailedException.class)
    public void testQueryPrimeConflictingConsistencies() {
        //given
        PrimingRequest prWithAllAndAny = PrimingRequest.queryBuilder()
                .withQuery("select * from people")
                .withConsistency(PrimingRequest.Consistency.ALL, PrimingRequest.Consistency.ANY)
                .build();
        PrimingRequest prWithAllAndONE = PrimingRequest.queryBuilder()
                .withQuery("select * from people")
                .withConsistency(PrimingRequest.Consistency.ALL, PrimingRequest.Consistency.ONE)
                .build();
        //when
        primingClient.primeQuery(prWithAllAndAny);
        primingClient.primeQuery(prWithAllAndONE);

        //then
    }

    @Test
    public void testQueryPrimeAndRetrieveOfPrime() {
        //given
        Map<String, Object> row = new HashMap<String, Object>();
        row.put("name", "chris");
        PrimingRequest prime = PrimingRequest.queryBuilder()
                .withQuery("select * from people")
                .withRows(row)
                .withResult(PrimingRequest.Result.success)
                .withConsistency(PrimingRequest.Consistency.ALL, PrimingRequest.Consistency.ANY)
                .build();

        //when
        primingClient.primeQuery(prime);
        List<PrimingRequest> primes = primingClient.retrievePrimes();
        //then
        assertEquals(1, primes.size());
        assertEquals(prime, primes.get(0));
    }

    @Test
    public void testPreparedPrime() {
        //given
        Map<String, String> row = new HashMap<String, String>();
        row.put("name", "chris");
        PrimingRequest prime = PrimingRequest.queryBuilder()
                .withQuery("select * from people where name = ?")
                .withRows(row)
                .build();

        //when
        primingClient.primePreparedStatement(prime);
        //then
    }

    @Test
    public void testActivityRetrieveOfPreparedStatementExecutions() throws Exception {
        //given
        //when
        activityClient.retrievePreparedStatementExecutions();
        //then
    }
}
