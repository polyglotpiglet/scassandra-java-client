package org.scassandra;

import com.google.common.collect.ImmutableMap;
import org.junit.*;
import org.scassandra.http.client.*;

import java.util.*;

import static org.junit.Assert.assertEquals;

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
                .withAdminPort(adminPort).build();

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
        primingClient.clearQueryPrimes();
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
        List<PrimingRequest> primes = primingClient.retrieveQueryPrimes();
        //then
        PrimingRequest expectedPrimeWithDefaults = PrimingRequest.queryBuilder()
                .withQuery("select * from people")
                .withRows(row)
                .withColumnTypes(ImmutableMap.of("name", ColumnTypes.Varchar))
                .withResult(PrimingRequest.Result.success)
                .withConsistency(PrimingRequest.Consistency.ALL, PrimingRequest.Consistency.ANY)
                .build();
        assertEquals(1, primes.size());
        assertEquals(expectedPrimeWithDefaults, primes.get(0));
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
}
