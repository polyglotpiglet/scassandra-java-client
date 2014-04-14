package org.scassandra;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.scassandra.http.client.ActivityClient;
import org.scassandra.http.client.PrimeFailedException;
import org.scassandra.http.client.PrimingClient;
import org.scassandra.http.client.PrimingRequest;

import java.util.Collections;

public class IntegrationTest {

    private static int binaryPort = 2345;
    private static int adminPort = 3456;
    public static final Scassandra SERVER = ScassandraFactory.createServer(binaryPort, adminPort);
    private ActivityClient ac;
    private PrimingClient pc;

    @BeforeClass
    public static void startScassandra() {
        SERVER.start();
    }

    @AfterClass
    public static void stopScassandra() {
        SERVER.stop();
    }

    @Before
    public void setup() {
        ac = new ActivityClient("localhost", adminPort);
        pc = new PrimingClient("localhost", adminPort);
        ac.clearConnections();
        ac.clearQueries();
        pc.clearPrimes();
    }

    @Test
    public void clientsShouldBeAbleToConnect() {
        //given
        //when
        ActivityClient ac = new ActivityClient("localhost", adminPort);
        PrimingClient pc = new PrimingClient("localhost", adminPort);
        PrimingRequest pr = PrimingRequest.builder()
                .withQuery("select * from people")
                .withResult(PrimingRequest.Result.read_request_timeout)
                .build();

        //then
        ac.clearConnections();
        pc.prime(pr);
    }

    @Test(expected = PrimeFailedException.class)
    public void testConflictingConsistencies() {
        //given
        ActivityClient ac = new ActivityClient("localhost", adminPort);
        PrimingClient pc = new PrimingClient("localhost", adminPort);

        PrimingRequest prWithAllAndAny = PrimingRequest.builder()
                .withQuery("select * from people")
                .withConsistency(PrimingRequest.Consistency.ALL, PrimingRequest.Consistency.ANY)
                .build();
        PrimingRequest prWithAllAndONE = PrimingRequest.builder()
                .withQuery("select * from people")
                .withConsistency(PrimingRequest.Consistency.ALL, PrimingRequest.Consistency.ONE)
                .build();
        //when
        pc.prime(prWithAllAndAny);
        pc.prime(prWithAllAndONE);

        //then
    }
}
