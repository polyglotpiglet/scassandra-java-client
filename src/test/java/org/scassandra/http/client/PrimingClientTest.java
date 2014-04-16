package org.scassandra.http.client;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.Assert.assertEquals;

import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.*;

public class PrimingClientTest {

    private static final int PORT = 1234;

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(PORT);

    private PrimingClient underTest;

    @Before
    public void setup() {
        underTest = new PrimingClient("localhost", PORT);
    }


    @Test
    public void testPrimingEmptyResults() {
        //given
        stubFor(post(urlEqualTo("/prime")).willReturn(aResponse().withStatus(200)));
        PrimingRequest pr = PrimingRequest.builder()
                .withQuery("select * from people")
                .withRows(Collections.<Map<String, String>>emptyList())
                .build();
        //when
        underTest.prime(pr);
        //then
        verify(postRequestedFor(urlEqualTo("/prime"))
                .withHeader("Content-Type", equalTo("application/json; charset=UTF-8"))
                .withRequestBody(equalTo("{\"when\":{\"query\":\"select * from people\"},\"then\":{\"rows\":[],\"result\":\"success\"}}")));
    }

    @Test
    public void testPrimingWithMultipleRows() {
        //given
        stubFor(post(urlEqualTo("/prime")).willReturn(aResponse().withStatus(200)));
        List<Map<String,String>> rows = new ArrayList<>();
        Map<String, String> row = new HashMap<>();
        row.put("name","Chris");
        rows.add(row);
        PrimingRequest pr = PrimingRequest.builder()
                .withQuery("select * from people")
                .withRows(rows)
                .build();
        //when
        underTest.prime(pr);
        //then
        verify(postRequestedFor(urlEqualTo("/prime"))
                .withHeader("Content-Type", equalTo("application/json; charset=UTF-8"))
                .withRequestBody(equalTo("{\"when\":{\"query\":\"select * from people\"},\"then\":{\"rows\":[{\"name\":\"Chris\"}],\"result\":\"success\"}}")));
    }

    @Test
    public void testPrimingReadRequestTimeout() {
        //given
        stubFor(post(urlEqualTo("/prime")).willReturn(aResponse().withStatus(200)));
        PrimingRequest pr = PrimingRequest.builder()
                .withQuery("select * from people")
                .withResult(PrimingRequest.Result.read_request_timeout)
                .build();
        //when
        underTest.prime(pr);
        //then
        verify(postRequestedFor(urlEqualTo("/prime"))
                .withHeader("Content-Type", equalTo("application/json; charset=UTF-8"))
                .withRequestBody(equalTo("{\"when\":{\"query\":\"select * from people\"},\"then\":{\"result\":\"read_request_timeout\"}}")));
    }

    @Test
    public void testPrimingUnavailableException() {
        //given
        stubFor(post(urlEqualTo("/prime")).willReturn(aResponse().withStatus(200)));
        PrimingClient pc = new PrimingClient("localhost", PORT);
        PrimingRequest pr = PrimingRequest.builder()
                .withQuery("select * from people")
                .withResult(PrimingRequest.Result.unavailable)
                .build();
        //when
        pc.prime(pr);
        //then
        verify(postRequestedFor(urlEqualTo("/prime"))
                .withHeader("Content-Type", equalTo("application/json; charset=UTF-8"))
                .withRequestBody(equalTo("{\"when\":{\"query\":\"select * from people\"},\"then\":{\"result\":\"unavailable\"}}")));
    }

    @Test
    public void testPrimingWriteRequestTimeout() {
        //given
        stubFor(post(urlEqualTo("/prime")).willReturn(aResponse().withStatus(200)));
        PrimingRequest pr = PrimingRequest.builder()
                .withQuery("select * from people")
                .withResult(PrimingRequest.Result.write_request_timeout)
                .build();
        //when
        underTest.prime(pr);
        //then
        verify(postRequestedFor(urlEqualTo("/prime"))
                .withHeader("Content-Type", equalTo("application/json; charset=UTF-8"))
                .withRequestBody(equalTo("{\"when\":{\"query\":\"select * from people\"},\"then\":{\"result\":\"write_request_timeout\"}}")));
    }

    @Test(expected = PrimeFailedException.class)
    public void testPrimeFailed() {
        //given
        stubFor(post(urlEqualTo("/prime")).willReturn(aResponse().withStatus(500)));
        PrimingRequest pr = PrimingRequest.builder()
                .withQuery("select * from people")
                .withResult(PrimingRequest.Result.read_request_timeout)
                .build();
        //when
        underTest.prime(pr);
        //then
    }

    @Test
    public void testPrimingConsistency() {
        //given
        stubFor(post(urlEqualTo("/prime")).willReturn(aResponse().withStatus(200)));
        PrimingRequest pr = PrimingRequest.builder()
                .withQuery("select * from people")
                .withConsistency(PrimingRequest.Consistency.ALL, PrimingRequest.Consistency.ONE)
                .build();

        //when
        underTest.prime(pr);

        //then
        verify(postRequestedFor(urlEqualTo("/prime"))
                .withHeader("Content-Type", equalTo("application/json; charset=UTF-8"))
                .withRequestBody(equalTo("{\"when\":{\"query\":\"select * from people\",\"consistency\":[\"ALL\",\"ONE\"]},\"then\":{\"rows\":[],\"result\":\"success\"}}")));

    }

    @Test
    public void testRetrieveOfPreviousPrimes() {
        //given
        Map<String, String> rows = new HashMap<>();
        rows.put("name","Chris");
        PrimingRequest pr = PrimingRequest.builder()
                .withQuery("select * from people")
                .withRows(Arrays.asList(rows))
                .build();
        stubFor(get(urlEqualTo("/prime")).willReturn(aResponse().withStatus(200).withBody(
                "[{\n" +
                        "  \"when\": {\n" +
                        "    \"query\": \"select * from people\"\n" +
                        "  },\n" +
                        "  \"then\": {\n" +
                        "    \"rows\": [{\n" +
                        "      \"name\": \"Chris\"\n" +
                        "    }],\n" +
                        "    \"result\":\"success\""+
                        "  }\n" +
                        "}]"
        )));
        //when
        List<PrimingRequest> primingRequests = underTest.retrievePrimes();
        //then
        assertEquals(1, primingRequests.size());
        assertEquals(pr, primingRequests.get(0));
    }

    @Test
    public void testDeletingOfPrimes() {
        //given
        stubFor(delete(urlEqualTo("/prime")).willReturn(aResponse().withStatus(200)));
        //when
        underTest.clearPrimes();
        //then
        verify(deleteRequestedFor(urlEqualTo("/prime")));
    }

    @Test(expected = PrimeFailedException.class)
    public void testDeletingOfPrimesFailedDueToStatusCode() {
        //given
        stubFor(delete(urlEqualTo("/prime")).willReturn(aResponse().withStatus(500)));
        //when
        underTest.clearPrimes();
        //then
    }
    @Test(expected = PrimeFailedException.class)
    public void testRetrievingOfPrimesFailedDueToStatusCode() {
        //given
        stubFor(get(urlEqualTo("/prime")).willReturn(aResponse().withStatus(500)));
        //when
        underTest.retrievePrimes();
        //then
    }

    @Test(expected = PrimeFailedException.class)
    public void testDeletingOfPrimesFailed() {
        //given
        stubFor(delete(urlEqualTo("/prime")).willReturn(aResponse().withFault(Fault.RANDOM_DATA_THEN_CLOSE)));
        //when
        underTest.clearPrimes();
        //then
    }
    @Test(expected = PrimeFailedException.class)
    public void testRetrievingOfPrimesFailed() {
        //given
        stubFor(get(urlEqualTo("/prime")).willReturn(aResponse().withFault(Fault.RANDOM_DATA_THEN_CLOSE)));
        //when
        underTest.retrievePrimes();
        //then
    }

}
