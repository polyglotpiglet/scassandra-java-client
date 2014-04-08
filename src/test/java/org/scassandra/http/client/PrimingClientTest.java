package org.scassandra.http.client;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

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
        PrimingRequest pr = new PrimingRequest("select * from people", Collections.<Map<String, String>>emptyList());
        //when
        underTest.prime(pr);
        //then
        verify(postRequestedFor(urlEqualTo("/prime"))
                .withHeader("Content-Type", equalTo("application/json; charset=UTF-8"))
                .withRequestBody(equalTo("{\"when\":\"select * from people\",\"then\":{\"rows\":[],\"result\":\"success\"}}")));
    }

    @Test
    public void testPrimingWithMultipleRows() {
        //given
        stubFor(post(urlEqualTo("/prime")).willReturn(aResponse().withStatus(200)));
        List<Map<String,String>> rows = new ArrayList<>();
        Map<String, String> row = new HashMap<>();
        row.put("name","Chris");
        rows.add(row);
        PrimingRequest pr = new PrimingRequest("select * from people", rows);
        //when
        underTest.prime(pr);
        //then
        verify(postRequestedFor(urlEqualTo("/prime"))
                .withHeader("Content-Type", equalTo("application/json; charset=UTF-8"))
                .withRequestBody(equalTo("{\"when\":\"select * from people\",\"then\":{\"rows\":[{\"name\":\"Chris\"}],\"result\":\"success\"}}")));
    }

    @Test
    public void testPrimingReadRequestTimeout() {
        //given
        stubFor(post(urlEqualTo("/prime")).willReturn(aResponse().withStatus(200)));
        PrimingRequest pr = new PrimingRequest("select * from people", PrimingRequest.Result.read_request_timeout);
        //when
        underTest.prime(pr);
        //then
        verify(postRequestedFor(urlEqualTo("/prime"))
                .withHeader("Content-Type", equalTo("application/json; charset=UTF-8"))
                .withRequestBody(equalTo("{\"when\":\"select * from people\",\"then\":{\"result\":\"read_request_timeout\"}}")));
    }

    @Test
    public void testPrimingUnavailableException() {
        //given
        stubFor(post(urlEqualTo("/prime")).willReturn(aResponse().withStatus(200)));
        PrimingClient pc = new PrimingClient("localhost", PORT);
        PrimingRequest pr = new PrimingRequest("select * from people", PrimingRequest.Result.unavailable);
        //when
        pc.prime(pr);
        //then
        verify(postRequestedFor(urlEqualTo("/prime"))
                .withHeader("Content-Type", equalTo("application/json; charset=UTF-8"))
                .withRequestBody(equalTo("{\"when\":\"select * from people\",\"then\":{\"result\":\"unavailable\"}}")));
    }

    @Test
    public void testPrimingWriteRequestTimeout() {
        //given
        stubFor(post(urlEqualTo("/prime")).willReturn(aResponse().withStatus(200)));
        PrimingRequest pr = new PrimingRequest("select * from people", PrimingRequest.Result.write_request_timeout);
        //when
        underTest.prime(pr);
        //then
        verify(postRequestedFor(urlEqualTo("/prime"))
                .withHeader("Content-Type", equalTo("application/json; charset=UTF-8"))
                .withRequestBody(equalTo("{\"when\":\"select * from people\",\"then\":{\"result\":\"write_request_timeout\"}}")));
    }

    @Test(expected = PrimeFailedException.class)
    public void testPrimeFailed() {
        //given
        stubFor(post(urlEqualTo("/prime")).willReturn(aResponse().withStatus(500)));
        PrimingRequest pr = new PrimingRequest("select * from people", Collections.<Map<String, String>>emptyList());
        //when
        underTest.prime(pr);
        //then
    }

}
