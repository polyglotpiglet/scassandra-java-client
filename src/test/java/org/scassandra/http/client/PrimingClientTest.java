package org.scassandra.http.client;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.junit.Rule;
import org.junit.Test;

import java.util.*;

public class PrimingClientTest {

    private static final int PORT = 1234;

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(PORT);

    @Test
    public void testPrimingEmptyResults() {
        //given
        stubFor(post(urlEqualTo("/prime")).willReturn(aResponse().withStatus(200)));
        PrimingClient pc = new PrimingClient("localhost", PORT);
        PrimingRequest pr = new PrimingRequest("select * from people", Collections.emptyList());
        //when
        pc.prime(pr);
        //then
        verify(postRequestedFor(urlEqualTo("/prime"))
                .withHeader("Content-Type", equalTo("application/json; charset=UTF-8"))
                .withRequestBody(equalTo("{\"when\":\"select * from people\",\"then\":[]}")));
    }

    @Test
    public void testPrimingWithMultipleRows() {
        //given
        stubFor(post(urlEqualTo("/prime")).willReturn(aResponse().withStatus(200)));
        PrimingClient pc = new PrimingClient("localhost", PORT);
        List<Map<String,String>> rows = new ArrayList<>();
        Map<String, String> row = new HashMap<>();
        row.put("name","Chris");
        rows.add(row);
        PrimingRequest pr = new PrimingRequest("select * from people", rows);
        //when
        pc.prime(pr);
        //then
        verify(postRequestedFor(urlEqualTo("/prime"))
                .withHeader("Content-Type", equalTo("application/json; charset=UTF-8"))
                .withRequestBody(equalTo("{\"when\":\"select * from people\",\"then\":{\"rows\":[{\"name\":\"Chris\"}],\"result\":\"success\"}}")));
    }

    @Test(expected = PrimeFailedException.class)
    public void testPrimeFailed() {
        //given
        stubFor(post(urlEqualTo("/prime")).willReturn(aResponse().withStatus(500)));
        PrimingClient pc = new PrimingClient("localhost", PORT);
        PrimingRequest pr = new PrimingRequest("select * from people", Collections.emptyList());
        //when
        pc.prime(pr);
        //then

    }
}
