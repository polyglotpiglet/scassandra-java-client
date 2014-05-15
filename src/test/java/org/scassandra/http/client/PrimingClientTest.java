package org.scassandra.http.client;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.Assert.assertEquals;

import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.*;

public class PrimingClientTest {

    private static final int PORT = 1234;

    public static final String PRIME_PREPARED_PATH = "/prime-prepared-single";
    public static final String PRIME_QUERY_PATH = "/prime-query-single";

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(PORT);

    private PrimingClient underTest;

    @Before
    public void setup() {
        underTest = PrimingClient.builder().withHost("localhost").withPort(PORT).build();
    }

    @Test
    public void testPrimingQueryEmptyResults() {
        //given
        stubFor(post(urlEqualTo(PRIME_QUERY_PATH)).willReturn(aResponse().withStatus(200)));
        PrimingRequest pr = PrimingRequest.queryBuilder()
                .withQuery("select * from people")
                .withRows(Collections.<Map<String, ?>>emptyList())
                .build();
        //when
        underTest.primeQuery(pr);
        //then
        verify(postRequestedFor(urlEqualTo(PRIME_QUERY_PATH))
                .withHeader("Content-Type", equalTo("application/json; charset=UTF-8"))
                .withRequestBody(equalToJson("{\"when\":{\"query\":\"select * from people\"}," +
                        " \"then\":{\"rows\":[],\"result\":\"success\"}}")));
    }

    @Test
    public void testPrimingQueryWithRow() {
        //given
        stubFor(post(urlEqualTo(PRIME_QUERY_PATH)).willReturn(aResponse().withStatus(200)));
        Map<String, String> row = new HashMap<String, String>();
        row.put("name", "Chris");
        PrimingRequest pr = PrimingRequest.queryBuilder()
                .withQuery("select * from people")
                .withRows(row)
                .build();
        //when
        underTest.primeQuery(pr);
        //then
        verify(postRequestedFor(urlEqualTo(PRIME_QUERY_PATH))
                .withHeader("Content-Type", equalTo("application/json; charset=UTF-8"))
                .withRequestBody(equalToJson("{\"when\":{\"query\":\"select * from people\"}," +
                        "\"then\":{\"rows\":[{\"name\":\"Chris\"}],\"result\":\"success\"}}")));
    }

    @Test
    public void testPrimingQueryReadRequestTimeout() {
        //given
        stubFor(post(urlEqualTo(PRIME_QUERY_PATH)).willReturn(aResponse().withStatus(200)));
        PrimingRequest pr = PrimingRequest.queryBuilder()
                .withQuery("select * from people")
                .withResult(PrimingRequest.Result.read_request_timeout)
                .build();
        //when
        underTest.primeQuery(pr);
        //then
        verify(postRequestedFor(urlEqualTo(PRIME_QUERY_PATH))
                .withHeader("Content-Type", equalTo("application/json; charset=UTF-8"))
                .withRequestBody(equalToJson("{\"when\":{\"query\":\"select * from people\"}," +
                        "\"then\":{\"result\":\"read_request_timeout\"}}")));
    }

    @Test
    public void testPrimingQueryUnavailableException() {
        //given
        stubFor(post(urlEqualTo(PRIME_QUERY_PATH)).willReturn(aResponse().withStatus(200)));
        PrimingRequest pr = PrimingRequest.queryBuilder()
                .withQuery("select * from people")
                .withResult(PrimingRequest.Result.unavailable)
                .build();
        //when
        underTest.primeQuery(pr);
        //then
        verify(postRequestedFor(urlEqualTo(PRIME_QUERY_PATH))
                .withHeader("Content-Type", equalTo("application/json; charset=UTF-8"))
                .withRequestBody(equalTo("{\"when\":{\"query\":\"select * from people\"}," +
                        "\"then\":{\"result\":\"unavailable\"}}")));
    }

    @Test
    public void testPrimingQueryWriteRequestTimeout() {
        //given
        stubFor(post(urlEqualTo(PRIME_QUERY_PATH)).willReturn(aResponse().withStatus(200)));
        PrimingRequest pr = PrimingRequest.queryBuilder()
                .withQuery("select * from people")
                .withResult(PrimingRequest.Result.write_request_timeout)
                .build();
        //when
        underTest.primeQuery(pr);
        //then
        verify(postRequestedFor(urlEqualTo(PRIME_QUERY_PATH))
                .withHeader("Content-Type", equalTo("application/json; charset=UTF-8"))
                .withRequestBody(equalTo("{\"when\":{\"query\":\"select * from people\"}," +
                        "\"then\":{\"result\":\"write_request_timeout\"}}")));
    }

    @Test(expected = PrimeFailedException.class)
    public void testPrimeQueryFailed() {
        //given
        stubFor(post(urlEqualTo(PRIME_QUERY_PATH)).willReturn(aResponse().withStatus(500)));
        PrimingRequest pr = PrimingRequest.queryBuilder()
                .withQuery("select * from people")
                .withResult(PrimingRequest.Result.read_request_timeout)
                .build();
        //when
        underTest.primeQuery(pr);
        //then
    }

    @Test
    public void testPrimingQueryConsistency() {
        //given
        stubFor(post(urlEqualTo(PRIME_QUERY_PATH)).willReturn(aResponse().withStatus(200)));
        PrimingRequest pr = PrimingRequest.queryBuilder()
                .withQuery("select * from people")
                .withConsistency(PrimingRequest.Consistency.ALL, PrimingRequest.Consistency.ONE)
                .build();

        //when
        underTest.primeQuery(pr);

        //then
        verify(postRequestedFor(urlEqualTo(PRIME_QUERY_PATH))
                .withHeader("Content-Type", equalTo("application/json; charset=UTF-8"))
                .withRequestBody(equalToJson("{\"when\":{\"query\":\"select * from people\"," +
                        "\"consistency\":[\"ALL\",\"ONE\"]}," +
                        "\"then\":{\"rows\":[],\"result\":\"success\"}}")));

    }

    @Test
    public void testRetrieveOfPreviousQueryPrimes() {
        //given
        Map<String, Object> rows = new HashMap<String, Object>();
        rows.put("name","Chris");
        PrimingRequest pr = PrimingRequest.queryBuilder()
                .withQuery("select * from people")
                .withRows(rows)
                .build();
        stubFor(get(urlEqualTo(PRIME_QUERY_PATH)).willReturn(aResponse().withStatus(200).withBody(
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
        List<PrimingRequest> primingRequests = underTest.retrieveQueryPrimes();
        //then
        assertEquals(1, primingRequests.size());
        assertEquals(pr, primingRequests.get(0));
    }

    @Test
    public void testDeletingOfQueryPrimes() {
        //given
        stubFor(delete(urlEqualTo(PRIME_QUERY_PATH)).willReturn(aResponse().withStatus(200)));
        //when
        underTest.clearQueryPrimes();
        //then
        verify(deleteRequestedFor(urlEqualTo(PRIME_QUERY_PATH)));
    }

    @Test(expected = PrimeFailedException.class)
    public void testDeletingOfQueryPrimesFailedDueToStatusCode() {
        //given
        stubFor(delete(urlEqualTo(PRIME_QUERY_PATH)).willReturn(aResponse().withStatus(500)));
        //when
        underTest.clearQueryPrimes();
        //then
    }

    @Test(expected = PrimeFailedException.class)
    public void testRetrievingOfQueryPrimesFailedDueToStatusCode() {
        //given
        stubFor(get(urlEqualTo(PRIME_QUERY_PATH)).willReturn(aResponse().withStatus(500)));
        //when
        underTest.retrieveQueryPrimes();
        //then
    }

    @Test(expected = PrimeFailedException.class)
    public void testDeletingOfQueryPrimesFailed() {
        //given
        stubFor(delete(urlEqualTo(PRIME_QUERY_PATH)).willReturn(aResponse().withFault(Fault.RANDOM_DATA_THEN_CLOSE)));
        //when
        underTest.clearQueryPrimes();
        //then
    }

    @Test(expected = PrimeFailedException.class)
    public void testRetrievingOfQueryPrimesFailed() {
        //given
        stubFor(get(urlEqualTo(PRIME_QUERY_PATH))
                .willReturn(aResponse()
                .withFault(Fault.RANDOM_DATA_THEN_CLOSE)));
        //when
        underTest.retrieveQueryPrimes();
        //then
    }

    @Test
    public void testPrimingQueryWithSets() {
        //given
        stubFor(post(urlEqualTo(PRIME_QUERY_PATH)).willReturn(aResponse().withStatus(200)));
        List<Map<String,? extends Object>> rows = new ArrayList<Map<String,? extends Object>>();
        Map<String, Object> row = new HashMap<String, Object>();
        List<String> set = Arrays.asList("one", "two", "three");
        row.put("set",set);
        rows.add(row);
        PrimingRequest pr = PrimingRequest.queryBuilder()
                .withQuery("select * from people")
                .withRows(rows)
                .build();
        //when
        underTest.primeQuery(pr);
        //then
        verify(postRequestedFor(urlEqualTo(PRIME_QUERY_PATH))
                .withHeader("Content-Type", equalTo("application/json; charset=UTF-8"))
                .withRequestBody(equalTo("{\"when\":{\"query\":\"select * from people\"}," +
                        "\"then\":{" +
                        "\"rows\":[" +
                        "{\"set\":[\"one\",\"two\",\"three\"]}]," +
                        "\"result\":\"success\"}}")));

    }

    @Test
    public void testPrimingQueryWithColumnTypesSpecified() {
        //given
        stubFor(post(urlEqualTo(PRIME_QUERY_PATH)).willReturn(aResponse().withStatus(200)));
        Map<String, ColumnTypes> types = ImmutableMap.of("set_column", ColumnTypes.Set);
        Map<String, Object> row = new HashMap<String, Object>();
        List<String> set = Arrays.asList("one", "two", "three");
        row.put("set_column",set);
        PrimingRequest pr = PrimingRequest.queryBuilder()
                .withQuery("select * from people")
                .withRows(row)
                .withColumnTypes(types)
                .build();
        //when
        underTest.primeQuery(pr);

        //then
        verify(postRequestedFor(urlEqualTo(PRIME_QUERY_PATH))
                .withHeader("Content-Type", equalTo("application/json; charset=UTF-8"))
                .withRequestBody(equalToJson("{\"when\":{\"query\":\"select * from people\"}," +
                        "\"then\":{" +
                        "\"rows\":[" +
                        "{\"set_column\":[\"one\",\"two\",\"three\"]}]," +
                        "\"result\":\"success\"" +
                        ",\"column_types\":{\"set_column\":\"set\"}}}")));
    }

    @Test
    public void testPrimingPreparedStatementWithJustQueryText() {
        //given
        stubFor(post(urlEqualTo(PRIME_PREPARED_PATH)).willReturn(aResponse().withStatus(200)));
        PrimingRequest primingRequest = PrimingRequest.preparedStatementBuilder()
                .withQuery("select * from people where people = ?")
                .build();

        //when
        underTest.primePreparedStatement(primingRequest);

        //then
        verify(postRequestedFor(urlEqualTo(PRIME_PREPARED_PATH))
                .withHeader("Content-Type", equalTo("application/json; charset=UTF-8"))
                .withRequestBody(equalToJson("{" +
                        "   \"when\": { " +
                        "     \"query\" :\"select * from people where people = ?\"" +
                        "   }," +
                        "   \"then\": {" +
                        "     \"rows\" :[]," +
                        "     \"result\":\"success\" " +
                        "   }" +
                        " }")));
    }

    @Test
    public void testPrimingPreparedStatementWithVariableTypesSpecified() {
        //given
        stubFor(post(urlEqualTo(PRIME_PREPARED_PATH)).willReturn(aResponse().withStatus(200)));
        PrimingRequest primingRequest = PrimingRequest.preparedStatementBuilder()
                .withQuery("select * from people where age = ?")
                .withVariableTypes(ColumnTypes.Int)
                .build();

        //when
        underTest.primePreparedStatement(primingRequest);

        //then
        verify(postRequestedFor(urlEqualTo(PRIME_PREPARED_PATH))
                .withHeader("Content-Type", equalTo("application/json; charset=UTF-8"))
                .withRequestBody(equalToJson("{" +
                        "   \"when\": { " +
                        "     \"query\" :\"select * from people where age = ?\"" +
                        "   }," +
                        "   \"then\": {" +
                        "     \"variable_types\" :[ \"int\" ]," +
                        "     \"rows\" :[]," +
                        "     \"result\":\"success\" " +
                        "   }" +
                        " }")));
    }

    @Test(expected = PrimeFailedException.class)
    public void testPrimingPreparedStatementFailureDueToStatusCode() {
        //given
        stubFor(post(urlEqualTo(PRIME_PREPARED_PATH))
                .willReturn(aResponse().withStatus(500)));
        //when
        underTest.primePreparedStatement(PrimingRequest.preparedStatementBuilder().build());
        //then
    }

    @Test(expected = PrimeFailedException.class)
    public void testPrimingPreparedStatementFailureDueToHttpError() {
        //given
        stubFor(post(urlEqualTo(PRIME_PREPARED_PATH))
                .willReturn(aResponse().withFault(Fault.RANDOM_DATA_THEN_CLOSE)));
        //when
        underTest.primePreparedStatement(PrimingRequest.preparedStatementBuilder().build());
        //then
    }

    @Test
    public void retrievingOfPreparedStatementPrimes() {
        //given
        Map<String, Object> rows = new HashMap<String, Object>();
        rows.put("name","Chris");
        PrimingRequest pr = PrimingRequest.preparedStatementBuilder()
                .withQuery("select * from people")
                .withConsistency(PrimingRequest.Consistency.ANY)
                .withVariableTypes(ColumnTypes.Varchar)
                .withColumnTypes(ImmutableMap.of("name", ColumnTypes.Varchar))
                .withRows(rows)
                .build();
        stubFor(get(urlEqualTo(PRIME_PREPARED_PATH)).willReturn(aResponse().withStatus(200).withBody(
                "[{" +
                        "  \"when\": {" +
                        "    \"query\": \"select * from people\"," +
                        "    \"consistency\": [\"ANY\"]" +
                        "  }," +
                        "  \"then\": {" +
                        "    \"variable_types\": [" +
                        "      \"varchar\"" +
                        "    ]," +
                        "    \"rows\": [" +
                        "      {" +
                        "        \"name\": \"Chris\"" +
                        "      }" +
                        "    ]," +
                        "    \"result\": \"success\"," +
                        "    \"column_types\": {" +
                        "      \"name\": \"varchar\"" +
                        "    }" +
                        "  }" +
                        "}]"
        )));
        //when
        List<PrimingRequest> primingRequests = underTest.retrievePreparedPrimes();
        //then
        assertEquals(1, primingRequests.size());
        assertEquals(pr, primingRequests.get(0));
    }

    @Test
    public void testDeletingOfPreparedPrimes() {
        //given
        stubFor(delete(urlEqualTo(PRIME_PREPARED_PATH)).willReturn(aResponse().withStatus(200)));
        //when
        underTest.clearPreparedPrimes();
        //then
        verify(deleteRequestedFor(urlEqualTo(PRIME_PREPARED_PATH)));
    }

    @Test(expected = PrimeFailedException.class)
    public void testDeletingOfPreparedPrimesFailedDueToStatusCode() {
        //given
        stubFor(delete(urlEqualTo(PRIME_PREPARED_PATH)).willReturn(aResponse().withStatus(300)));
        //when
        underTest.clearPreparedPrimes();
        //then
    }

    @Test(expected = PrimeFailedException.class)
    public void testDeletingOfPreparedPrimesFailed() {
        //given
        stubFor(delete(urlEqualTo(PRIME_PREPARED_PATH)).willReturn(aResponse().withFault(Fault.RANDOM_DATA_THEN_CLOSE).withStatus(200)));
        //when
        underTest.clearPreparedPrimes();
        //then
    }

    @Test
    public void testClearAllPrimes() {
        //given
        stubFor(delete(urlEqualTo(PRIME_PREPARED_PATH)).willReturn(aResponse().withStatus(200)));
        stubFor(delete(urlEqualTo(PRIME_QUERY_PATH)).willReturn(aResponse().withStatus(200)));
        //when
        underTest.clearAllPrimes();
        //then
        verify(deleteRequestedFor(urlEqualTo(PRIME_PREPARED_PATH)));
        verify(deleteRequestedFor(urlEqualTo(PRIME_QUERY_PATH)));
    }
}
