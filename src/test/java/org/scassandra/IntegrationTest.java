/*
 * Copyright (C) 2014 Christopher Batey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.scassandra;

import com.google.common.collect.ImmutableMap;
import org.junit.*;
import org.scassandra.http.client.*;
import org.scassandra.junit.ScassandraServerRule;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/*
TODO: Break up into multiple integration tests.
 */
public class IntegrationTest {

    private static int binaryPort = 2345;
    private static int adminPort = 3456;

    @ClassRule
    public static ScassandraServerRule rule = new ScassandraServerRule(binaryPort, adminPort);

    @Rule
    public ScassandraServerRule clearRule = rule;

    private static ActivityClient activityClient = rule.activityClient();
    private static PrimingClient primingClient = rule.primingClient();


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
    public void testPreparedPrimeAndRetrievalOfPrime() {
        //given
        Map<String, String> row = new HashMap<String, String>();
        row.put("name", "chris");
        PrimingRequest prime = PrimingRequest.queryBuilder()
                .withQuery("select * from people where name = ?")
                .withRows(row)
                .withConsistency(PrimingRequest.Consistency.ALL)
                .build();

        //when
        primingClient.primePreparedStatement(prime);
        List<PrimingRequest> retrievedPrimes = primingClient.retrievePreparedPrimes();
        //then
        PrimingRequest expectedPrimeWithDefaults = PrimingRequest.preparedStatementBuilder()
                .withQuery("select * from people where name = ?")
                .withRows(row)
                .withColumnTypes(ImmutableMap.of("name", ColumnTypes.Varchar))
                .withResult(PrimingRequest.Result.success)
                .withConsistency(PrimingRequest.Consistency.ALL)
                .withVariableTypes(ColumnTypes.Varchar)
                .build();

       PrimingRequest retrievedPrime = retrievedPrimes.get(0);
        assertEquals(expectedPrimeWithDefaults, retrievedPrime);
    }

    @Test
    public void testActivityRetrieveOfPreparedStatementExecutions() throws Exception {
        //given
        //when
        activityClient.retrievePreparedStatementExecutions();
        //then
    }
}
