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
package org.scassandra.http.client;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.scassandra.http.client.types.CqlType;
import org.scassandra.http.client.types.CqlTypeSerialiser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class PrimingClient {

    public static final String DELETING_OF_PRIMES_FAILED = "Deleting of primes failed";
    public static final String PRIMING_FAILED = "Priming failed";

    public static class PrimingClientBuilder {

        private String host = "localhost";
        private int port = 8043;

        private PrimingClientBuilder() {}

        public PrimingClientBuilder withHost(String host){
            this.host = host;
            return this;
        }

        public PrimingClientBuilder withPort(int port){
            this.port = port;
            return this;
        }

        public PrimingClient build(){
            return new PrimingClient(this.host, this.port);
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(PrimingClient.class);

    public static PrimingClientBuilder builder() { return new PrimingClientBuilder(); }

    private Gson gson = new GsonBuilder().registerTypeAdapter(CqlType.class, new CqlTypeSerialiser()).create();
    private CloseableHttpClient httpClient = HttpClients.createDefault();
    private String primeQueryUrl;
    private String primePreparedUrl;

    private PrimingClient(String host, int port) {

        this.primeQueryUrl = "http://" + host + ":" + port + "/prime-query-single";
        this.primePreparedUrl = "http://" + host + ":" + port + "/prime-prepared-single";
    }

    public void prime(PrimingRequest prime) throws PrimeFailedException {
        if (prime.primeType == PrimingRequest.PrimingRequestBuilder.PrimeType.QUERY) {
            this.primeQuery(prime);
        } else {
            this.primePreparedStatement(prime);
        }
    }

    /**
     * @param primeRequest The Prime
     * @deprecated Use prime() instead.
     */
    @Deprecated
    public void primeQuery(PrimingRequest primeRequest) throws PrimeFailedException {
        if (primeRequest.primeType != PrimingRequest.PrimingRequestBuilder.PrimeType.QUERY) {
            throw new IllegalArgumentException("Can't pass a prepared statement prime to primeQuery, use queryBuilder()");
        }
        prime(primeRequest, primeQueryUrl);
    }

    /**
     * @param primeRequest The Prime
     * @deprecated Use prime() instead.
     */
    public void primePreparedStatement(PrimingRequest primeRequest) throws PrimeFailedException {
        if (primeRequest.primeType != PrimingRequest.PrimingRequestBuilder.PrimeType.PREPARED) {
            throw new IllegalArgumentException("Can't pass a query prime to primePreparedStatement, use preparedStatementBuilder()");
        }
        prime(primeRequest, primePreparedUrl);
    }

    public List<PrimingRequest> retrievePreparedPrimes() {
        return httpGetPrimingRequests(primePreparedUrl);
    }

    public List<PrimingRequest> retrieveQueryPrimes() {
        return httpGetPrimingRequests(primeQueryUrl);
    }

    public void clearAllPrimes(){
        clearQueryPrimes();
        clearPreparedPrimes();
    }

    public void clearQueryPrimes() {
        httpDelete(primeQueryUrl);
    }

    public void clearPreparedPrimes() {
        httpDelete(primePreparedUrl);
    }

    private List<PrimingRequest> httpGetPrimingRequests(String url) {
        HttpGet get = new HttpGet(url);
        try {
            CloseableHttpResponse httpResponse = httpClient.execute(get);
            int statusCode = httpResponse.getStatusLine().getStatusCode();
            String responseAsString = EntityUtils.toString(httpResponse.getEntity());
            if (statusCode != 200) {
                String errorMessage = String.format("Retrieving of primes failed with http status %s body %s", statusCode, responseAsString);
                LOGGER.info(errorMessage);
                throw new PrimeFailedException(errorMessage);
            }
            LOGGER.debug("Received response from scassandra {}", responseAsString);
            PrimingRequest[] primes = (PrimingRequest[]) gson.fromJson(responseAsString, (Class) PrimingRequest[].class);
            return Arrays.asList(primes);
        } catch (IOException e) {
            LOGGER.info("Retrieving failed", e);
            throw new PrimeFailedException("Retrieving of primes failed.", e);
        }
    }

    private void httpDelete(String url){

        HttpDelete delete = new HttpDelete(url);
        CloseableHttpResponse httpResponse = null;
        try {
            httpResponse = httpClient.execute(delete);
            EntityUtils.consumeQuietly(httpResponse.getEntity());
            int statusCode = httpResponse.getStatusLine().getStatusCode();
            if (statusCode != 200) {
                String errorMessage = String.format("Clearing of primes failed with http status %s", statusCode);
                LOGGER.info(errorMessage);
                throw new PrimeFailedException(errorMessage);
            }
        } catch (IOException e) {
            LOGGER.info(DELETING_OF_PRIMES_FAILED, e);
            throw new PrimeFailedException(DELETING_OF_PRIMES_FAILED, e);
        } finally {
            if (httpResponse != null) {
                EntityUtils.consumeQuietly(httpResponse.getEntity());
            }
        }
    }

    private void prime(PrimingRequest primeRequest, String url) {
        HttpPost httpPost = new HttpPost(url);
        String jsonAsString = gson.toJson(primeRequest);
        LOGGER.info("Sending {} to url {}", jsonAsString, url);
        httpPost.setEntity(new StringEntity(jsonAsString, ContentType.APPLICATION_JSON));
        CloseableHttpResponse response = null;
        try {
            response = httpClient.execute(httpPost);
            if (response.getStatusLine().getStatusCode() != 200) {
                String body = EntityUtils.toString(response.getEntity());
                String errorMessage = String.format("Priming came back with non-200 response code: %s and body: %s", response.getStatusLine(), body);
                LOGGER.warn(errorMessage);
                throw new PrimeFailedException(errorMessage);
            }
        } catch (IOException e) {
            LOGGER.warn(PRIMING_FAILED, e);
            throw new PrimeFailedException(PRIMING_FAILED, e);
        } finally {
            if (response != null) {
                EntityUtils.consumeQuietly(response.getEntity());
            }
        }
    }

}
