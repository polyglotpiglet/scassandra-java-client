package org.scassandra.http.client;

import com.google.gson.Gson;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class PrimingClient {

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

    private Gson gson = new Gson();
    private CloseableHttpClient httpClient = HttpClients.createDefault();
    private String primeQueryUrl;
    private String primePreparedUrl;

    private PrimingClient(String host, int port) {
        this.primeQueryUrl = "http://" + host + ":" + port + "/prime-query-single";
        this.primePreparedUrl = "http://" + host + ":" + port + "/prime-prepared-single";
    }
    
    public void primeQuery(PrimingRequest primeRequest) throws PrimeFailedException {
        prime(primeRequest, primeQueryUrl);
    }

    public void primePreparedStatement(PrimingRequest primeRequest) throws PrimeFailedException {
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
                LOGGER.info("Retrieving of primes failed with http status {} body {}", statusCode, responseAsString);
                throw new PrimeFailedException();
            }
            LOGGER.debug("Received response from scassandra {}", responseAsString);
            PrimingRequest[] primes = (PrimingRequest[]) gson.fromJson(responseAsString, (Class) PrimingRequest[].class);
            return Arrays.asList(primes);
        } catch (IOException e) {
            LOGGER.info("retrieving failed", e);
            throw new PrimeFailedException();
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
                LOGGER.info("Clearing of primes failed with http status {}", statusCode);
                throw new PrimeFailedException();
            }
        } catch (IOException e) {
            LOGGER.info("priming failed", e);
            throw new PrimeFailedException();
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
        } catch (IOException e) {
            LOGGER.warn("Priming failed", e);
            throw new PrimeFailedException();
        } finally {
            if (response != null) {
                EntityUtils.consumeQuietly(response.getEntity());
            }
        }

        if (response.getStatusLine().getStatusCode() != 200) {
            LOGGER.warn("Priming came back with non-200 response code {}", response.getStatusLine());
            throw new PrimeFailedException();
        }
    }

}
