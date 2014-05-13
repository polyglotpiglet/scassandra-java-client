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
        HttpPost httpPost = new HttpPost(primeQueryUrl);
        String jsonAsString = gson.toJson(primeRequest);
        LOGGER.info("Sending primeQuery to server {}", jsonAsString);
        httpPost.setEntity(new StringEntity(jsonAsString, ContentType.APPLICATION_JSON));
        CloseableHttpResponse response1 = null;
        try {
            response1 = httpClient.execute(httpPost);
        } catch (IOException e) {
            LOGGER.warn("priming failed", e);
            throw new PrimeFailedException();
        } finally {
            if (response1 != null) {
                EntityUtils.consumeQuietly(response1.getEntity());
            }
        }

        if (response1.getStatusLine().getStatusCode() != 200) {
            LOGGER.warn("Priming came back with non-200 response code {}", response1.getStatusLine());
            throw new PrimeFailedException();
        }

    }

    public void clearPrimes() {
        HttpDelete delete = new HttpDelete(primeQueryUrl);
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

    public List<PrimingRequest> retrievePrimes() {
        HttpGet get = new HttpGet(primeQueryUrl);
        try {
            CloseableHttpResponse httpResponse = httpClient.execute(get);
            int statusCode = httpResponse.getStatusLine().getStatusCode();
            if (statusCode != 200) {
                LOGGER.info("Retrieving of primes failed with http status {}", statusCode);
                throw new PrimeFailedException();
            }
            String responseAsString = EntityUtils.toString(httpResponse.getEntity());
            LOGGER.debug("Received response from scassandra {}", responseAsString);
            PrimingRequest[] primes = (PrimingRequest[]) gson.fromJson(responseAsString, (Class) PrimingRequest[].class);
            return Arrays.asList(primes);
        } catch (IOException e) {
            LOGGER.info("retrieving failed", e);
            throw new PrimeFailedException();
        }
    }

    public void primePreparedStatement(PrimingRequest primingRequest) {
        HttpPost httpPost = new HttpPost(primePreparedUrl);
        String primeAsJson = gson.toJson(primingRequest);
        httpPost.setEntity(new StringEntity(primeAsJson, ContentType.APPLICATION_JSON));
        try {
            CloseableHttpResponse response = httpClient.execute(httpPost);
            EntityUtils.consumeQuietly(response.getEntity());
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != 200) {
                throw new PrimeFailedException("Response code from server: " + statusCode);

            }
        } catch (IOException e) {
            LOGGER.info("failed prepared prime {}", e);
            throw new PrimeFailedException();
        }
    }
}
