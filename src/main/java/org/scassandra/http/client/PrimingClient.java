package org.scassandra.http.client;

import com.google.gson.Gson;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class PrimingClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(PrimingClient.class);

    private String host;
    private int port;
    private Gson gson = new Gson();
    private CloseableHttpClient httpClient = HttpClients.createDefault();
    private String primeUrl;

    public PrimingClient(String host, int port) {
        this.host = host;
        this.port = port;
        this.primeUrl = "http://" + host + ":" + port + "/prime";
    }

    public void prime(PrimingRequest primeRequest) throws PrimeFailedException {
        LOGGER.info("Prime request {}", primeRequest);
        HttpPost httpPost = new HttpPost(primeUrl);
        String jsonAsString = gson.toJson(primeRequest);
        httpPost.setEntity(new StringEntity(jsonAsString, ContentType.APPLICATION_JSON));
        CloseableHttpResponse response1;
        try {
            response1 = httpClient.execute(httpPost);
        } catch (IOException e) {
            LOGGER.warn("priming failed", e);
            throw new PrimeFailedException();
        }

        if (response1.getStatusLine().getStatusCode() != 200) {
            LOGGER.warn("Priming came back with non-200 response code {}", response1.getStatusLine());
            throw new PrimeFailedException();
        }

    }

    public void clearPrimes() {
        HttpDelete delete = new HttpDelete(primeUrl);
        try {
            CloseableHttpResponse httpResponse = httpClient.execute(delete);
            EntityUtils.consumeQuietly(httpResponse.getEntity());
            int statusCode = httpResponse.getStatusLine().getStatusCode();
            if (statusCode != 200) {
                LOGGER.info("Clearing of primes failed with http status {}", statusCode);
                throw new PrimeFailedException();
            }
        } catch (IOException e) {
            LOGGER.info("priming failed", e);
            throw new PrimeFailedException();
        }
    }
}
