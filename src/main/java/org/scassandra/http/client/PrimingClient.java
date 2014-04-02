package org.scassandra.http.client;

import com.google.gson.Gson;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class PrimingClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(PrimingClient.class);

    private String host;
    private int port;
    private Gson gson = new Gson();
    private CloseableHttpClient httpClient = HttpClients.createDefault();

    public PrimingClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void prime(PrimingRequest primeRequest) throws PrimeFailedException {
        LOGGER.info("Prime request {}", primeRequest);
        HttpPost httpPost = new HttpPost("http://" + host + ":" + port + "/prime");
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
}
