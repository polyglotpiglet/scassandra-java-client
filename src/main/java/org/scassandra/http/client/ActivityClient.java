package org.scassandra.http.client;

import com.google.gson.Gson;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class ActivityClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(ActivityClient.class);

    private Gson gson = new Gson();
    private CloseableHttpClient httpClient = HttpClients.createDefault();
    private String connectionUrl;
    private String queryUrl;

    public ActivityClient(String host, int port) {
        RequestConfig.Builder requestBuilder = RequestConfig.custom();
        requestBuilder = requestBuilder.setConnectTimeout(500);
        requestBuilder = requestBuilder.setConnectionRequestTimeout(500);
        requestBuilder = requestBuilder.setSocketTimeout(500);
        HttpClientBuilder builder = HttpClientBuilder.create();
        builder.setDefaultRequestConfig(requestBuilder.build());
        httpClient = builder.build();
        this.connectionUrl = "http://" + host + ":" + port + "/connection";
        this.queryUrl = "http://" + host + ":" + port + "/query";


    }

    public List<Query> retrieveQueries() {
        HttpGet get = new HttpGet(queryUrl);
        try {
            CloseableHttpResponse response = httpClient.execute(get);
            String body = EntityUtils.toString(response.getEntity());
            LOGGER.debug("Received response {}", body);
            Query[] queries = (Query[]) gson.fromJson(body, (Class) Query[].class);
            LOGGER.debug("Parsed queries {}", Arrays.toString(queries));
            return Arrays.asList(queries);
        } catch (IOException e) {
            LOGGER.info("Request for queries failed", e);
            throw new ActivityRequestFailed();
        }
    }

    public List<Connection> retrieveConnections() {
        HttpGet get = new HttpGet(connectionUrl);

        try {
            CloseableHttpResponse response = httpClient.execute(get);
            String body = EntityUtils.toString(response.getEntity());
            LOGGER.debug("Received response {}", body);
            Connection[] queries = (Connection[]) gson.fromJson(body, (Class) Connection[].class);
            LOGGER.debug("Parsed connections {}", Arrays.toString(queries));
            return Arrays.asList(queries);
        } catch (IOException e) {
            LOGGER.info("Request for queries failed", e);
            throw new ActivityRequestFailed();
        }
    }

    public void clearConnections() {
        HttpDelete delete = new HttpDelete(connectionUrl);
        try {
            CloseableHttpResponse httpResponse = httpClient.execute(delete);
            EntityUtils.consumeQuietly(httpResponse.getEntity());
        } catch (IOException e) {
           LOGGER.warn("clearing of connections failed",e);
            throw new ActivityRequestFailed();
        }
    }

    public void clearQueries() {
        HttpDelete delete = new HttpDelete(queryUrl);
        try {
            CloseableHttpResponse httpResponse = httpClient.execute(delete);
            EntityUtils.consumeQuietly(httpResponse.getEntity());
        } catch (IOException e) {
            LOGGER.warn("clearing of connections failed",e);
            throw new ActivityRequestFailed();
        }
    }
}
