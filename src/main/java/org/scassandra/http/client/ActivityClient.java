package org.scassandra.http.client;

import com.google.gson.Gson;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ActivityClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(ActivityClient.class);

    private String host;
    private int port;
    private Gson gson = new Gson();
    private CloseableHttpClient httpClient = HttpClients.createDefault();

    public ActivityClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public List<Query> retrieveQueries() {
        HttpGet get = new HttpGet("http://" + host + ":" + port + "/query");
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
        HttpGet get = new HttpGet("http://" + host + ":" + port + "/connection");
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
}
