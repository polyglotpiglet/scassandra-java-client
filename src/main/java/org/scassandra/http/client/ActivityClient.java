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

/**
 * Responsible for retrieving and clearing interactions with the Scassandra server. Including
 * - Queries
 * - Prepared statements
 * - Connections
 */
public class ActivityClient {

    public static class ActivityClientBuilder {

        private String host = "localhost";
        private int port = 8043;

        private ActivityClientBuilder() {}

        public ActivityClientBuilder withHost(String host){
            this.host = host;
            return this;
        }

        public ActivityClientBuilder withPort(int port){
            this.port = port;
            return this;
        }

        public ActivityClient build(){
            return new ActivityClient(this.host, this.port);
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ActivityClient.class);

    public static ActivityClientBuilder builder() { return new ActivityClientBuilder(); }

    private Gson gson = new Gson();
    private CloseableHttpClient httpClient = HttpClients.createDefault();
    private final String connectionUrl;
    private final String queryUrl;
    private final String preparedStatementExecutionUrl;

    private ActivityClient(String host, int port) {
        RequestConfig.Builder requestBuilder = RequestConfig.custom();
        requestBuilder = requestBuilder.setConnectTimeout(500);
        requestBuilder = requestBuilder.setConnectionRequestTimeout(500);
        requestBuilder = requestBuilder.setSocketTimeout(500);
        HttpClientBuilder builder = HttpClientBuilder.create();
        builder.setDefaultRequestConfig(requestBuilder.build());
        httpClient = builder.build();
        this.connectionUrl = "http://" + host + ":" + port + "/connection";
        this.queryUrl = "http://" + host + ":" + port + "/query";
        this.preparedStatementExecutionUrl = "http://" + host + ":" + port + "/prepared-statement-execution";
    }

    /**
     * Retrieves all the queries that have been sent to the configured Scassandra server.
     *
     * @return A List of Query objects
     */
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
    /**
     * Retrieves all the connections that have been sent to the configured Scassandra server.
     *
     * @return A List of Connection objects
     */
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
            LOGGER.info("Request for connections failed", e);
            throw new ActivityRequestFailed();
        }
    }



    /**
     * Deletes all the recorded connections from the configured Scassandra server.
     */
    public void clearConnections() {
        httpDelete(connectionUrl, "clearing of connections failed" );
    }

    /**
     * Deletes all the recorded queries from the configured Scassandra server.
     */
    public void clearQueries() {
        httpDelete(queryUrl, "clearing of queries failed" );
    }

    public void clearPreparedStatementExecutions() {
        httpDelete(preparedStatementExecutionUrl, "clearing of prepared statement executions failed");
    }

    public void clearAllRecordedActivity(){
        clearConnections();
        clearQueries();
        clearPreparedStatementExecutions();
    }

    private void httpDelete(String url, String warningMessage) {
        HttpDelete delete = new HttpDelete(url);
        try {
            CloseableHttpResponse httpResponse = httpClient.execute(delete);
            EntityUtils.consumeQuietly(httpResponse.getEntity());
        } catch (IOException e) {
            LOGGER.warn(warningMessage, e);
            throw new ActivityRequestFailed();
        }
    }

    public List<PreparedStatementExecution> retrievePreparedStatementExecutions() {
        HttpGet get = new HttpGet(preparedStatementExecutionUrl);
        try {
            CloseableHttpResponse response = httpClient.execute(get);
            String body = EntityUtils.toString(response.getEntity());
            LOGGER.debug("Received response {}", body);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != 200) {
                LOGGER.info("Non 200 status code when retrieving prepared statement executions {}", statusCode);
                throw new ActivityRequestFailed();
            }
            PreparedStatementExecution[] executions = (PreparedStatementExecution[]) gson.fromJson(body, (Class) PreparedStatementExecution[].class);
            LOGGER.debug("Parsed prepared statement executions {}", Arrays.toString(executions));
            return Arrays.asList(executions);
        } catch (IOException e) {
            LOGGER.info("Request for queries failed", e);
            throw new ActivityRequestFailed();
        }
    }

}
