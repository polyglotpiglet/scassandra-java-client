package org.scassandra.http.client;

public class Query {
    private String query;

    public Query(String query) {
        this.query = query;
    }

    public String getQuery() {
        return query;
    }

    @Override
    public String toString() {
        return "Query{" +
                "query='" + query + '\'' +
                '}';
    }
}
