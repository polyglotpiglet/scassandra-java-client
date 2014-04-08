package org.scassandra.http.client;

public class Query {
    private String query;
    private String consistency;

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

    public String getConsistency() {
        return consistency;
    }
}
