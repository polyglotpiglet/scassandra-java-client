package org.scassandra.http.client;

public class Query {
    private String query;
    private String consistency;

    public Query(String query, String consistency) {
        this.query = query;
        this.consistency = consistency;
    }

    public String getQuery() {
        return query;
    }

    public String getConsistency() {
        return consistency;
    }

    @Override
    public String toString() {
        return "Query{" +
                "query='" + query + '\'' +
                ", consistency='" + consistency + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Query query1 = (Query) o;

        if (consistency != null ? !consistency.equals(query1.consistency) : query1.consistency != null) return false;
        if (query != null ? !query.equals(query1.query) : query1.query != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = query != null ? query.hashCode() : 0;
        result = 31 * result + (consistency != null ? consistency.hashCode() : 0);
        return result;
    }
}
