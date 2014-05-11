package org.scassandra.http.client;

public class Query {
    
    public static class QueryBuilder {

        private String query;
        private String consistency;

        private QueryBuilder() {}

        public QueryBuilder withQuery(String query){
            this.query = query;
            return this;
        }

        public QueryBuilder withConsistency(String consistency){
            this.consistency = consistency;
            return this;
        }
        
        public Query build(){
            return new Query(this.query, this.consistency);
        }
    }

    public static QueryBuilder builder() { return new QueryBuilder(); }

    private String query;
    private String consistency;

    private Query(String query, String consistency) {
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
