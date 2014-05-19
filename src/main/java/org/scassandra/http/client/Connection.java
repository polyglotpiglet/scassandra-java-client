package org.scassandra.http.client;

public final class Connection {

    public static class ConnectionBuilder {

        private String result;

        private ConnectionBuilder() {}

        public ConnectionBuilder withResult(String result) {
            this.result = result;
            return this;
        }

        public Connection build() {
            return new Connection(this.result);
        }

    }

    public static ConnectionBuilder builder() { return new ConnectionBuilder(); }

    private final String result;

    private Connection(String result) {
        this.result = result;
    }

    public String getResult() {
        return result;
    }

    @Override
    public String toString() {
        return "Connection{" +
                "result='" + result + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Connection that = (Connection) o;

        if (result != null ? !result.equals(that.result) : that.result != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return result != null ? result.hashCode() : 0;
    }
}
