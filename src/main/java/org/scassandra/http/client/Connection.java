package org.scassandra.http.client;

public class Connection {
    private String result;

    public Connection(String result) {
        this.result = result;
    }

    public String getResult() {
        return result;
    }
}
