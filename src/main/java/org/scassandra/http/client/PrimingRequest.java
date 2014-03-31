package org.scassandra.http.client;

import java.util.List;
import java.util.Map;

public class PrimingRequest {
    private String when;
    private List<Map<String, String>> then;

    public PrimingRequest(String when, List<Map<String, String>> rows) {
        this.when = when;
        this.then = rows;
    }
}