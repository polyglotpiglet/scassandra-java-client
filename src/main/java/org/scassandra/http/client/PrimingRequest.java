package org.scassandra.http.client;

import java.util.List;
import java.util.Map;

public class PrimingRequest {
    private String when;
    private Then then;

    public PrimingRequest(String when, List<Map<String, String>> rows) {
        this.when = when;
        this.then = new Then(rows);
    }

    public PrimingRequest(String when, Result result) {
        this.when = when;
        this.then = new Then(result);
    }

    @Override
    public String toString() {
        return "PrimingRequest{" +
                "when='" + when + '\'' +
                ", then=" + then +
                '}';
    }

    private static class Then {
        private List<Map<String, String>> rows;
        private Result result;

        private Then(List<Map<String, String>> rows) {
            this.rows = rows;
            this.result = Result.success;
        }
        private Then(Result result) {
            this.result = result;
        }

        @Override
        public String toString() {
            return "Then{" +
                    "rows=" + rows +
                    ", result=" + result +
                    '}';
        }
    }

    public static enum Result {
        success,
        read_request_timeout,
        unavailable,
        write_request_timeout
    }
}
