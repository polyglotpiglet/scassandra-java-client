package org.scassandra.http.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PrimingRequest {

    public static class PrimingRequestBuilder {

        private PrimingRequestBuilder() {}

        private String query;
        private List<Map<String, String>> rows;
        private Result result = Result.success;

        public PrimingRequestBuilder withQuery(String query) {
            this.query = query;
            return this;
        }

        public PrimingRequestBuilder withRows(List<Map<String, String>> rows) {
            this.rows = rows;
            return this;
        }

        public PrimingRequestBuilder withResult(Result result) {
            this.result = result;
            return this;
        }

        public PrimingRequest build() {
            return new PrimingRequest(this.query, this.rows, this.result);
        }
    }

    public static PrimingRequestBuilder builder() {
        return new PrimingRequestBuilder();
    }

    private String when;
    private Then then;

    private PrimingRequest(String when, List<Map<String, String>> rows, Result result) {
        this.when = when;
        this.then = new Then(rows, result);
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

        private Then(List<Map<String, String>> rows, Result result) {
            this.rows = rows;
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
