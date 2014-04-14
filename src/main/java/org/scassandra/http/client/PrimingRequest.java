package org.scassandra.http.client;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PrimingRequest {

    public static class PrimingRequestBuilder {


        private PrimingRequestBuilder() {}

        private Consistency[] consistency;
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
            List<Consistency> consistencies = this.consistency == null ? null : Arrays.asList(this.consistency);

            List<Map<String, String>> rowsDefaultedToEmptyForSuccess = this.rows;

            if (result == Result.success && rows == null) {
                rowsDefaultedToEmptyForSuccess = Collections.emptyList();
            }
            return new PrimingRequest(this.query, consistencies, rowsDefaultedToEmptyForSuccess, this.result);
        }

        public PrimingRequestBuilder withConsistency(Consistency... consistencies) {
            consistency = consistencies;
            return this;
        }
    }

    public static PrimingRequestBuilder builder() {
        return new PrimingRequestBuilder();
    }

    private When when;
    private Then then;

    private PrimingRequest(String query, List<Consistency> consistency, List<Map<String, String>> rows, Result result) {
        this.when = new When(query, consistency);
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

    private static class When {
        private String query;
        private List<Consistency> consistency;

        private When(String query, List<Consistency> consistency) {
            this.query = query;
            this.consistency = consistency;
        }
    }

    public static enum Consistency {
        ANY,
        ONE,
        TWO,
        THREE,
        QUORUM,
        ALL,
        LOCAL_QUORUM,
        EACH_QUORUM,
        SERIAL,
        LOCAL_SERIAL,
        LOCAL_ONE
    }

    public static enum Result {
        success,
        read_request_timeout,
        unavailable,
        write_request_timeout
    }
}
