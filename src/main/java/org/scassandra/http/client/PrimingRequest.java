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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PrimingRequest that = (PrimingRequest) o;

        if (then != null ? !then.equals(that.then) : that.then != null) return false;
        if (when != null ? !when.equals(that.when) : that.when != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = when != null ? when.hashCode() : 0;
        result = 31 * result + (then != null ? then.hashCode() : 0);
        return result;
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Then then = (Then) o;

            if (result != then.result) return false;
            if (rows != null ? !rows.equals(then.rows) : then.rows != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result1 = rows != null ? rows.hashCode() : 0;
            result1 = 31 * result1 + (result != null ? result.hashCode() : 0);
            return result1;
        }
    }

    private static class When {
        private String query;
        private List<Consistency> consistency;

        private When(String query, List<Consistency> consistency) {
            this.query = query;
            this.consistency = consistency;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            When when = (When) o;

            if (consistency != null ? !consistency.equals(when.consistency) : when.consistency != null) return false;
            if (query != null ? !query.equals(when.query) : when.query != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = query != null ? query.hashCode() : 0;
            result = 31 * result + (consistency != null ? consistency.hashCode() : 0);
            return result;
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
