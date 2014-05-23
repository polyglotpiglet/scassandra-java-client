/*
 * Copyright (C) 2014 Christopher Batey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.scassandra.http.client;

import java.util.*;

public final class PrimingRequest {

    public static class PrimingRequestBuilder {

        private PrimeType type;

        private static enum PrimeType {
            QUERY, PREPARED
        }

        private PrimingRequestBuilder(PrimeType type) {
            this.type = type;
        }

        private Consistency[] consistency;
        private ColumnTypes[] variableTypes;
        private Map<String, ColumnTypes> columnTypes;
        private String query;
        private List<Map<String, ? extends Object>> rows;
        private Result result = Result.success;

        public PrimingRequestBuilder withQuery(String query) {
            this.query = query;
            return this;
        }

        public PrimingRequestBuilder withRows(List<Map<String, ? extends Object>> rows) {
            this.rows = rows;
            return this;
        }

        @SafeVarargs
        public final PrimingRequestBuilder withRows(Map<String, ? extends Object>... rows) {
            this.rows = Arrays.asList(rows);
            return this;
        }

        public PrimingRequestBuilder withResult(Result result) {
            this.result = result;
            return this;
        }

        public PrimingRequest build() {

            if (PrimeType.QUERY.equals(this.type) && this.variableTypes != null) {
                throw new IllegalStateException("Variable types only applicable for a prepared statement prime. Not a query prime.");
            }

            if (query == null) {
                throw new IllegalStateException("Must set query for PrimingRequest.");
            }

            List<Consistency> consistencies = this.consistency == null ? null : Arrays.asList(this.consistency);

            List<Map<String, ? extends Object>> rowsDefaultedToEmptyForSuccess = this.rows;

            if (result == Result.success && rows == null) {
                rowsDefaultedToEmptyForSuccess = Collections.emptyList();
            }
            return new PrimingRequest(this.query, consistencies, rowsDefaultedToEmptyForSuccess, this.result, this.columnTypes, this.variableTypes);
        }

        public PrimingRequestBuilder withConsistency(Consistency... consistencies) {
            consistency = consistencies;
            return this;
        }

        public PrimingRequestBuilder withColumnTypes(Map<String, ColumnTypes> types) {
            this.columnTypes = types;
            return this;
        }

        public PrimingRequestBuilder withVariableTypes(ColumnTypes... variableTypes) {
            this.variableTypes = variableTypes;
            return this;
        }
    }

    public static PrimingRequestBuilder queryBuilder() {
        return new PrimingRequestBuilder(PrimingRequestBuilder.PrimeType.QUERY);
    }

    public static PrimingRequestBuilder preparedStatementBuilder() {
        return new PrimingRequestBuilder(PrimingRequestBuilder.PrimeType.PREPARED);
    }

    private final When when;
    private final Then then;

    private PrimingRequest(String query, List<Consistency> consistency, List<Map<String, ? extends Object>> rows, Result result, Map<String, ColumnTypes> columnTypes, ColumnTypes[] variableTypes) {
        this.when = new When(query, consistency);
        this.then = new Then(rows, result, columnTypes, variableTypes);
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
        private final ColumnTypes[] variable_types;
        private List<Map<String, ? extends Object>> rows;
        private Result result;
        private Map<String, ColumnTypes> column_types;

        private Then(List<Map<String, ? extends Object>> rows, Result result, Map<String, ColumnTypes> column_types, ColumnTypes[] variable_types) {
            this.rows = rows;
            this.result = result;
            this.column_types = column_types;
            this.variable_types = variable_types;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Then then = (Then) o;

            if (column_types != null ? !column_types.equals(then.column_types) : then.column_types != null)
                return false;
            if (result != then.result) return false;
            if (rows != null ? !rows.equals(then.rows) : then.rows != null) return false;
            if (!Arrays.equals(variable_types, then.variable_types)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result1 = variable_types != null ? Arrays.hashCode(variable_types) : 0;
            result1 = 31 * result1 + (rows != null ? rows.hashCode() : 0);
            result1 = 31 * result1 + (result != null ? result.hashCode() : 0);
            result1 = 31 * result1 + (column_types != null ? column_types.hashCode() : 0);
            return result1;
        }

        @Override
        public String toString() {
            return "Then{" +
                    "variable_types=" + Arrays.toString(variable_types) +
                    ", rows=" + rows +
                    ", result=" + result +
                    ", column_types=" + column_types +
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

        @Override
        public String toString() {
            return "When{" +
                    "query='" + query + '\'' +
                    ", consistency=" + consistency +
                    '}';
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
