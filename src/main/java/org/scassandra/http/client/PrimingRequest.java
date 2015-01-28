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

import org.scassandra.cql.CqlType;
import org.scassandra.http.client.types.ColumnMetadata;

import java.util.*;

public final class PrimingRequest {

    transient PrimingRequestBuilder.PrimeType primeType;

    public static class PrimingRequestBuilder {

        PrimeType type;

        static enum PrimeType {
            QUERY, PREPARED
        }

        private PrimingRequestBuilder(PrimeType type) {
            this.type = type;
        }

        private Consistency[] consistency;
        private ColumnTypes[] variableTypes;
        private List<ColumnMetadata> columnTypesMeta;
        private String query;
        private String queryPattern;
        private List<Map<String, ?>> rows;
        private Result result = Result.success;
        private Long fixedDelay;

        public PrimingRequestBuilder withQuery(String query) {
            this.query = query;
            return this;
        }

        public PrimingRequestBuilder withQueryPattern(String queryPattern) {
            this.queryPattern = queryPattern;
            return this;
        }


        public PrimingRequestBuilder withFixedDelay(long fixedDelay) {
            this.fixedDelay = fixedDelay;
            return this;
        }

        public PrimingRequestBuilder withRows(List<Map<String, ?>> rows) {
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

        public PrimingRequestBuilder withConsistency(Consistency... consistencies) {
            consistency = consistencies;
            return this;
        }

        /**
         * @deprecated Use ColumnMetadata instead. This will be removed in version 1.0
         */
        @Deprecated
        public PrimingRequestBuilder withColumnTypes(Map<String, ColumnTypes> types) {
            List<ColumnMetadata> columnMetadata = new ArrayList<ColumnMetadata>();
            for (Map.Entry<String, ColumnTypes> entry : types.entrySet()) {
                columnMetadata.add(ColumnMetadata.column(entry.getKey(), entry.getValue().getType()));
            }
            this.columnTypesMeta = columnMetadata;
            return this;
        }

        public PrimingRequestBuilder withColumnTypes(ColumnMetadata... columnMetadata) {
            this.columnTypesMeta = Arrays.asList(columnMetadata);
            return this;
        }

        /**
         * @deprecated Use ColumnMetadata instead. This will be removed in version 1.0
         */
        @Deprecated
        public PrimingRequestBuilder withVariableTypes(ColumnTypes... variableTypes) {
            this.variableTypes = variableTypes;
            return this;
        }

        public PrimingRequest build() {

            if (PrimeType.QUERY.equals(this.type) && this.variableTypes != null) {
                throw new IllegalStateException("Variable types only applicable for a prepared statement prime. Not a query prime");
            }

            if (query != null && queryPattern != null) {
                throw new IllegalStateException("Can't specify query and queryPattern");
            }

            if (query == null && queryPattern == null) {
                throw new IllegalStateException("Must set either query or queryPattern for PrimingRequest");
            }

            List<Consistency> consistencies = this.consistency == null ? null : Arrays.asList(this.consistency);

            List<Map<String, ? extends Object>> rowsDefaultedToEmptyForSuccess = this.rows;

            if (result == Result.success && rows == null) {
                rowsDefaultedToEmptyForSuccess = Collections.emptyList();
            }
            return new PrimingRequest(type, query, queryPattern, consistencies, rowsDefaultedToEmptyForSuccess, result, columnTypesMeta, variableTypes, fixedDelay);
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

    private PrimingRequest(PrimingRequestBuilder.PrimeType primeType, String query, String queryPattern, List<Consistency> consistency, List<Map<String, ?>> rows, Result result, List<ColumnMetadata> columnTypes, ColumnTypes[] variableTypes, Long fixedDelay) {
        this.primeType = primeType;
        this.when = new When(query, queryPattern, consistency);
        this.then = new Then(rows, result, columnTypes, variableTypes, fixedDelay);
    }

    public When getWhen() {
        return when;
    }

    public Then getThen() {
        return then;
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

    public final static class Then {
        private final ColumnTypes[] variable_types;
        private final List<Map<String, ? extends Object>> rows;
        private final Result result;
        private final Map<String, CqlType> column_types;
        private final Long fixedDelay;

        private Then(List<Map<String, ?>> rows, Result result, List<ColumnMetadata> column_types, ColumnTypes[] variable_types, Long fixedDelay) {
            this.rows = rows;
            this.result = result;
            this.variable_types = variable_types;
            this.fixedDelay = fixedDelay;
            if (column_types != null) {
                this.column_types = new HashMap<String, CqlType>();
                for (ColumnMetadata column_type : column_types) {
                    this.column_types.put(column_type.getName(), column_type.getType());
                }
            } else {
                this.column_types = null;
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(rows, result, column_types, fixedDelay) + Arrays.hashCode(variable_types);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            final Then other = (Then) obj;
            return Arrays.equals(this.variable_types, other.variable_types) &&
                    Objects.equals(this.rows, other.rows) &&
                    Objects.equals(this.result, other.result) &&
                    Objects.equals(this.column_types, other.column_types) &&
                    Objects.equals(this.fixedDelay, other.fixedDelay);
        }

        @Override
        public String toString() {
            return "Then{" +
                    "variable_types=" + Arrays.toString(variable_types) +
                    ", rows=" + rows +
                    ", result=" + result +
                    ", column_types=" + column_types +
                    ", fixedDelay=" + fixedDelay +
                    '}';
        }

        public ColumnTypes[] getVariableTypes() {
            return variable_types;
        }

        public List<Map<String, ? extends Object>> getRows() {
            return Collections.unmodifiableList(rows);
        }

        public Result getResult() {
            return result;
        }

        public Map<String, CqlType> getColumnTypes() {
            return column_types;
        }

        public long getFixedDelay() {
            return fixedDelay;
        }
    }

    public final static class When {
        private final String query;
        private final String queryPattern;
        private final List<Consistency> consistency;

        private When(String query, String queryPattern, List<Consistency> consistency) {
            this.query = query;
            this.consistency = consistency;
            this.queryPattern = queryPattern;
        }

        @Override
        public String toString() {
            return "When{" +
                    "query='" + query + '\'' +
                    ", queryPattern='" + queryPattern + '\'' +
                    ", consistency=" + consistency +
                    '}';
        }

        @Override
        public int hashCode() {
            return Objects.hash(query, queryPattern, consistency);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            final When other = (When) obj;
            return Objects.equals(this.query, other.query) && Objects.equals(this.queryPattern, other.queryPattern) && Objects.equals(this.consistency, other.consistency);
        }

        public String getQuery() {
            return query;
        }

        public List<Consistency> getConsistency() {
            return Collections.unmodifiableList(consistency);
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
