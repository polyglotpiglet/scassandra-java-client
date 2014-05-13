package org.scassandra.http.client;

import java.util.List;

public class PreparedStatementExecution {
    private String preparedStatementText;
    private String consistency;
    private List<String> variables;

    public String getPreparedStatementText() {
        return preparedStatementText;
    }

    public String getConsistency() {
        return consistency;
    }

    public List<String> getVariables() {
        return variables;
    }

    @Override
    public String toString() {
        return "PreparedStatementExecution{" +
                "preparedStatementText='" + preparedStatementText + '\'' +
                ", consistency='" + consistency + '\'' +
                ", variables=" + variables +
                '}';
    }
}
