package org.scassandra.http.client;

import com.google.common.collect.ImmutableMap;
import org.scassandra.server.priming.ErrorConstants;

import java.util.Map;

public class ReadTimeoutConfig extends Config {
    private final int receivedAcknowledgements;
    private final int requiredAcknowledgements;
    private final boolean dataRetrieved;

    public ReadTimeoutConfig(int receivedAcknowledgements, int requiredAcknowledgements, boolean dataRetrieved) {
        this.receivedAcknowledgements = receivedAcknowledgements;
        this.requiredAcknowledgements = requiredAcknowledgements;
        this.dataRetrieved = dataRetrieved;
    }

    @Override
    Map<String, ?> getProperties() {
        return ImmutableMap.of(
                ErrorConstants.ReceivedResponse(), String.valueOf(this.receivedAcknowledgements),
                ErrorConstants.RequiredResponse(), String.valueOf(this.requiredAcknowledgements),
                ErrorConstants.DataPresent(), String.valueOf(this.dataRetrieved)
        );
    }
}
