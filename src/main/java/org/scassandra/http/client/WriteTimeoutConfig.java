package org.scassandra.http.client;

import com.google.common.collect.ImmutableMap;
import org.scassandra.server.priming.ErrorConstants;

import java.util.Map;

public class WriteTimeoutConfig extends Config {

    private final WriteTypePrime writeType;
    private final int receivedAcknowledgements;
    private final int requiredAcknowledgements;

    public WriteTimeoutConfig(WriteTypePrime writeType, int receivedAcknowledgements, int requiredAcknowledgements) {
        this.writeType = writeType;
        this.receivedAcknowledgements = receivedAcknowledgements;
        this.requiredAcknowledgements = requiredAcknowledgements;
    }

    @Override
    Map<String, ?> getProperties() {
        return ImmutableMap.of(
                ErrorConstants.ReceivedResponse(), String.valueOf(this.receivedAcknowledgements),
                ErrorConstants.RequiredResponse(), String.valueOf(this.requiredAcknowledgements),
                ErrorConstants.WriteType(), String.valueOf(writeType.toString())
        );
    }
}
