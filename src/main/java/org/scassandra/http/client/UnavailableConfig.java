package org.scassandra.http.client;

import com.google.common.collect.ImmutableMap;
import org.scassandra.server.priming.ErrorConstants;

import java.util.Map;

public class UnavailableConfig extends Config {

    private final int requiredAcknowledgements;
    private final int alive;

    public UnavailableConfig(int requiredAcknowledgements, int alive) {
        this.requiredAcknowledgements = requiredAcknowledgements;
        this.alive = alive;
    }

    @Override
    Map<String, ?> getProperties() {
        return ImmutableMap.of(
                ErrorConstants.Alive(), String.valueOf(this.alive),
                ErrorConstants.RequiredResponse(), String.valueOf(this.requiredAcknowledgements)
        );
    }
}
