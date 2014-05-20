package org.scassandra;

import org.scassandra.http.client.ActivityClient;
import org.scassandra.http.client.PrimingClient;

/**
 * Interface with Scassandra.
 */
public interface Scassandra {
    /**
     * Retrieves a Priming client that is configured with the same admin port
     * as Scassandra.
     * @return PrimingClient
     */
    PrimingClient primingClient();

    /**
     * Retrieves an Activity client that is configured with the same admin port
     * as Scassandra.
     * @return ActivityClient
     */
    ActivityClient activityClient();

    /**
     * Start Scassandra. This will result in both the binary port for Cassandra to be opened
     * and the admin port for priming and verifying recorded activity.
     */
    void start();

    /**
     * Stops Scassandra.
     */
    void stop();
}
