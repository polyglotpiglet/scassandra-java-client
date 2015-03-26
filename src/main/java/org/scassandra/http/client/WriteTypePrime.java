package org.scassandra.http.client;

public enum WriteTypePrime {
    SIMPLE,
    BATCH,
    UNLOGGED_BATCH,
    COUNTER,
    BATCH_LOG,
    CAS
}
