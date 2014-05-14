package org.scassandra.http.client;

public class PrimeFailedException extends RuntimeException {
    PrimeFailedException(String s) {
        super(s);
    }

    PrimeFailedException() {
    }
}
