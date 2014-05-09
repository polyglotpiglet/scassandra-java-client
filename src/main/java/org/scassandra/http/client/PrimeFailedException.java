package org.scassandra.http.client;

public class PrimeFailedException extends RuntimeException {
    public PrimeFailedException(String s) {
        super(s);
    }

    public PrimeFailedException() {
    }
}
