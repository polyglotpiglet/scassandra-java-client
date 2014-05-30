package org.scassandra;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.lang.annotation.Annotation;

public class ScassandraServerRule implements TestRule {

    private Scassandra scassandra;

    public ScassandraServerRule(Scassandra scassandra){
        this.scassandra = scassandra;
    }

    @Override
    public Statement apply(final Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                scassandra.start();
                try {
                    base.evaluate();
                }
                finally {
                    scassandra.stop();
                }
            }
        };
    }

}
