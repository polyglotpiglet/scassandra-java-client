package org.scassandra.http.client;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class PrimingRequestTest {
    @Test
    public void throwsIllegalStateExceptionIfVariablesTypesSetForQueryPrime() {
        //given
        //when
        try {
            PrimingRequest.queryBuilder()
                    .withVariableTypes(ColumnTypes.Bigint)
                    .build();
            fail("Expected illegal state exception");
        } catch (IllegalStateException e) {
            //then
            assertEquals(e.getMessage(), "Variable types only applicable for a prepared statement prime. Not a query prime.");
        }
    }
}
