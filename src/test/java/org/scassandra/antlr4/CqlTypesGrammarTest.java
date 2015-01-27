package org.scassandra.antlr4;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.NotNull;
import org.junit.Test;

public class CqlTypesGrammarTest {
    @Test
    public void testParser() throws Exception {
        CqlTypesLexer lexer = new CqlTypesLexer(new ANTLRInputStream("map<text,text>"));
        CqlTypesParser parser = new CqlTypesParser(new CommonTokenStream(lexer));

        parser.addErrorListener(new BaseErrorListener() {
            @Override
            public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
                throw new IllegalArgumentException(msg);
            }
        });
        parser.addParseListener(new CqlTypesBaseListener() {
            @Override
            public void enterData_type(@NotNull CqlTypesParser.Data_typeContext ctx) {
                System.out.println("Type begins: " + ctx.start.getText());
            }

            @Override
            public void exitData_type(@NotNull CqlTypesParser.Data_typeContext ctx) {
                System.out.println("Type ends: " + ctx.start.getText());
            }

            @Override
            public void exitNative_type(@NotNull CqlTypesParser.Native_typeContext ctx) {
                System.out.println(ctx.start.getText());
            }

            @Override
            public void exitMap_type(@NotNull CqlTypesParser.Map_typeContext ctx) {
                System.out.println("start map:" + ctx.start.getText());
            }

            @Override
            public void enterMap_type(@NotNull CqlTypesParser.Map_typeContext ctx) {
                System.out.println("end map:" + ctx.start.getText());
            }
        });


        CqlTypesParser.Data_typeContext x = parser.data_type();
        System.out.println(x);
    }
}
