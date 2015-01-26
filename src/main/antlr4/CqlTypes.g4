grammar CqlTypes;

r  : 'Hello' ID ;
ID : [a-z]+ ;
WS : [\t\r\n]+ -> skip;