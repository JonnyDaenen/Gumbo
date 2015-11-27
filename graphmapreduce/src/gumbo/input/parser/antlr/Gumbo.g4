grammar Gumbo;		

script : (outputpath | scratchpath | input | select)* ;

input
	: relname '=' LOAD format file ARITY relarity SEMICOLON	#InputArity
	| relname '=' LOAD format file AS loadschema SEMICOLON	#InputSchema
	;

format
	: REL? 
	| CSV
	;

relname
	: ID
	;

relarity
	: INT
	;

file
	: QUOTE anystring QUOTE 
	;

schema
	: LPAR selector (',' selector)* RPAR
	| selector (',' selector)*
	;

selector
	: ID | PLACEHOLDER
	;

loadschema
	: LPAR ID (',' ID)* RPAR
	;

outputpath
	: SET OUTPUT DIR file SEMICOLON
	;

scratchpath
	: SET SCRATCH DIR file SEMICOLON
	;

select
	: relname '=' gfquery SEMICOLON
	;

gfquery
	: SELECT schema FROM relname satclause? WHERE expr
	;

satclause
	: SATISFYING assrt (AND assrt)*
	;

assrt
	: selector '=' QUOTE anystring QUOTE 	#ConstantAssert
	| selector '=' selector					#EqualityAssert
	;

expr
    : NOT expr 				#NotExpr
    | expr AND expr 		#AndExpr
    | expr OR expr 			#OrExpr	
    | guardedrel			#GuardedExpr
    | LPAR expr RPAR		#ParExpr
    ;

guardedrel
	: schema IN relname				#RegularGuarded
	| schema IN LPAR gfquery RPAR	#NestedGuarded
	;

anystring
	: keyword
	| ID
	| INT
	| FILENAME
	;

keyword
	: LOAD | REL | CSV | ARITY | AS | SET | DIR | OUTPUT | SCRATCH | SELECT | FROM | WHERE | IN | OR | AND | NOT
	;

LOAD 	: 'load' | 'LOAD' ;
REL 	: 'rel' | 'REL' ;
CSV 	: 'csv' | 'CSV' ;
ARITY	: 'arity' | 'ARITY' ;
AS		: 'as' | 'AS' ;
SET 	: 'set' | 'SET' ;
DIR	 	: 'dir' | 'DIR' ;
OUTPUT	: 'output' | 'OUTPUT' ;
SCRATCH	: 'scratch' | 'SCRATCH' ;
SELECT	: 'select' | 'SELECT' ;
FROM	: 'from' | 'FROM' ;
SATISFYING	: 'satisfying' | 'SATISFYING' ;
WHERE	: 'where' | 'WHERE' ;
IN		: 'in' | 'IN' ;
OR		: 'or' | 'OR' ;
AND		: 'and' | 'AND' ;
NOT		: 'not' | 'NOT' ;

INT			: [0-9]+ ; 
ID 			: [a-zA-Z0-9]+ ;
PLACEHOLDER	: '$' [0-9]+ ;
FILENAME 	: [a-zA-Z_/0-9\-.]+;

QUOTE 		: '\'' | '\"' ;
LPAR 		: '(' ;
RPAR 		: ')' ;
SEMICOLON	: ';' ;

WS	: [ \n\t\r]+ -> channel(HIDDEN) ;
SINGLE_LINE_COMMENT
	: '//' ~[\r\n]* -> channel(HIDDEN)
	;
