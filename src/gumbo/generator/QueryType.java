package gumbo.generator;

/**
 * Enum to specify the type of query to generate
 * @author brentchesny
 *
 */
public enum QueryType {
	AND,
	OR,
	XOR,
	NEGATED_AND,
	NEGATED_OR,
	NEGATED_XOR,
	UNKNOWN
}