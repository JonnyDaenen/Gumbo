package gumbo.input.parser;

// Generated from Gumbo.g4 by ANTLR 4.5
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.RuntimeMetaData;
import org.antlr.v4.runtime.Vocabulary;
import org.antlr.v4.runtime.VocabularyImpl;
import org.antlr.v4.runtime.atn.ATN;
import org.antlr.v4.runtime.atn.ATNDeserializer;
import org.antlr.v4.runtime.atn.LexerATNSimulator;
import org.antlr.v4.runtime.atn.PredictionContextCache;
import org.antlr.v4.runtime.dfa.DFA;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class GumboLexer extends Lexer {
	static { RuntimeMetaData.checkVersion("4.5", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, LOAD=3, REL=4, CSV=5, ARITY=6, SET=7, DIR=8, OUTPUT=9, 
		SCRATCH=10, SELECT=11, FROM=12, SATISFYING=13, WHERE=14, IN=15, OR=16, 
		AND=17, NOT=18, INT=19, ID=20, PLACEHOLDER=21, FILENAME=22, QUOTE=23, 
		LPAR=24, RPAR=25, SEMICOLON=26, WS=27, SINGLE_LINE_COMMENT=28;
	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	public static final String[] ruleNames = {
		"T__0", "T__1", "LOAD", "REL", "CSV", "ARITY", "SET", "DIR", "OUTPUT", 
		"SCRATCH", "SELECT", "FROM", "SATISFYING", "WHERE", "IN", "OR", "AND", 
		"NOT", "INT", "ID", "PLACEHOLDER", "FILENAME", "QUOTE", "LPAR", "RPAR", 
		"SEMICOLON", "WS", "SINGLE_LINE_COMMENT"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'='", "','", null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		"'('", "')'", "';'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, "LOAD", "REL", "CSV", "ARITY", "SET", "DIR", "OUTPUT", 
		"SCRATCH", "SELECT", "FROM", "SATISFYING", "WHERE", "IN", "OR", "AND", 
		"NOT", "INT", "ID", "PLACEHOLDER", "FILENAME", "QUOTE", "LPAR", "RPAR", 
		"SEMICOLON", "WS", "SINGLE_LINE_COMMENT"
	};
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}


	public GumboLexer(CharStream input) {
		super(input);
		_interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@Override
	public String getGrammarFileName() { return "Gumbo.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public String[] getModeNames() { return modeNames; }

	@Override
	public ATN getATN() { return _ATN; }

	public static final String _serializedATN =
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2\36\u0118\b\1\4\2"+
		"\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4"+
		"\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22"+
		"\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31"+
		"\t\31\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\3\2\3\2\3\3\3\3\3\4\3\4"+
		"\3\4\3\4\3\4\3\4\3\4\3\4\5\4H\n\4\3\5\3\5\3\5\3\5\3\5\3\5\5\5P\n\5\3\6"+
		"\3\6\3\6\3\6\3\6\3\6\5\6X\n\6\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7"+
		"\5\7d\n\7\3\b\3\b\3\b\3\b\3\b\3\b\5\bl\n\b\3\t\3\t\3\t\3\t\3\t\3\t\5\t"+
		"t\n\t\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\5\n\u0082\n\n\3"+
		"\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\5"+
		"\13\u0092\n\13\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\5\f\u00a0"+
		"\n\f\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r\5\r\u00aa\n\r\3\16\3\16\3\16\3\16"+
		"\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16"+
		"\3\16\3\16\5\16\u00c0\n\16\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17"+
		"\3\17\5\17\u00cc\n\17\3\20\3\20\3\20\3\20\5\20\u00d2\n\20\3\21\3\21\3"+
		"\21\3\21\5\21\u00d8\n\21\3\22\3\22\3\22\3\22\3\22\3\22\5\22\u00e0\n\22"+
		"\3\23\3\23\3\23\3\23\3\23\3\23\5\23\u00e8\n\23\3\24\6\24\u00eb\n\24\r"+
		"\24\16\24\u00ec\3\25\6\25\u00f0\n\25\r\25\16\25\u00f1\3\26\3\26\6\26\u00f6"+
		"\n\26\r\26\16\26\u00f7\3\27\6\27\u00fb\n\27\r\27\16\27\u00fc\3\30\3\30"+
		"\3\31\3\31\3\32\3\32\3\33\3\33\3\34\6\34\u0108\n\34\r\34\16\34\u0109\3"+
		"\34\3\34\3\35\3\35\3\35\3\35\7\35\u0112\n\35\f\35\16\35\u0115\13\35\3"+
		"\35\3\35\2\2\36\3\3\5\4\7\5\t\6\13\7\r\b\17\t\21\n\23\13\25\f\27\r\31"+
		"\16\33\17\35\20\37\21!\22#\23%\24\'\25)\26+\27-\30/\31\61\32\63\33\65"+
		"\34\67\359\36\3\2\b\3\2\62;\5\2\62;C\\c|\6\2/;C\\aac|\4\2$$))\5\2\13\f"+
		"\17\17\"\"\4\2\f\f\17\17\u012d\2\3\3\2\2\2\2\5\3\2\2\2\2\7\3\2\2\2\2\t"+
		"\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2\2\2\2\23\3\2\2"+
		"\2\2\25\3\2\2\2\2\27\3\2\2\2\2\31\3\2\2\2\2\33\3\2\2\2\2\35\3\2\2\2\2"+
		"\37\3\2\2\2\2!\3\2\2\2\2#\3\2\2\2\2%\3\2\2\2\2\'\3\2\2\2\2)\3\2\2\2\2"+
		"+\3\2\2\2\2-\3\2\2\2\2/\3\2\2\2\2\61\3\2\2\2\2\63\3\2\2\2\2\65\3\2\2\2"+
		"\2\67\3\2\2\2\29\3\2\2\2\3;\3\2\2\2\5=\3\2\2\2\7G\3\2\2\2\tO\3\2\2\2\13"+
		"W\3\2\2\2\rc\3\2\2\2\17k\3\2\2\2\21s\3\2\2\2\23\u0081\3\2\2\2\25\u0091"+
		"\3\2\2\2\27\u009f\3\2\2\2\31\u00a9\3\2\2\2\33\u00bf\3\2\2\2\35\u00cb\3"+
		"\2\2\2\37\u00d1\3\2\2\2!\u00d7\3\2\2\2#\u00df\3\2\2\2%\u00e7\3\2\2\2\'"+
		"\u00ea\3\2\2\2)\u00ef\3\2\2\2+\u00f3\3\2\2\2-\u00fa\3\2\2\2/\u00fe\3\2"+
		"\2\2\61\u0100\3\2\2\2\63\u0102\3\2\2\2\65\u0104\3\2\2\2\67\u0107\3\2\2"+
		"\29\u010d\3\2\2\2;<\7?\2\2<\4\3\2\2\2=>\7.\2\2>\6\3\2\2\2?@\7n\2\2@A\7"+
		"q\2\2AB\7c\2\2BH\7f\2\2CD\7N\2\2DE\7Q\2\2EF\7C\2\2FH\7F\2\2G?\3\2\2\2"+
		"GC\3\2\2\2H\b\3\2\2\2IJ\7t\2\2JK\7g\2\2KP\7n\2\2LM\7T\2\2MN\7G\2\2NP\7"+
		"N\2\2OI\3\2\2\2OL\3\2\2\2P\n\3\2\2\2QR\7e\2\2RS\7u\2\2SX\7x\2\2TU\7E\2"+
		"\2UV\7U\2\2VX\7X\2\2WQ\3\2\2\2WT\3\2\2\2X\f\3\2\2\2YZ\7c\2\2Z[\7t\2\2"+
		"[\\\7k\2\2\\]\7v\2\2]d\7{\2\2^_\7C\2\2_`\7T\2\2`a\7K\2\2ab\7V\2\2bd\7"+
		"[\2\2cY\3\2\2\2c^\3\2\2\2d\16\3\2\2\2ef\7u\2\2fg\7g\2\2gl\7v\2\2hi\7U"+
		"\2\2ij\7G\2\2jl\7V\2\2ke\3\2\2\2kh\3\2\2\2l\20\3\2\2\2mn\7f\2\2no\7k\2"+
		"\2ot\7t\2\2pq\7F\2\2qr\7K\2\2rt\7T\2\2sm\3\2\2\2sp\3\2\2\2t\22\3\2\2\2"+
		"uv\7q\2\2vw\7w\2\2wx\7v\2\2xy\7r\2\2yz\7w\2\2z\u0082\7v\2\2{|\7Q\2\2|"+
		"}\7W\2\2}~\7V\2\2~\177\7R\2\2\177\u0080\7W\2\2\u0080\u0082\7V\2\2\u0081"+
		"u\3\2\2\2\u0081{\3\2\2\2\u0082\24\3\2\2\2\u0083\u0084\7u\2\2\u0084\u0085"+
		"\7e\2\2\u0085\u0086\7t\2\2\u0086\u0087\7c\2\2\u0087\u0088\7v\2\2\u0088"+
		"\u0089\7e\2\2\u0089\u0092\7j\2\2\u008a\u008b\7U\2\2\u008b\u008c\7E\2\2"+
		"\u008c\u008d\7T\2\2\u008d\u008e\7C\2\2\u008e\u008f\7V\2\2\u008f\u0090"+
		"\7E\2\2\u0090\u0092\7J\2\2\u0091\u0083\3\2\2\2\u0091\u008a\3\2\2\2\u0092"+
		"\26\3\2\2\2\u0093\u0094\7u\2\2\u0094\u0095\7g\2\2\u0095\u0096\7n\2\2\u0096"+
		"\u0097\7g\2\2\u0097\u0098\7e\2\2\u0098\u00a0\7v\2\2\u0099\u009a\7U\2\2"+
		"\u009a\u009b\7G\2\2\u009b\u009c\7N\2\2\u009c\u009d\7G\2\2\u009d\u009e"+
		"\7E\2\2\u009e\u00a0\7V\2\2\u009f\u0093\3\2\2\2\u009f\u0099\3\2\2\2\u00a0"+
		"\30\3\2\2\2\u00a1\u00a2\7h\2\2\u00a2\u00a3\7t\2\2\u00a3\u00a4\7q\2\2\u00a4"+
		"\u00aa\7o\2\2\u00a5\u00a6\7H\2\2\u00a6\u00a7\7T\2\2\u00a7\u00a8\7Q\2\2"+
		"\u00a8\u00aa\7O\2\2\u00a9\u00a1\3\2\2\2\u00a9\u00a5\3\2\2\2\u00aa\32\3"+
		"\2\2\2\u00ab\u00ac\7u\2\2\u00ac\u00ad\7c\2\2\u00ad\u00ae\7v\2\2\u00ae"+
		"\u00af\7k\2\2\u00af\u00b0\7u\2\2\u00b0\u00b1\7h\2\2\u00b1\u00b2\7{\2\2"+
		"\u00b2\u00b3\7k\2\2\u00b3\u00b4\7p\2\2\u00b4\u00c0\7i\2\2\u00b5\u00b6"+
		"\7U\2\2\u00b6\u00b7\7C\2\2\u00b7\u00b8\7V\2\2\u00b8\u00b9\7K\2\2\u00b9"+
		"\u00ba\7U\2\2\u00ba\u00bb\7H\2\2\u00bb\u00bc\7[\2\2\u00bc\u00bd\7K\2\2"+
		"\u00bd\u00be\7P\2\2\u00be\u00c0\7I\2\2\u00bf\u00ab\3\2\2\2\u00bf\u00b5"+
		"\3\2\2\2\u00c0\34\3\2\2\2\u00c1\u00c2\7y\2\2\u00c2\u00c3\7j\2\2\u00c3"+
		"\u00c4\7g\2\2\u00c4\u00c5\7t\2\2\u00c5\u00cc\7g\2\2\u00c6\u00c7\7Y\2\2"+
		"\u00c7\u00c8\7J\2\2\u00c8\u00c9\7G\2\2\u00c9\u00ca\7T\2\2\u00ca\u00cc"+
		"\7G\2\2\u00cb\u00c1\3\2\2\2\u00cb\u00c6\3\2\2\2\u00cc\36\3\2\2\2\u00cd"+
		"\u00ce\7k\2\2\u00ce\u00d2\7p\2\2\u00cf\u00d0\7K\2\2\u00d0\u00d2\7P\2\2"+
		"\u00d1\u00cd\3\2\2\2\u00d1\u00cf\3\2\2\2\u00d2 \3\2\2\2\u00d3\u00d4\7"+
		"q\2\2\u00d4\u00d8\7t\2\2\u00d5\u00d6\7Q\2\2\u00d6\u00d8\7T\2\2\u00d7\u00d3"+
		"\3\2\2\2\u00d7\u00d5\3\2\2\2\u00d8\"\3\2\2\2\u00d9\u00da\7c\2\2\u00da"+
		"\u00db\7p\2\2\u00db\u00e0\7f\2\2\u00dc\u00dd\7C\2\2\u00dd\u00de\7P\2\2"+
		"\u00de\u00e0\7F\2\2\u00df\u00d9\3\2\2\2\u00df\u00dc\3\2\2\2\u00e0$\3\2"+
		"\2\2\u00e1\u00e2\7p\2\2\u00e2\u00e3\7q\2\2\u00e3\u00e8\7v\2\2\u00e4\u00e5"+
		"\7P\2\2\u00e5\u00e6\7Q\2\2\u00e6\u00e8\7V\2\2\u00e7\u00e1\3\2\2\2\u00e7"+
		"\u00e4\3\2\2\2\u00e8&\3\2\2\2\u00e9\u00eb\t\2\2\2\u00ea\u00e9\3\2\2\2"+
		"\u00eb\u00ec\3\2\2\2\u00ec\u00ea\3\2\2\2\u00ec\u00ed\3\2\2\2\u00ed(\3"+
		"\2\2\2\u00ee\u00f0\t\3\2\2\u00ef\u00ee\3\2\2\2\u00f0\u00f1\3\2\2\2\u00f1"+
		"\u00ef\3\2\2\2\u00f1\u00f2\3\2\2\2\u00f2*\3\2\2\2\u00f3\u00f5\7&\2\2\u00f4"+
		"\u00f6\t\2\2\2\u00f5\u00f4\3\2\2\2\u00f6\u00f7\3\2\2\2\u00f7\u00f5\3\2"+
		"\2\2\u00f7\u00f8\3\2\2\2\u00f8,\3\2\2\2\u00f9\u00fb\t\4\2\2\u00fa\u00f9"+
		"\3\2\2\2\u00fb\u00fc\3\2\2\2\u00fc\u00fa\3\2\2\2\u00fc\u00fd\3\2\2\2\u00fd"+
		".\3\2\2\2\u00fe\u00ff\t\5\2\2\u00ff\60\3\2\2\2\u0100\u0101\7*\2\2\u0101"+
		"\62\3\2\2\2\u0102\u0103\7+\2\2\u0103\64\3\2\2\2\u0104\u0105\7=\2\2\u0105"+
		"\66\3\2\2\2\u0106\u0108\t\6\2\2\u0107\u0106\3\2\2\2\u0108\u0109\3\2\2"+
		"\2\u0109\u0107\3\2\2\2\u0109\u010a\3\2\2\2\u010a\u010b\3\2\2\2\u010b\u010c"+
		"\b\34\2\2\u010c8\3\2\2\2\u010d\u010e\7\61\2\2\u010e\u010f\7\61\2\2\u010f"+
		"\u0113\3\2\2\2\u0110\u0112\n\7\2\2\u0111\u0110\3\2\2\2\u0112\u0115\3\2"+
		"\2\2\u0113\u0111\3\2\2\2\u0113\u0114\3\2\2\2\u0114\u0116\3\2\2\2\u0115"+
		"\u0113\3\2\2\2\u0116\u0117\b\35\2\2\u0117:\3\2\2\2\31\2GOWcks\u0081\u0091"+
		"\u009f\u00a9\u00bf\u00cb\u00d1\u00d7\u00df\u00e7\u00ec\u00f1\u00f7\u00fc"+
		"\u0109\u0113\3\2\3\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}