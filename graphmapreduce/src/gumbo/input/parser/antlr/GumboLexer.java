package gumbo.input.parser.antlr;

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
		T__0=1, T__1=2, LOAD=3, REL=4, CSV=5, ARITY=6, AS=7, SET=8, DIR=9, OUTPUT=10, 
		SCRATCH=11, SELECT=12, FROM=13, SATISFYING=14, WHERE=15, IN=16, OR=17, 
		AND=18, NOT=19, INT=20, ID=21, PLACEHOLDER=22, FILENAME=23, QUOTE=24, 
		LPAR=25, RPAR=26, SEMICOLON=27, WS=28, SINGLE_LINE_COMMENT=29;
	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	public static final String[] ruleNames = {
		"T__0", "T__1", "LOAD", "REL", "CSV", "ARITY", "AS", "SET", "DIR", "OUTPUT", 
		"SCRATCH", "SELECT", "FROM", "SATISFYING", "WHERE", "IN", "OR", "AND", 
		"NOT", "INT", "ID", "PLACEHOLDER", "FILENAME", "QUOTE", "LPAR", "RPAR", 
		"SEMICOLON", "WS", "SINGLE_LINE_COMMENT"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'='", "','", null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, "'('", "')'", "';'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, "LOAD", "REL", "CSV", "ARITY", "AS", "SET", "DIR", "OUTPUT", 
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
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2\37\u0120\b\1\4\2"+
		"\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4"+
		"\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22"+
		"\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31"+
		"\t\31\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\3\2\3\2\3\3\3"+
		"\3\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\5\4J\n\4\3\5\3\5\3\5\3\5\3\5\3\5\5"+
		"\5R\n\5\3\6\3\6\3\6\3\6\3\6\3\6\5\6Z\n\6\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3"+
		"\7\3\7\3\7\5\7f\n\7\3\b\3\b\3\b\3\b\5\bl\n\b\3\t\3\t\3\t\3\t\3\t\3\t\5"+
		"\tt\n\t\3\n\3\n\3\n\3\n\3\n\3\n\5\n|\n\n\3\13\3\13\3\13\3\13\3\13\3\13"+
		"\3\13\3\13\3\13\3\13\3\13\3\13\5\13\u008a\n\13\3\f\3\f\3\f\3\f\3\f\3\f"+
		"\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\5\f\u009a\n\f\3\r\3\r\3\r\3\r\3\r\3\r"+
		"\3\r\3\r\3\r\3\r\3\r\3\r\5\r\u00a8\n\r\3\16\3\16\3\16\3\16\3\16\3\16\3"+
		"\16\3\16\5\16\u00b2\n\16\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17"+
		"\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\5\17\u00c8\n\17"+
		"\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\5\20\u00d4\n\20\3\21"+
		"\3\21\3\21\3\21\5\21\u00da\n\21\3\22\3\22\3\22\3\22\5\22\u00e0\n\22\3"+
		"\23\3\23\3\23\3\23\3\23\3\23\5\23\u00e8\n\23\3\24\3\24\3\24\3\24\3\24"+
		"\3\24\5\24\u00f0\n\24\3\25\6\25\u00f3\n\25\r\25\16\25\u00f4\3\26\6\26"+
		"\u00f8\n\26\r\26\16\26\u00f9\3\27\3\27\6\27\u00fe\n\27\r\27\16\27\u00ff"+
		"\3\30\6\30\u0103\n\30\r\30\16\30\u0104\3\31\3\31\3\32\3\32\3\33\3\33\3"+
		"\34\3\34\3\35\6\35\u0110\n\35\r\35\16\35\u0111\3\35\3\35\3\36\3\36\3\36"+
		"\3\36\7\36\u011a\n\36\f\36\16\36\u011d\13\36\3\36\3\36\2\2\37\3\3\5\4"+
		"\7\5\t\6\13\7\r\b\17\t\21\n\23\13\25\f\27\r\31\16\33\17\35\20\37\21!\22"+
		"#\23%\24\'\25)\26+\27-\30/\31\61\32\63\33\65\34\67\359\36;\37\3\2\b\3"+
		"\2\62;\5\2\62;C\\c|\6\2/;C\\aac|\4\2$$))\5\2\13\f\17\17\"\"\4\2\f\f\17"+
		"\17\u0136\2\3\3\2\2\2\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2"+
		"\2\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\27"+
		"\3\2\2\2\2\31\3\2\2\2\2\33\3\2\2\2\2\35\3\2\2\2\2\37\3\2\2\2\2!\3\2\2"+
		"\2\2#\3\2\2\2\2%\3\2\2\2\2\'\3\2\2\2\2)\3\2\2\2\2+\3\2\2\2\2-\3\2\2\2"+
		"\2/\3\2\2\2\2\61\3\2\2\2\2\63\3\2\2\2\2\65\3\2\2\2\2\67\3\2\2\2\29\3\2"+
		"\2\2\2;\3\2\2\2\3=\3\2\2\2\5?\3\2\2\2\7I\3\2\2\2\tQ\3\2\2\2\13Y\3\2\2"+
		"\2\re\3\2\2\2\17k\3\2\2\2\21s\3\2\2\2\23{\3\2\2\2\25\u0089\3\2\2\2\27"+
		"\u0099\3\2\2\2\31\u00a7\3\2\2\2\33\u00b1\3\2\2\2\35\u00c7\3\2\2\2\37\u00d3"+
		"\3\2\2\2!\u00d9\3\2\2\2#\u00df\3\2\2\2%\u00e7\3\2\2\2\'\u00ef\3\2\2\2"+
		")\u00f2\3\2\2\2+\u00f7\3\2\2\2-\u00fb\3\2\2\2/\u0102\3\2\2\2\61\u0106"+
		"\3\2\2\2\63\u0108\3\2\2\2\65\u010a\3\2\2\2\67\u010c\3\2\2\29\u010f\3\2"+
		"\2\2;\u0115\3\2\2\2=>\7?\2\2>\4\3\2\2\2?@\7.\2\2@\6\3\2\2\2AB\7n\2\2B"+
		"C\7q\2\2CD\7c\2\2DJ\7f\2\2EF\7N\2\2FG\7Q\2\2GH\7C\2\2HJ\7F\2\2IA\3\2\2"+
		"\2IE\3\2\2\2J\b\3\2\2\2KL\7t\2\2LM\7g\2\2MR\7n\2\2NO\7T\2\2OP\7G\2\2P"+
		"R\7N\2\2QK\3\2\2\2QN\3\2\2\2R\n\3\2\2\2ST\7e\2\2TU\7u\2\2UZ\7x\2\2VW\7"+
		"E\2\2WX\7U\2\2XZ\7X\2\2YS\3\2\2\2YV\3\2\2\2Z\f\3\2\2\2[\\\7c\2\2\\]\7"+
		"t\2\2]^\7k\2\2^_\7v\2\2_f\7{\2\2`a\7C\2\2ab\7T\2\2bc\7K\2\2cd\7V\2\2d"+
		"f\7[\2\2e[\3\2\2\2e`\3\2\2\2f\16\3\2\2\2gh\7c\2\2hl\7u\2\2ij\7C\2\2jl"+
		"\7U\2\2kg\3\2\2\2ki\3\2\2\2l\20\3\2\2\2mn\7u\2\2no\7g\2\2ot\7v\2\2pq\7"+
		"U\2\2qr\7G\2\2rt\7V\2\2sm\3\2\2\2sp\3\2\2\2t\22\3\2\2\2uv\7f\2\2vw\7k"+
		"\2\2w|\7t\2\2xy\7F\2\2yz\7K\2\2z|\7T\2\2{u\3\2\2\2{x\3\2\2\2|\24\3\2\2"+
		"\2}~\7q\2\2~\177\7w\2\2\177\u0080\7v\2\2\u0080\u0081\7r\2\2\u0081\u0082"+
		"\7w\2\2\u0082\u008a\7v\2\2\u0083\u0084\7Q\2\2\u0084\u0085\7W\2\2\u0085"+
		"\u0086\7V\2\2\u0086\u0087\7R\2\2\u0087\u0088\7W\2\2\u0088\u008a\7V\2\2"+
		"\u0089}\3\2\2\2\u0089\u0083\3\2\2\2\u008a\26\3\2\2\2\u008b\u008c\7u\2"+
		"\2\u008c\u008d\7e\2\2\u008d\u008e\7t\2\2\u008e\u008f\7c\2\2\u008f\u0090"+
		"\7v\2\2\u0090\u0091\7e\2\2\u0091\u009a\7j\2\2\u0092\u0093\7U\2\2\u0093"+
		"\u0094\7E\2\2\u0094\u0095\7T\2\2\u0095\u0096\7C\2\2\u0096\u0097\7V\2\2"+
		"\u0097\u0098\7E\2\2\u0098\u009a\7J\2\2\u0099\u008b\3\2\2\2\u0099\u0092"+
		"\3\2\2\2\u009a\30\3\2\2\2\u009b\u009c\7u\2\2\u009c\u009d\7g\2\2\u009d"+
		"\u009e\7n\2\2\u009e\u009f\7g\2\2\u009f\u00a0\7e\2\2\u00a0\u00a8\7v\2\2"+
		"\u00a1\u00a2\7U\2\2\u00a2\u00a3\7G\2\2\u00a3\u00a4\7N\2\2\u00a4\u00a5"+
		"\7G\2\2\u00a5\u00a6\7E\2\2\u00a6\u00a8\7V\2\2\u00a7\u009b\3\2\2\2\u00a7"+
		"\u00a1\3\2\2\2\u00a8\32\3\2\2\2\u00a9\u00aa\7h\2\2\u00aa\u00ab\7t\2\2"+
		"\u00ab\u00ac\7q\2\2\u00ac\u00b2\7o\2\2\u00ad\u00ae\7H\2\2\u00ae\u00af"+
		"\7T\2\2\u00af\u00b0\7Q\2\2\u00b0\u00b2\7O\2\2\u00b1\u00a9\3\2\2\2\u00b1"+
		"\u00ad\3\2\2\2\u00b2\34\3\2\2\2\u00b3\u00b4\7u\2\2\u00b4\u00b5\7c\2\2"+
		"\u00b5\u00b6\7v\2\2\u00b6\u00b7\7k\2\2\u00b7\u00b8\7u\2\2\u00b8\u00b9"+
		"\7h\2\2\u00b9\u00ba\7{\2\2\u00ba\u00bb\7k\2\2\u00bb\u00bc\7p\2\2\u00bc"+
		"\u00c8\7i\2\2\u00bd\u00be\7U\2\2\u00be\u00bf\7C\2\2\u00bf\u00c0\7V\2\2"+
		"\u00c0\u00c1\7K\2\2\u00c1\u00c2\7U\2\2\u00c2\u00c3\7H\2\2\u00c3\u00c4"+
		"\7[\2\2\u00c4\u00c5\7K\2\2\u00c5\u00c6\7P\2\2\u00c6\u00c8\7I\2\2\u00c7"+
		"\u00b3\3\2\2\2\u00c7\u00bd\3\2\2\2\u00c8\36\3\2\2\2\u00c9\u00ca\7y\2\2"+
		"\u00ca\u00cb\7j\2\2\u00cb\u00cc\7g\2\2\u00cc\u00cd\7t\2\2\u00cd\u00d4"+
		"\7g\2\2\u00ce\u00cf\7Y\2\2\u00cf\u00d0\7J\2\2\u00d0\u00d1\7G\2\2\u00d1"+
		"\u00d2\7T\2\2\u00d2\u00d4\7G\2\2\u00d3\u00c9\3\2\2\2\u00d3\u00ce\3\2\2"+
		"\2\u00d4 \3\2\2\2\u00d5\u00d6\7k\2\2\u00d6\u00da\7p\2\2\u00d7\u00d8\7"+
		"K\2\2\u00d8\u00da\7P\2\2\u00d9\u00d5\3\2\2\2\u00d9\u00d7\3\2\2\2\u00da"+
		"\"\3\2\2\2\u00db\u00dc\7q\2\2\u00dc\u00e0\7t\2\2\u00dd\u00de\7Q\2\2\u00de"+
		"\u00e0\7T\2\2\u00df\u00db\3\2\2\2\u00df\u00dd\3\2\2\2\u00e0$\3\2\2\2\u00e1"+
		"\u00e2\7c\2\2\u00e2\u00e3\7p\2\2\u00e3\u00e8\7f\2\2\u00e4\u00e5\7C\2\2"+
		"\u00e5\u00e6\7P\2\2\u00e6\u00e8\7F\2\2\u00e7\u00e1\3\2\2\2\u00e7\u00e4"+
		"\3\2\2\2\u00e8&\3\2\2\2\u00e9\u00ea\7p\2\2\u00ea\u00eb\7q\2\2\u00eb\u00f0"+
		"\7v\2\2\u00ec\u00ed\7P\2\2\u00ed\u00ee\7Q\2\2\u00ee\u00f0\7V\2\2\u00ef"+
		"\u00e9\3\2\2\2\u00ef\u00ec\3\2\2\2\u00f0(\3\2\2\2\u00f1\u00f3\t\2\2\2"+
		"\u00f2\u00f1\3\2\2\2\u00f3\u00f4\3\2\2\2\u00f4\u00f2\3\2\2\2\u00f4\u00f5"+
		"\3\2\2\2\u00f5*\3\2\2\2\u00f6\u00f8\t\3\2\2\u00f7\u00f6\3\2\2\2\u00f8"+
		"\u00f9\3\2\2\2\u00f9\u00f7\3\2\2\2\u00f9\u00fa\3\2\2\2\u00fa,\3\2\2\2"+
		"\u00fb\u00fd\7&\2\2\u00fc\u00fe\t\2\2\2\u00fd\u00fc\3\2\2\2\u00fe\u00ff"+
		"\3\2\2\2\u00ff\u00fd\3\2\2\2\u00ff\u0100\3\2\2\2\u0100.\3\2\2\2\u0101"+
		"\u0103\t\4\2\2\u0102\u0101\3\2\2\2\u0103\u0104\3\2\2\2\u0104\u0102\3\2"+
		"\2\2\u0104\u0105\3\2\2\2\u0105\60\3\2\2\2\u0106\u0107\t\5\2\2\u0107\62"+
		"\3\2\2\2\u0108\u0109\7*\2\2\u0109\64\3\2\2\2\u010a\u010b\7+\2\2\u010b"+
		"\66\3\2\2\2\u010c\u010d\7=\2\2\u010d8\3\2\2\2\u010e\u0110\t\6\2\2\u010f"+
		"\u010e\3\2\2\2\u0110\u0111\3\2\2\2\u0111\u010f\3\2\2\2\u0111\u0112\3\2"+
		"\2\2\u0112\u0113\3\2\2\2\u0113\u0114\b\35\2\2\u0114:\3\2\2\2\u0115\u0116"+
		"\7\61\2\2\u0116\u0117\7\61\2\2\u0117\u011b\3\2\2\2\u0118\u011a\n\7\2\2"+
		"\u0119\u0118\3\2\2\2\u011a\u011d\3\2\2\2\u011b\u0119\3\2\2\2\u011b\u011c"+
		"\3\2\2\2\u011c\u011e\3\2\2\2\u011d\u011b\3\2\2\2\u011e\u011f\b\36\2\2"+
		"\u011f<\3\2\2\2\32\2IQYeks{\u0089\u0099\u00a7\u00b1\u00c7\u00d3\u00d9"+
		"\u00df\u00e7\u00ef\u00f4\u00f9\u00ff\u0104\u0111\u011b\3\2\3\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}