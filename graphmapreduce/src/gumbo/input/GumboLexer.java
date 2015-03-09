package gumbo.input;

// Generated from Gumbo.g4 by ANTLR 4.5
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class GumboLexer extends Lexer {
	static { RuntimeMetaData.checkVersion("4.5", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, LOAD=3, REL=4, CSV=5, AS=6, SET=7, DIR=8, OUTPUT=9, SCRATCH=10, 
		SELECT=11, FROM=12, WHERE=13, IN=14, OR=15, AND=16, NOT=17, ID=18, FILENAME=19, 
		QUOTE=20, LPAR=21, RPAR=22, SEMICOLON=23, WS=24, SINGLE_LINE_COMMENT=25;
	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	public static final String[] ruleNames = {
		"T__0", "T__1", "LOAD", "REL", "CSV", "AS", "SET", "DIR", "OUTPUT", "SCRATCH", 
		"SELECT", "FROM", "WHERE", "IN", "OR", "AND", "NOT", "ID", "FILENAME", 
		"QUOTE", "LPAR", "RPAR", "SEMICOLON", "WS", "SINGLE_LINE_COMMENT"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'='", "','", null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, "'('", "')'", "';'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, "LOAD", "REL", "CSV", "AS", "SET", "DIR", "OUTPUT", 
		"SCRATCH", "SELECT", "FROM", "WHERE", "IN", "OR", "AND", "NOT", "ID", 
		"FILENAME", "QUOTE", "LPAR", "RPAR", "SEMICOLON", "WS", "SINGLE_LINE_COMMENT"
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
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2\33\u00eb\b\1\4\2"+
		"\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4"+
		"\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22"+
		"\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31"+
		"\t\31\4\32\t\32\3\2\3\2\3\3\3\3\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\5\4B\n"+
		"\4\3\5\3\5\3\5\3\5\3\5\3\5\5\5J\n\5\3\6\3\6\3\6\3\6\3\6\3\6\5\6R\n\6\3"+
		"\7\3\7\3\7\3\7\5\7X\n\7\3\b\3\b\3\b\3\b\3\b\3\b\5\b`\n\b\3\t\3\t\3\t\3"+
		"\t\3\t\3\t\5\th\n\t\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\5"+
		"\nv\n\n\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3"+
		"\13\3\13\5\13\u0086\n\13\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3"+
		"\f\5\f\u0094\n\f\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r\5\r\u009e\n\r\3\16\3"+
		"\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\5\16\u00aa\n\16\3\17\3\17"+
		"\3\17\3\17\5\17\u00b0\n\17\3\20\3\20\3\20\3\20\5\20\u00b6\n\20\3\21\3"+
		"\21\3\21\3\21\3\21\3\21\5\21\u00be\n\21\3\22\3\22\3\22\3\22\3\22\3\22"+
		"\5\22\u00c6\n\22\3\23\6\23\u00c9\n\23\r\23\16\23\u00ca\3\24\6\24\u00ce"+
		"\n\24\r\24\16\24\u00cf\3\25\3\25\3\26\3\26\3\27\3\27\3\30\3\30\3\31\6"+
		"\31\u00db\n\31\r\31\16\31\u00dc\3\31\3\31\3\32\3\32\3\32\3\32\7\32\u00e5"+
		"\n\32\f\32\16\32\u00e8\13\32\3\32\3\32\2\2\33\3\3\5\4\7\5\t\6\13\7\r\b"+
		"\17\t\21\n\23\13\25\f\27\r\31\16\33\17\35\20\37\21!\22#\23%\24\'\25)\26"+
		"+\27-\30/\31\61\32\63\33\3\2\7\5\2\62;C\\c|\6\2/;C\\aac|\4\2$$))\5\2\13"+
		"\f\17\17\"\"\4\2\f\f\17\17\u00fd\2\3\3\2\2\2\2\5\3\2\2\2\2\7\3\2\2\2\2"+
		"\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2\2\2\2\23\3\2"+
		"\2\2\2\25\3\2\2\2\2\27\3\2\2\2\2\31\3\2\2\2\2\33\3\2\2\2\2\35\3\2\2\2"+
		"\2\37\3\2\2\2\2!\3\2\2\2\2#\3\2\2\2\2%\3\2\2\2\2\'\3\2\2\2\2)\3\2\2\2"+
		"\2+\3\2\2\2\2-\3\2\2\2\2/\3\2\2\2\2\61\3\2\2\2\2\63\3\2\2\2\3\65\3\2\2"+
		"\2\5\67\3\2\2\2\7A\3\2\2\2\tI\3\2\2\2\13Q\3\2\2\2\rW\3\2\2\2\17_\3\2\2"+
		"\2\21g\3\2\2\2\23u\3\2\2\2\25\u0085\3\2\2\2\27\u0093\3\2\2\2\31\u009d"+
		"\3\2\2\2\33\u00a9\3\2\2\2\35\u00af\3\2\2\2\37\u00b5\3\2\2\2!\u00bd\3\2"+
		"\2\2#\u00c5\3\2\2\2%\u00c8\3\2\2\2\'\u00cd\3\2\2\2)\u00d1\3\2\2\2+\u00d3"+
		"\3\2\2\2-\u00d5\3\2\2\2/\u00d7\3\2\2\2\61\u00da\3\2\2\2\63\u00e0\3\2\2"+
		"\2\65\66\7?\2\2\66\4\3\2\2\2\678\7.\2\28\6\3\2\2\29:\7n\2\2:;\7q\2\2;"+
		"<\7c\2\2<B\7f\2\2=>\7N\2\2>?\7Q\2\2?@\7C\2\2@B\7F\2\2A9\3\2\2\2A=\3\2"+
		"\2\2B\b\3\2\2\2CD\7t\2\2DE\7g\2\2EJ\7n\2\2FG\7T\2\2GH\7G\2\2HJ\7N\2\2"+
		"IC\3\2\2\2IF\3\2\2\2J\n\3\2\2\2KL\7e\2\2LM\7u\2\2MR\7x\2\2NO\7E\2\2OP"+
		"\7U\2\2PR\7X\2\2QK\3\2\2\2QN\3\2\2\2R\f\3\2\2\2ST\7c\2\2TX\7u\2\2UV\7"+
		"C\2\2VX\7U\2\2WS\3\2\2\2WU\3\2\2\2X\16\3\2\2\2YZ\7u\2\2Z[\7g\2\2[`\7v"+
		"\2\2\\]\7U\2\2]^\7G\2\2^`\7V\2\2_Y\3\2\2\2_\\\3\2\2\2`\20\3\2\2\2ab\7"+
		"f\2\2bc\7k\2\2ch\7t\2\2de\7F\2\2ef\7K\2\2fh\7T\2\2ga\3\2\2\2gd\3\2\2\2"+
		"h\22\3\2\2\2ij\7q\2\2jk\7w\2\2kl\7v\2\2lm\7r\2\2mn\7w\2\2nv\7v\2\2op\7"+
		"Q\2\2pq\7W\2\2qr\7V\2\2rs\7R\2\2st\7W\2\2tv\7V\2\2ui\3\2\2\2uo\3\2\2\2"+
		"v\24\3\2\2\2wx\7u\2\2xy\7e\2\2yz\7t\2\2z{\7c\2\2{|\7v\2\2|}\7e\2\2}\u0086"+
		"\7j\2\2~\177\7U\2\2\177\u0080\7E\2\2\u0080\u0081\7T\2\2\u0081\u0082\7"+
		"C\2\2\u0082\u0083\7V\2\2\u0083\u0084\7E\2\2\u0084\u0086\7J\2\2\u0085w"+
		"\3\2\2\2\u0085~\3\2\2\2\u0086\26\3\2\2\2\u0087\u0088\7u\2\2\u0088\u0089"+
		"\7g\2\2\u0089\u008a\7n\2\2\u008a\u008b\7g\2\2\u008b\u008c\7e\2\2\u008c"+
		"\u0094\7v\2\2\u008d\u008e\7U\2\2\u008e\u008f\7G\2\2\u008f\u0090\7N\2\2"+
		"\u0090\u0091\7G\2\2\u0091\u0092\7E\2\2\u0092\u0094\7V\2\2\u0093\u0087"+
		"\3\2\2\2\u0093\u008d\3\2\2\2\u0094\30\3\2\2\2\u0095\u0096\7h\2\2\u0096"+
		"\u0097\7t\2\2\u0097\u0098\7q\2\2\u0098\u009e\7o\2\2\u0099\u009a\7H\2\2"+
		"\u009a\u009b\7T\2\2\u009b\u009c\7Q\2\2\u009c\u009e\7O\2\2\u009d\u0095"+
		"\3\2\2\2\u009d\u0099\3\2\2\2\u009e\32\3\2\2\2\u009f\u00a0\7y\2\2\u00a0"+
		"\u00a1\7j\2\2\u00a1\u00a2\7g\2\2\u00a2\u00a3\7t\2\2\u00a3\u00aa\7g\2\2"+
		"\u00a4\u00a5\7Y\2\2\u00a5\u00a6\7J\2\2\u00a6\u00a7\7G\2\2\u00a7\u00a8"+
		"\7T\2\2\u00a8\u00aa\7G\2\2\u00a9\u009f\3\2\2\2\u00a9\u00a4\3\2\2\2\u00aa"+
		"\34\3\2\2\2\u00ab\u00ac\7k\2\2\u00ac\u00b0\7p\2\2\u00ad\u00ae\7K\2\2\u00ae"+
		"\u00b0\7P\2\2\u00af\u00ab\3\2\2\2\u00af\u00ad\3\2\2\2\u00b0\36\3\2\2\2"+
		"\u00b1\u00b2\7q\2\2\u00b2\u00b6\7t\2\2\u00b3\u00b4\7Q\2\2\u00b4\u00b6"+
		"\7T\2\2\u00b5\u00b1\3\2\2\2\u00b5\u00b3\3\2\2\2\u00b6 \3\2\2\2\u00b7\u00b8"+
		"\7c\2\2\u00b8\u00b9\7p\2\2\u00b9\u00be\7f\2\2\u00ba\u00bb\7C\2\2\u00bb"+
		"\u00bc\7P\2\2\u00bc\u00be\7F\2\2\u00bd\u00b7\3\2\2\2\u00bd\u00ba\3\2\2"+
		"\2\u00be\"\3\2\2\2\u00bf\u00c0\7p\2\2\u00c0\u00c1\7q\2\2\u00c1\u00c6\7"+
		"v\2\2\u00c2\u00c3\7P\2\2\u00c3\u00c4\7Q\2\2\u00c4\u00c6\7V\2\2\u00c5\u00bf"+
		"\3\2\2\2\u00c5\u00c2\3\2\2\2\u00c6$\3\2\2\2\u00c7\u00c9\t\2\2\2\u00c8"+
		"\u00c7\3\2\2\2\u00c9\u00ca\3\2\2\2\u00ca\u00c8\3\2\2\2\u00ca\u00cb\3\2"+
		"\2\2\u00cb&\3\2\2\2\u00cc\u00ce\t\3\2\2\u00cd\u00cc\3\2\2\2\u00ce\u00cf"+
		"\3\2\2\2\u00cf\u00cd\3\2\2\2\u00cf\u00d0\3\2\2\2\u00d0(\3\2\2\2\u00d1"+
		"\u00d2\t\4\2\2\u00d2*\3\2\2\2\u00d3\u00d4\7*\2\2\u00d4,\3\2\2\2\u00d5"+
		"\u00d6\7+\2\2\u00d6.\3\2\2\2\u00d7\u00d8\7=\2\2\u00d8\60\3\2\2\2\u00d9"+
		"\u00db\t\5\2\2\u00da\u00d9\3\2\2\2\u00db\u00dc\3\2\2\2\u00dc\u00da\3\2"+
		"\2\2\u00dc\u00dd\3\2\2\2\u00dd\u00de\3\2\2\2\u00de\u00df\b\31\2\2\u00df"+
		"\62\3\2\2\2\u00e0\u00e1\7\61\2\2\u00e1\u00e2\7\61\2\2\u00e2\u00e6\3\2"+
		"\2\2\u00e3\u00e5\n\6\2\2\u00e4\u00e3\3\2\2\2\u00e5\u00e8\3\2\2\2\u00e6"+
		"\u00e4\3\2\2\2\u00e6\u00e7\3\2\2\2\u00e7\u00e9\3\2\2\2\u00e8\u00e6\3\2"+
		"\2\2\u00e9\u00ea\b\32\2\2\u00ea\64\3\2\2\2\26\2AIQW_gu\u0085\u0093\u009d"+
		"\u00a9\u00af\u00b5\u00bd\u00c5\u00ca\u00cf\u00dc\u00e6\3\2\3\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}