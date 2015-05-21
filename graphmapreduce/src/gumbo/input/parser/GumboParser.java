package gumbo.input.parser;

// Generated from Gumbo.g4 by ANTLR 4.5
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class GumboParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.5", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, LOAD=3, REL=4, CSV=5, ARITY=6, AS=7, SET=8, DIR=9, OUTPUT=10, 
		SCRATCH=11, SELECT=12, FROM=13, SATISFYING=14, WHERE=15, IN=16, OR=17, 
		AND=18, NOT=19, INT=20, ID=21, PLACEHOLDER=22, FILENAME=23, QUOTE=24, 
		LPAR=25, RPAR=26, SEMICOLON=27, WS=28, SINGLE_LINE_COMMENT=29;
	public static final int
		RULE_script = 0, RULE_input = 1, RULE_format = 2, RULE_relname = 3, RULE_relarity = 4, 
		RULE_file = 5, RULE_schema = 6, RULE_selector = 7, RULE_loadschema = 8, 
		RULE_outputpath = 9, RULE_scratchpath = 10, RULE_select = 11, RULE_gfquery = 12, 
		RULE_satclause = 13, RULE_assrt = 14, RULE_expr = 15, RULE_guardedrel = 16, 
		RULE_anystring = 17, RULE_keyword = 18;
	public static final String[] ruleNames = {
		"script", "input", "format", "relname", "relarity", "file", "schema", 
		"selector", "loadschema", "outputpath", "scratchpath", "select", "gfquery", 
		"satclause", "assrt", "expr", "guardedrel", "anystring", "keyword"
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

	@Override
	public String getGrammarFileName() { return "Gumbo.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public GumboParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}
	public static class ScriptContext extends ParserRuleContext {
		public List<OutputpathContext> outputpath() {
			return getRuleContexts(OutputpathContext.class);
		}
		public OutputpathContext outputpath(int i) {
			return getRuleContext(OutputpathContext.class,i);
		}
		public List<ScratchpathContext> scratchpath() {
			return getRuleContexts(ScratchpathContext.class);
		}
		public ScratchpathContext scratchpath(int i) {
			return getRuleContext(ScratchpathContext.class,i);
		}
		public List<InputContext> input() {
			return getRuleContexts(InputContext.class);
		}
		public InputContext input(int i) {
			return getRuleContext(InputContext.class,i);
		}
		public List<SelectContext> select() {
			return getRuleContexts(SelectContext.class);
		}
		public SelectContext select(int i) {
			return getRuleContext(SelectContext.class,i);
		}
		public ScriptContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_script; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof GumboVisitor ) return ((GumboVisitor<? extends T>)visitor).visitScript(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ScriptContext script() throws RecognitionException {
		ScriptContext _localctx = new ScriptContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_script);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(44);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==SET || _la==ID) {
				{
				setState(42);
				switch ( getInterpreter().adaptivePredict(_input,0,_ctx) ) {
				case 1:
					{
					setState(38);
					outputpath();
					}
					break;
				case 2:
					{
					setState(39);
					scratchpath();
					}
					break;
				case 3:
					{
					setState(40);
					input();
					}
					break;
				case 4:
					{
					setState(41);
					select();
					}
					break;
				}
				}
				setState(46);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class InputContext extends ParserRuleContext {
		public InputContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_input; }
	 
		public InputContext() { }
		public void copyFrom(InputContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class InputArityContext extends InputContext {
		public RelnameContext relname() {
			return getRuleContext(RelnameContext.class,0);
		}
		public TerminalNode LOAD() { return getToken(GumboParser.LOAD, 0); }
		public FormatContext format() {
			return getRuleContext(FormatContext.class,0);
		}
		public FileContext file() {
			return getRuleContext(FileContext.class,0);
		}
		public TerminalNode ARITY() { return getToken(GumboParser.ARITY, 0); }
		public RelarityContext relarity() {
			return getRuleContext(RelarityContext.class,0);
		}
		public TerminalNode SEMICOLON() { return getToken(GumboParser.SEMICOLON, 0); }
		public InputArityContext(InputContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof GumboVisitor ) return ((GumboVisitor<? extends T>)visitor).visitInputArity(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class InputSchemaContext extends InputContext {
		public RelnameContext relname() {
			return getRuleContext(RelnameContext.class,0);
		}
		public TerminalNode LOAD() { return getToken(GumboParser.LOAD, 0); }
		public FormatContext format() {
			return getRuleContext(FormatContext.class,0);
		}
		public FileContext file() {
			return getRuleContext(FileContext.class,0);
		}
		public TerminalNode AS() { return getToken(GumboParser.AS, 0); }
		public LoadschemaContext loadschema() {
			return getRuleContext(LoadschemaContext.class,0);
		}
		public TerminalNode SEMICOLON() { return getToken(GumboParser.SEMICOLON, 0); }
		public InputSchemaContext(InputContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof GumboVisitor ) return ((GumboVisitor<? extends T>)visitor).visitInputSchema(this);
			else return visitor.visitChildren(this);
		}
	}

	public final InputContext input() throws RecognitionException {
		InputContext _localctx = new InputContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_input);
		try {
			setState(65);
			switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
			case 1:
				_localctx = new InputArityContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(47);
				relname();
				setState(48);
				match(T__0);
				setState(49);
				match(LOAD);
				setState(50);
				format();
				setState(51);
				file();
				setState(52);
				match(ARITY);
				setState(53);
				relarity();
				setState(54);
				match(SEMICOLON);
				}
				break;
			case 2:
				_localctx = new InputSchemaContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(56);
				relname();
				setState(57);
				match(T__0);
				setState(58);
				match(LOAD);
				setState(59);
				format();
				setState(60);
				file();
				setState(61);
				match(AS);
				setState(62);
				loadschema();
				setState(63);
				match(SEMICOLON);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FormatContext extends ParserRuleContext {
		public TerminalNode REL() { return getToken(GumboParser.REL, 0); }
		public TerminalNode CSV() { return getToken(GumboParser.CSV, 0); }
		public FormatContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_format; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof GumboVisitor ) return ((GumboVisitor<? extends T>)visitor).visitFormat(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FormatContext format() throws RecognitionException {
		FormatContext _localctx = new FormatContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_format);
		int _la;
		try {
			setState(71);
			switch (_input.LA(1)) {
			case REL:
			case QUOTE:
				enterOuterAlt(_localctx, 1);
				{
				setState(68);
				_la = _input.LA(1);
				if (_la==REL) {
					{
					setState(67);
					match(REL);
					}
				}

				}
				break;
			case CSV:
				enterOuterAlt(_localctx, 2);
				{
				setState(70);
				match(CSV);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RelnameContext extends ParserRuleContext {
		public TerminalNode ID() { return getToken(GumboParser.ID, 0); }
		public RelnameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relname; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof GumboVisitor ) return ((GumboVisitor<? extends T>)visitor).visitRelname(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RelnameContext relname() throws RecognitionException {
		RelnameContext _localctx = new RelnameContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_relname);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(73);
			match(ID);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RelarityContext extends ParserRuleContext {
		public TerminalNode INT() { return getToken(GumboParser.INT, 0); }
		public RelarityContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relarity; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof GumboVisitor ) return ((GumboVisitor<? extends T>)visitor).visitRelarity(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RelarityContext relarity() throws RecognitionException {
		RelarityContext _localctx = new RelarityContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_relarity);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(75);
			match(INT);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FileContext extends ParserRuleContext {
		public List<TerminalNode> QUOTE() { return getTokens(GumboParser.QUOTE); }
		public TerminalNode QUOTE(int i) {
			return getToken(GumboParser.QUOTE, i);
		}
		public AnystringContext anystring() {
			return getRuleContext(AnystringContext.class,0);
		}
		public FileContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_file; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof GumboVisitor ) return ((GumboVisitor<? extends T>)visitor).visitFile(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FileContext file() throws RecognitionException {
		FileContext _localctx = new FileContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_file);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(77);
			match(QUOTE);
			setState(78);
			anystring();
			setState(79);
			match(QUOTE);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SchemaContext extends ParserRuleContext {
		public TerminalNode LPAR() { return getToken(GumboParser.LPAR, 0); }
		public List<SelectorContext> selector() {
			return getRuleContexts(SelectorContext.class);
		}
		public SelectorContext selector(int i) {
			return getRuleContext(SelectorContext.class,i);
		}
		public TerminalNode RPAR() { return getToken(GumboParser.RPAR, 0); }
		public SchemaContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_schema; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof GumboVisitor ) return ((GumboVisitor<? extends T>)visitor).visitSchema(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SchemaContext schema() throws RecognitionException {
		SchemaContext _localctx = new SchemaContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_schema);
		int _la;
		try {
			setState(100);
			switch (_input.LA(1)) {
			case LPAR:
				enterOuterAlt(_localctx, 1);
				{
				setState(81);
				match(LPAR);
				setState(82);
				selector();
				setState(87);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(83);
					match(T__1);
					setState(84);
					selector();
					}
					}
					setState(89);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(90);
				match(RPAR);
				}
				break;
			case ID:
			case PLACEHOLDER:
				enterOuterAlt(_localctx, 2);
				{
				setState(92);
				selector();
				setState(97);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(93);
					match(T__1);
					setState(94);
					selector();
					}
					}
					setState(99);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SelectorContext extends ParserRuleContext {
		public TerminalNode ID() { return getToken(GumboParser.ID, 0); }
		public TerminalNode PLACEHOLDER() { return getToken(GumboParser.PLACEHOLDER, 0); }
		public SelectorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_selector; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof GumboVisitor ) return ((GumboVisitor<? extends T>)visitor).visitSelector(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SelectorContext selector() throws RecognitionException {
		SelectorContext _localctx = new SelectorContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_selector);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(102);
			_la = _input.LA(1);
			if ( !(_la==ID || _la==PLACEHOLDER) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LoadschemaContext extends ParserRuleContext {
		public TerminalNode LPAR() { return getToken(GumboParser.LPAR, 0); }
		public List<TerminalNode> ID() { return getTokens(GumboParser.ID); }
		public TerminalNode ID(int i) {
			return getToken(GumboParser.ID, i);
		}
		public TerminalNode RPAR() { return getToken(GumboParser.RPAR, 0); }
		public LoadschemaContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_loadschema; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof GumboVisitor ) return ((GumboVisitor<? extends T>)visitor).visitLoadschema(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LoadschemaContext loadschema() throws RecognitionException {
		LoadschemaContext _localctx = new LoadschemaContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_loadschema);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(104);
			match(LPAR);
			setState(105);
			match(ID);
			setState(110);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__1) {
				{
				{
				setState(106);
				match(T__1);
				setState(107);
				match(ID);
				}
				}
				setState(112);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(113);
			match(RPAR);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OutputpathContext extends ParserRuleContext {
		public TerminalNode SET() { return getToken(GumboParser.SET, 0); }
		public TerminalNode OUTPUT() { return getToken(GumboParser.OUTPUT, 0); }
		public TerminalNode DIR() { return getToken(GumboParser.DIR, 0); }
		public FileContext file() {
			return getRuleContext(FileContext.class,0);
		}
		public TerminalNode SEMICOLON() { return getToken(GumboParser.SEMICOLON, 0); }
		public OutputpathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_outputpath; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof GumboVisitor ) return ((GumboVisitor<? extends T>)visitor).visitOutputpath(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OutputpathContext outputpath() throws RecognitionException {
		OutputpathContext _localctx = new OutputpathContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_outputpath);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(115);
			match(SET);
			setState(116);
			match(OUTPUT);
			setState(117);
			match(DIR);
			setState(118);
			file();
			setState(119);
			match(SEMICOLON);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ScratchpathContext extends ParserRuleContext {
		public TerminalNode SET() { return getToken(GumboParser.SET, 0); }
		public TerminalNode SCRATCH() { return getToken(GumboParser.SCRATCH, 0); }
		public TerminalNode DIR() { return getToken(GumboParser.DIR, 0); }
		public FileContext file() {
			return getRuleContext(FileContext.class,0);
		}
		public TerminalNode SEMICOLON() { return getToken(GumboParser.SEMICOLON, 0); }
		public ScratchpathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_scratchpath; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof GumboVisitor ) return ((GumboVisitor<? extends T>)visitor).visitScratchpath(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ScratchpathContext scratchpath() throws RecognitionException {
		ScratchpathContext _localctx = new ScratchpathContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_scratchpath);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(121);
			match(SET);
			setState(122);
			match(SCRATCH);
			setState(123);
			match(DIR);
			setState(124);
			file();
			setState(125);
			match(SEMICOLON);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SelectContext extends ParserRuleContext {
		public RelnameContext relname() {
			return getRuleContext(RelnameContext.class,0);
		}
		public GfqueryContext gfquery() {
			return getRuleContext(GfqueryContext.class,0);
		}
		public TerminalNode SEMICOLON() { return getToken(GumboParser.SEMICOLON, 0); }
		public SelectContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_select; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof GumboVisitor ) return ((GumboVisitor<? extends T>)visitor).visitSelect(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SelectContext select() throws RecognitionException {
		SelectContext _localctx = new SelectContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_select);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(127);
			relname();
			setState(128);
			match(T__0);
			setState(129);
			gfquery();
			setState(130);
			match(SEMICOLON);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class GfqueryContext extends ParserRuleContext {
		public TerminalNode SELECT() { return getToken(GumboParser.SELECT, 0); }
		public SchemaContext schema() {
			return getRuleContext(SchemaContext.class,0);
		}
		public TerminalNode FROM() { return getToken(GumboParser.FROM, 0); }
		public RelnameContext relname() {
			return getRuleContext(RelnameContext.class,0);
		}
		public TerminalNode WHERE() { return getToken(GumboParser.WHERE, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public SatclauseContext satclause() {
			return getRuleContext(SatclauseContext.class,0);
		}
		public GfqueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_gfquery; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof GumboVisitor ) return ((GumboVisitor<? extends T>)visitor).visitGfquery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GfqueryContext gfquery() throws RecognitionException {
		GfqueryContext _localctx = new GfqueryContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_gfquery);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(132);
			match(SELECT);
			setState(133);
			schema();
			setState(134);
			match(FROM);
			setState(135);
			relname();
			setState(137);
			_la = _input.LA(1);
			if (_la==SATISFYING) {
				{
				setState(136);
				satclause();
				}
			}

			setState(139);
			match(WHERE);
			setState(140);
			expr(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SatclauseContext extends ParserRuleContext {
		public TerminalNode SATISFYING() { return getToken(GumboParser.SATISFYING, 0); }
		public List<AssrtContext> assrt() {
			return getRuleContexts(AssrtContext.class);
		}
		public AssrtContext assrt(int i) {
			return getRuleContext(AssrtContext.class,i);
		}
		public List<TerminalNode> AND() { return getTokens(GumboParser.AND); }
		public TerminalNode AND(int i) {
			return getToken(GumboParser.AND, i);
		}
		public SatclauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_satclause; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof GumboVisitor ) return ((GumboVisitor<? extends T>)visitor).visitSatclause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SatclauseContext satclause() throws RecognitionException {
		SatclauseContext _localctx = new SatclauseContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_satclause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(142);
			match(SATISFYING);
			setState(143);
			assrt();
			setState(148);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==AND) {
				{
				{
				setState(144);
				match(AND);
				setState(145);
				assrt();
				}
				}
				setState(150);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AssrtContext extends ParserRuleContext {
		public TerminalNode ID() { return getToken(GumboParser.ID, 0); }
		public List<TerminalNode> QUOTE() { return getTokens(GumboParser.QUOTE); }
		public TerminalNode QUOTE(int i) {
			return getToken(GumboParser.QUOTE, i);
		}
		public AnystringContext anystring() {
			return getRuleContext(AnystringContext.class,0);
		}
		public AssrtContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_assrt; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof GumboVisitor ) return ((GumboVisitor<? extends T>)visitor).visitAssrt(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AssrtContext assrt() throws RecognitionException {
		AssrtContext _localctx = new AssrtContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_assrt);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(151);
			match(ID);
			setState(152);
			match(T__0);
			setState(153);
			match(QUOTE);
			setState(154);
			anystring();
			setState(155);
			match(QUOTE);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExprContext extends ParserRuleContext {
		public ExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expr; }
	 
		public ExprContext() { }
		public void copyFrom(ExprContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class AndExprContext extends ExprContext {
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public TerminalNode AND() { return getToken(GumboParser.AND, 0); }
		public AndExprContext(ExprContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof GumboVisitor ) return ((GumboVisitor<? extends T>)visitor).visitAndExpr(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class GuardedExprContext extends ExprContext {
		public GuardedrelContext guardedrel() {
			return getRuleContext(GuardedrelContext.class,0);
		}
		public GuardedExprContext(ExprContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof GumboVisitor ) return ((GumboVisitor<? extends T>)visitor).visitGuardedExpr(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ParExprContext extends ExprContext {
		public TerminalNode LPAR() { return getToken(GumboParser.LPAR, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TerminalNode RPAR() { return getToken(GumboParser.RPAR, 0); }
		public ParExprContext(ExprContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof GumboVisitor ) return ((GumboVisitor<? extends T>)visitor).visitParExpr(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class NotExprContext extends ExprContext {
		public TerminalNode NOT() { return getToken(GumboParser.NOT, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public NotExprContext(ExprContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof GumboVisitor ) return ((GumboVisitor<? extends T>)visitor).visitNotExpr(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class OrExprContext extends ExprContext {
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public TerminalNode OR() { return getToken(GumboParser.OR, 0); }
		public OrExprContext(ExprContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof GumboVisitor ) return ((GumboVisitor<? extends T>)visitor).visitOrExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExprContext expr() throws RecognitionException {
		return expr(0);
	}

	private ExprContext expr(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		ExprContext _localctx = new ExprContext(_ctx, _parentState);
		ExprContext _prevctx = _localctx;
		int _startState = 30;
		enterRecursionRule(_localctx, 30, RULE_expr, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(165);
			switch ( getInterpreter().adaptivePredict(_input,11,_ctx) ) {
			case 1:
				{
				_localctx = new NotExprContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(158);
				match(NOT);
				setState(159);
				expr(5);
				}
				break;
			case 2:
				{
				_localctx = new GuardedExprContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(160);
				guardedrel();
				}
				break;
			case 3:
				{
				_localctx = new ParExprContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(161);
				match(LPAR);
				setState(162);
				expr(0);
				setState(163);
				match(RPAR);
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(175);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,13,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(173);
					switch ( getInterpreter().adaptivePredict(_input,12,_ctx) ) {
					case 1:
						{
						_localctx = new AndExprContext(new ExprContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(167);
						if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
						setState(168);
						match(AND);
						setState(169);
						expr(5);
						}
						break;
					case 2:
						{
						_localctx = new OrExprContext(new ExprContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(170);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(171);
						match(OR);
						setState(172);
						expr(4);
						}
						break;
					}
					} 
				}
				setState(177);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,13,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class GuardedrelContext extends ParserRuleContext {
		public GuardedrelContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_guardedrel; }
	 
		public GuardedrelContext() { }
		public void copyFrom(GuardedrelContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class RegularGuardedContext extends GuardedrelContext {
		public SchemaContext schema() {
			return getRuleContext(SchemaContext.class,0);
		}
		public TerminalNode IN() { return getToken(GumboParser.IN, 0); }
		public RelnameContext relname() {
			return getRuleContext(RelnameContext.class,0);
		}
		public RegularGuardedContext(GuardedrelContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof GumboVisitor ) return ((GumboVisitor<? extends T>)visitor).visitRegularGuarded(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class NestedGuardedContext extends GuardedrelContext {
		public SchemaContext schema() {
			return getRuleContext(SchemaContext.class,0);
		}
		public TerminalNode IN() { return getToken(GumboParser.IN, 0); }
		public TerminalNode LPAR() { return getToken(GumboParser.LPAR, 0); }
		public GfqueryContext gfquery() {
			return getRuleContext(GfqueryContext.class,0);
		}
		public TerminalNode RPAR() { return getToken(GumboParser.RPAR, 0); }
		public NestedGuardedContext(GuardedrelContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof GumboVisitor ) return ((GumboVisitor<? extends T>)visitor).visitNestedGuarded(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GuardedrelContext guardedrel() throws RecognitionException {
		GuardedrelContext _localctx = new GuardedrelContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_guardedrel);
		try {
			setState(188);
			switch ( getInterpreter().adaptivePredict(_input,14,_ctx) ) {
			case 1:
				_localctx = new RegularGuardedContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(178);
				schema();
				setState(179);
				match(IN);
				setState(180);
				relname();
				}
				break;
			case 2:
				_localctx = new NestedGuardedContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(182);
				schema();
				setState(183);
				match(IN);
				setState(184);
				match(LPAR);
				setState(185);
				gfquery();
				setState(186);
				match(RPAR);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AnystringContext extends ParserRuleContext {
		public KeywordContext keyword() {
			return getRuleContext(KeywordContext.class,0);
		}
		public TerminalNode ID() { return getToken(GumboParser.ID, 0); }
		public TerminalNode INT() { return getToken(GumboParser.INT, 0); }
		public TerminalNode FILENAME() { return getToken(GumboParser.FILENAME, 0); }
		public AnystringContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_anystring; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof GumboVisitor ) return ((GumboVisitor<? extends T>)visitor).visitAnystring(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AnystringContext anystring() throws RecognitionException {
		AnystringContext _localctx = new AnystringContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_anystring);
		try {
			setState(194);
			switch (_input.LA(1)) {
			case LOAD:
			case REL:
			case CSV:
			case ARITY:
			case AS:
			case SET:
			case DIR:
			case OUTPUT:
			case SCRATCH:
			case SELECT:
			case FROM:
			case WHERE:
			case IN:
			case OR:
			case AND:
			case NOT:
				enterOuterAlt(_localctx, 1);
				{
				setState(190);
				keyword();
				}
				break;
			case ID:
				enterOuterAlt(_localctx, 2);
				{
				setState(191);
				match(ID);
				}
				break;
			case INT:
				enterOuterAlt(_localctx, 3);
				{
				setState(192);
				match(INT);
				}
				break;
			case FILENAME:
				enterOuterAlt(_localctx, 4);
				{
				setState(193);
				match(FILENAME);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class KeywordContext extends ParserRuleContext {
		public TerminalNode LOAD() { return getToken(GumboParser.LOAD, 0); }
		public TerminalNode REL() { return getToken(GumboParser.REL, 0); }
		public TerminalNode CSV() { return getToken(GumboParser.CSV, 0); }
		public TerminalNode ARITY() { return getToken(GumboParser.ARITY, 0); }
		public TerminalNode AS() { return getToken(GumboParser.AS, 0); }
		public TerminalNode SET() { return getToken(GumboParser.SET, 0); }
		public TerminalNode DIR() { return getToken(GumboParser.DIR, 0); }
		public TerminalNode OUTPUT() { return getToken(GumboParser.OUTPUT, 0); }
		public TerminalNode SCRATCH() { return getToken(GumboParser.SCRATCH, 0); }
		public TerminalNode SELECT() { return getToken(GumboParser.SELECT, 0); }
		public TerminalNode FROM() { return getToken(GumboParser.FROM, 0); }
		public TerminalNode WHERE() { return getToken(GumboParser.WHERE, 0); }
		public TerminalNode IN() { return getToken(GumboParser.IN, 0); }
		public TerminalNode OR() { return getToken(GumboParser.OR, 0); }
		public TerminalNode AND() { return getToken(GumboParser.AND, 0); }
		public TerminalNode NOT() { return getToken(GumboParser.NOT, 0); }
		public KeywordContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_keyword; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof GumboVisitor ) return ((GumboVisitor<? extends T>)visitor).visitKeyword(this);
			else return visitor.visitChildren(this);
		}
	}

	public final KeywordContext keyword() throws RecognitionException {
		KeywordContext _localctx = new KeywordContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_keyword);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(196);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LOAD) | (1L << REL) | (1L << CSV) | (1L << ARITY) | (1L << AS) | (1L << SET) | (1L << DIR) | (1L << OUTPUT) | (1L << SCRATCH) | (1L << SELECT) | (1L << FROM) | (1L << WHERE) | (1L << IN) | (1L << OR) | (1L << AND) | (1L << NOT))) != 0)) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
		case 15:
			return expr_sempred((ExprContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean expr_sempred(ExprContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return precpred(_ctx, 4);
		case 1:
			return precpred(_ctx, 3);
		}
		return true;
	}

	public static final String _serializedATN =
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3\37\u00c9\4\2\t\2"+
		"\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\3\2\3\2\3\2\3\2\7\2-\n\2\f\2\16\2\60\13\2\3\3\3\3"+
		"\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\5\3D"+
		"\n\3\3\4\5\4G\n\4\3\4\5\4J\n\4\3\5\3\5\3\6\3\6\3\7\3\7\3\7\3\7\3\b\3\b"+
		"\3\b\3\b\7\bX\n\b\f\b\16\b[\13\b\3\b\3\b\3\b\3\b\3\b\7\bb\n\b\f\b\16\b"+
		"e\13\b\5\bg\n\b\3\t\3\t\3\n\3\n\3\n\3\n\7\no\n\n\f\n\16\nr\13\n\3\n\3"+
		"\n\3\13\3\13\3\13\3\13\3\13\3\13\3\f\3\f\3\f\3\f\3\f\3\f\3\r\3\r\3\r\3"+
		"\r\3\r\3\16\3\16\3\16\3\16\3\16\5\16\u008c\n\16\3\16\3\16\3\16\3\17\3"+
		"\17\3\17\3\17\7\17\u0095\n\17\f\17\16\17\u0098\13\17\3\20\3\20\3\20\3"+
		"\20\3\20\3\20\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\5\21\u00a8\n\21"+
		"\3\21\3\21\3\21\3\21\3\21\3\21\7\21\u00b0\n\21\f\21\16\21\u00b3\13\21"+
		"\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\5\22\u00bf\n\22\3\23"+
		"\3\23\3\23\3\23\5\23\u00c5\n\23\3\24\3\24\3\24\2\3 \25\2\4\6\b\n\f\16"+
		"\20\22\24\26\30\32\34\36 \"$&\2\4\3\2\27\30\4\2\5\17\21\25\u00ca\2.\3"+
		"\2\2\2\4C\3\2\2\2\6I\3\2\2\2\bK\3\2\2\2\nM\3\2\2\2\fO\3\2\2\2\16f\3\2"+
		"\2\2\20h\3\2\2\2\22j\3\2\2\2\24u\3\2\2\2\26{\3\2\2\2\30\u0081\3\2\2\2"+
		"\32\u0086\3\2\2\2\34\u0090\3\2\2\2\36\u0099\3\2\2\2 \u00a7\3\2\2\2\"\u00be"+
		"\3\2\2\2$\u00c4\3\2\2\2&\u00c6\3\2\2\2(-\5\24\13\2)-\5\26\f\2*-\5\4\3"+
		"\2+-\5\30\r\2,(\3\2\2\2,)\3\2\2\2,*\3\2\2\2,+\3\2\2\2-\60\3\2\2\2.,\3"+
		"\2\2\2./\3\2\2\2/\3\3\2\2\2\60.\3\2\2\2\61\62\5\b\5\2\62\63\7\3\2\2\63"+
		"\64\7\5\2\2\64\65\5\6\4\2\65\66\5\f\7\2\66\67\7\b\2\2\678\5\n\6\289\7"+
		"\35\2\29D\3\2\2\2:;\5\b\5\2;<\7\3\2\2<=\7\5\2\2=>\5\6\4\2>?\5\f\7\2?@"+
		"\7\t\2\2@A\5\22\n\2AB\7\35\2\2BD\3\2\2\2C\61\3\2\2\2C:\3\2\2\2D\5\3\2"+
		"\2\2EG\7\6\2\2FE\3\2\2\2FG\3\2\2\2GJ\3\2\2\2HJ\7\7\2\2IF\3\2\2\2IH\3\2"+
		"\2\2J\7\3\2\2\2KL\7\27\2\2L\t\3\2\2\2MN\7\26\2\2N\13\3\2\2\2OP\7\32\2"+
		"\2PQ\5$\23\2QR\7\32\2\2R\r\3\2\2\2ST\7\33\2\2TY\5\20\t\2UV\7\4\2\2VX\5"+
		"\20\t\2WU\3\2\2\2X[\3\2\2\2YW\3\2\2\2YZ\3\2\2\2Z\\\3\2\2\2[Y\3\2\2\2\\"+
		"]\7\34\2\2]g\3\2\2\2^c\5\20\t\2_`\7\4\2\2`b\5\20\t\2a_\3\2\2\2be\3\2\2"+
		"\2ca\3\2\2\2cd\3\2\2\2dg\3\2\2\2ec\3\2\2\2fS\3\2\2\2f^\3\2\2\2g\17\3\2"+
		"\2\2hi\t\2\2\2i\21\3\2\2\2jk\7\33\2\2kp\7\27\2\2lm\7\4\2\2mo\7\27\2\2"+
		"nl\3\2\2\2or\3\2\2\2pn\3\2\2\2pq\3\2\2\2qs\3\2\2\2rp\3\2\2\2st\7\34\2"+
		"\2t\23\3\2\2\2uv\7\n\2\2vw\7\f\2\2wx\7\13\2\2xy\5\f\7\2yz\7\35\2\2z\25"+
		"\3\2\2\2{|\7\n\2\2|}\7\r\2\2}~\7\13\2\2~\177\5\f\7\2\177\u0080\7\35\2"+
		"\2\u0080\27\3\2\2\2\u0081\u0082\5\b\5\2\u0082\u0083\7\3\2\2\u0083\u0084"+
		"\5\32\16\2\u0084\u0085\7\35\2\2\u0085\31\3\2\2\2\u0086\u0087\7\16\2\2"+
		"\u0087\u0088\5\16\b\2\u0088\u0089\7\17\2\2\u0089\u008b\5\b\5\2\u008a\u008c"+
		"\5\34\17\2\u008b\u008a\3\2\2\2\u008b\u008c\3\2\2\2\u008c\u008d\3\2\2\2"+
		"\u008d\u008e\7\21\2\2\u008e\u008f\5 \21\2\u008f\33\3\2\2\2\u0090\u0091"+
		"\7\20\2\2\u0091\u0096\5\36\20\2\u0092\u0093\7\24\2\2\u0093\u0095\5\36"+
		"\20\2\u0094\u0092\3\2\2\2\u0095\u0098\3\2\2\2\u0096\u0094\3\2\2\2\u0096"+
		"\u0097\3\2\2\2\u0097\35\3\2\2\2\u0098\u0096\3\2\2\2\u0099\u009a\7\27\2"+
		"\2\u009a\u009b\7\3\2\2\u009b\u009c\7\32\2\2\u009c\u009d\5$\23\2\u009d"+
		"\u009e\7\32\2\2\u009e\37\3\2\2\2\u009f\u00a0\b\21\1\2\u00a0\u00a1\7\25"+
		"\2\2\u00a1\u00a8\5 \21\7\u00a2\u00a8\5\"\22\2\u00a3\u00a4\7\33\2\2\u00a4"+
		"\u00a5\5 \21\2\u00a5\u00a6\7\34\2\2\u00a6\u00a8\3\2\2\2\u00a7\u009f\3"+
		"\2\2\2\u00a7\u00a2\3\2\2\2\u00a7\u00a3\3\2\2\2\u00a8\u00b1\3\2\2\2\u00a9"+
		"\u00aa\f\6\2\2\u00aa\u00ab\7\24\2\2\u00ab\u00b0\5 \21\7\u00ac\u00ad\f"+
		"\5\2\2\u00ad\u00ae\7\23\2\2\u00ae\u00b0\5 \21\6\u00af\u00a9\3\2\2\2\u00af"+
		"\u00ac\3\2\2\2\u00b0\u00b3\3\2\2\2\u00b1\u00af\3\2\2\2\u00b1\u00b2\3\2"+
		"\2\2\u00b2!\3\2\2\2\u00b3\u00b1\3\2\2\2\u00b4\u00b5\5\16\b\2\u00b5\u00b6"+
		"\7\22\2\2\u00b6\u00b7\5\b\5\2\u00b7\u00bf\3\2\2\2\u00b8\u00b9\5\16\b\2"+
		"\u00b9\u00ba\7\22\2\2\u00ba\u00bb\7\33\2\2\u00bb\u00bc\5\32\16\2\u00bc"+
		"\u00bd\7\34\2\2\u00bd\u00bf\3\2\2\2\u00be\u00b4\3\2\2\2\u00be\u00b8\3"+
		"\2\2\2\u00bf#\3\2\2\2\u00c0\u00c5\5&\24\2\u00c1\u00c5\7\27\2\2\u00c2\u00c5"+
		"\7\26\2\2\u00c3\u00c5\7\31\2\2\u00c4\u00c0\3\2\2\2\u00c4\u00c1\3\2\2\2"+
		"\u00c4\u00c2\3\2\2\2\u00c4\u00c3\3\2\2\2\u00c5%\3\2\2\2\u00c6\u00c7\t"+
		"\3\2\2\u00c7\'\3\2\2\2\22,.CFIYcfp\u008b\u0096\u00a7\u00af\u00b1\u00be"+
		"\u00c4";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}