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
		T__0=1, T__1=2, LOAD=3, REL=4, CSV=5, ARITY=6, SET=7, DIR=8, OUTPUT=9, 
		SCRATCH=10, SELECT=11, FROM=12, SATISFYING=13, WHERE=14, IN=15, OR=16, 
		AND=17, NOT=18, INT=19, ID=20, PLACEHOLDER=21, FILENAME=22, QUOTE=23, 
		LPAR=24, RPAR=25, SEMICOLON=26, WS=27, SINGLE_LINE_COMMENT=28;
	public static final int
		RULE_script = 0, RULE_input = 1, RULE_relname = 2, RULE_relarity = 3, 
		RULE_file = 4, RULE_schema = 5, RULE_selector = 6, RULE_guardschema = 7, 
		RULE_outputpath = 8, RULE_scratchpath = 9, RULE_select = 10, RULE_gfquery = 11, 
		RULE_expr = 12, RULE_guardedrel = 13, RULE_anystring = 14, RULE_keyword = 15;
	public static final String[] ruleNames = {
		"script", "input", "relname", "relarity", "file", "schema", "selector", 
		"guardschema", "outputpath", "scratchpath", "select", "gfquery", "expr", 
		"guardedrel", "anystring", "keyword"
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
			setState(38);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==SET || _la==ID) {
				{
				setState(36);
				switch ( getInterpreter().adaptivePredict(_input,0,_ctx) ) {
				case 1:
					{
					setState(32);
					outputpath();
					}
					break;
				case 2:
					{
					setState(33);
					scratchpath();
					}
					break;
				case 3:
					{
					setState(34);
					input();
					}
					break;
				case 4:
					{
					setState(35);
					select();
					}
					break;
				}
				}
				setState(40);
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
	public static class InputCsvContext extends InputContext {
		public RelnameContext relname() {
			return getRuleContext(RelnameContext.class,0);
		}
		public TerminalNode LOAD() { return getToken(GumboParser.LOAD, 0); }
		public TerminalNode CSV() { return getToken(GumboParser.CSV, 0); }
		public FileContext file() {
			return getRuleContext(FileContext.class,0);
		}
		public TerminalNode ARITY() { return getToken(GumboParser.ARITY, 0); }
		public RelarityContext relarity() {
			return getRuleContext(RelarityContext.class,0);
		}
		public TerminalNode SEMICOLON() { return getToken(GumboParser.SEMICOLON, 0); }
		public InputCsvContext(InputContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof GumboVisitor ) return ((GumboVisitor<? extends T>)visitor).visitInputCsv(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class InputRelContext extends InputContext {
		public RelnameContext relname() {
			return getRuleContext(RelnameContext.class,0);
		}
		public TerminalNode LOAD() { return getToken(GumboParser.LOAD, 0); }
		public FileContext file() {
			return getRuleContext(FileContext.class,0);
		}
		public TerminalNode ARITY() { return getToken(GumboParser.ARITY, 0); }
		public RelarityContext relarity() {
			return getRuleContext(RelarityContext.class,0);
		}
		public TerminalNode SEMICOLON() { return getToken(GumboParser.SEMICOLON, 0); }
		public TerminalNode REL() { return getToken(GumboParser.REL, 0); }
		public InputRelContext(InputContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof GumboVisitor ) return ((GumboVisitor<? extends T>)visitor).visitInputRel(this);
			else return visitor.visitChildren(this);
		}
	}

	public final InputContext input() throws RecognitionException {
		InputContext _localctx = new InputContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_input);
		int _la;
		try {
			setState(61);
			switch ( getInterpreter().adaptivePredict(_input,3,_ctx) ) {
			case 1:
				_localctx = new InputRelContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(41);
				relname();
				setState(42);
				match(T__0);
				setState(43);
				match(LOAD);
				setState(45);
				_la = _input.LA(1);
				if (_la==REL) {
					{
					setState(44);
					match(REL);
					}
				}

				setState(47);
				file();
				setState(48);
				match(ARITY);
				setState(49);
				relarity();
				setState(50);
				match(SEMICOLON);
				}
				break;
			case 2:
				_localctx = new InputCsvContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(52);
				relname();
				setState(53);
				match(T__0);
				setState(54);
				match(LOAD);
				setState(55);
				match(CSV);
				setState(56);
				file();
				setState(57);
				match(ARITY);
				setState(58);
				relarity();
				setState(59);
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
		enterRule(_localctx, 4, RULE_relname);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(63);
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
		enterRule(_localctx, 6, RULE_relarity);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(65);
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
		enterRule(_localctx, 8, RULE_file);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(67);
			match(QUOTE);
			setState(68);
			anystring();
			setState(69);
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
		enterRule(_localctx, 10, RULE_schema);
		int _la;
		try {
			setState(90);
			switch (_input.LA(1)) {
			case LPAR:
				enterOuterAlt(_localctx, 1);
				{
				setState(71);
				match(LPAR);
				setState(72);
				selector();
				setState(77);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(73);
					match(T__1);
					setState(74);
					selector();
					}
					}
					setState(79);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(80);
				match(RPAR);
				}
				break;
			case ID:
			case PLACEHOLDER:
				enterOuterAlt(_localctx, 2);
				{
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
		enterRule(_localctx, 12, RULE_selector);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(92);
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

	public static class GuardschemaContext extends ParserRuleContext {
		public TerminalNode LPAR() { return getToken(GumboParser.LPAR, 0); }
		public List<TerminalNode> ID() { return getTokens(GumboParser.ID); }
		public TerminalNode ID(int i) {
			return getToken(GumboParser.ID, i);
		}
		public TerminalNode RPAR() { return getToken(GumboParser.RPAR, 0); }
		public GuardschemaContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_guardschema; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof GumboVisitor ) return ((GumboVisitor<? extends T>)visitor).visitGuardschema(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GuardschemaContext guardschema() throws RecognitionException {
		GuardschemaContext _localctx = new GuardschemaContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_guardschema);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(94);
			match(LPAR);
			setState(95);
			match(ID);
			setState(100);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__1) {
				{
				{
				setState(96);
				match(T__1);
				setState(97);
				match(ID);
				}
				}
				setState(102);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(103);
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
		enterRule(_localctx, 16, RULE_outputpath);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(105);
			match(SET);
			setState(106);
			match(OUTPUT);
			setState(107);
			match(DIR);
			setState(108);
			file();
			setState(109);
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
		enterRule(_localctx, 18, RULE_scratchpath);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(111);
			match(SET);
			setState(112);
			match(SCRATCH);
			setState(113);
			match(DIR);
			setState(114);
			file();
			setState(115);
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
		enterRule(_localctx, 20, RULE_select);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(117);
			relname();
			setState(118);
			match(T__0);
			setState(119);
			gfquery();
			setState(120);
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
		public GuardschemaContext guardschema() {
			return getRuleContext(GuardschemaContext.class,0);
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
		enterRule(_localctx, 22, RULE_gfquery);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(122);
			match(SELECT);
			setState(123);
			schema();
			setState(124);
			match(FROM);
			setState(125);
			relname();
			setState(127);
			_la = _input.LA(1);
			if (_la==LPAR) {
				{
				setState(126);
				guardschema();
				}
			}

			setState(129);
			match(WHERE);
			setState(130);
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
		int _startState = 24;
		enterRecursionRule(_localctx, 24, RULE_expr, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(140);
			switch ( getInterpreter().adaptivePredict(_input,9,_ctx) ) {
			case 1:
				{
				_localctx = new NotExprContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(133);
				match(NOT);
				setState(134);
				expr(5);
				}
				break;
			case 2:
				{
				_localctx = new GuardedExprContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(135);
				guardedrel();
				}
				break;
			case 3:
				{
				_localctx = new ParExprContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(136);
				match(LPAR);
				setState(137);
				expr(0);
				setState(138);
				match(RPAR);
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(150);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,11,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(148);
					switch ( getInterpreter().adaptivePredict(_input,10,_ctx) ) {
					case 1:
						{
						_localctx = new AndExprContext(new ExprContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(142);
						if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
						setState(143);
						match(AND);
						setState(144);
						expr(5);
						}
						break;
					case 2:
						{
						_localctx = new OrExprContext(new ExprContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(145);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(146);
						match(OR);
						setState(147);
						expr(4);
						}
						break;
					}
					} 
				}
				setState(152);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,11,_ctx);
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
		enterRule(_localctx, 26, RULE_guardedrel);
		try {
			setState(163);
			switch ( getInterpreter().adaptivePredict(_input,12,_ctx) ) {
			case 1:
				_localctx = new RegularGuardedContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(153);
				schema();
				setState(154);
				match(IN);
				setState(155);
				relname();
				}
				break;
			case 2:
				_localctx = new NestedGuardedContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(157);
				schema();
				setState(158);
				match(IN);
				setState(159);
				match(LPAR);
				setState(160);
				gfquery();
				setState(161);
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
		enterRule(_localctx, 28, RULE_anystring);
		try {
			setState(168);
			switch (_input.LA(1)) {
			case LOAD:
			case REL:
			case CSV:
			case ARITY:
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
				setState(165);
				keyword();
				}
				break;
			case ID:
				enterOuterAlt(_localctx, 2);
				{
				setState(166);
				match(ID);
				}
				break;
			case FILENAME:
				enterOuterAlt(_localctx, 3);
				{
				setState(167);
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
		enterRule(_localctx, 30, RULE_keyword);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(170);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LOAD) | (1L << REL) | (1L << CSV) | (1L << ARITY) | (1L << SET) | (1L << DIR) | (1L << OUTPUT) | (1L << SCRATCH) | (1L << SELECT) | (1L << FROM) | (1L << WHERE) | (1L << IN) | (1L << OR) | (1L << AND) | (1L << NOT))) != 0)) ) {
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
		case 12:
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
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3\36\u00af\4\2\t\2"+
		"\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\3\2\3\2"+
		"\3\2\3\2\7\2\'\n\2\f\2\16\2*\13\2\3\3\3\3\3\3\3\3\5\3\60\n\3\3\3\3\3\3"+
		"\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\5\3@\n\3\3\4\3\4\3\5\3"+
		"\5\3\6\3\6\3\6\3\6\3\7\3\7\3\7\3\7\7\7N\n\7\f\7\16\7Q\13\7\3\7\3\7\3\7"+
		"\3\7\3\7\7\7X\n\7\f\7\16\7[\13\7\5\7]\n\7\3\b\3\b\3\t\3\t\3\t\3\t\7\t"+
		"e\n\t\f\t\16\th\13\t\3\t\3\t\3\n\3\n\3\n\3\n\3\n\3\n\3\13\3\13\3\13\3"+
		"\13\3\13\3\13\3\f\3\f\3\f\3\f\3\f\3\r\3\r\3\r\3\r\3\r\5\r\u0082\n\r\3"+
		"\r\3\r\3\r\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\5\16\u008f\n\16\3\16"+
		"\3\16\3\16\3\16\3\16\3\16\7\16\u0097\n\16\f\16\16\16\u009a\13\16\3\17"+
		"\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\5\17\u00a6\n\17\3\20\3\20"+
		"\3\20\5\20\u00ab\n\20\3\21\3\21\3\21\2\3\32\22\2\4\6\b\n\f\16\20\22\24"+
		"\26\30\32\34\36 \2\4\3\2\26\27\4\2\5\16\20\24\u00b0\2(\3\2\2\2\4?\3\2"+
		"\2\2\6A\3\2\2\2\bC\3\2\2\2\nE\3\2\2\2\f\\\3\2\2\2\16^\3\2\2\2\20`\3\2"+
		"\2\2\22k\3\2\2\2\24q\3\2\2\2\26w\3\2\2\2\30|\3\2\2\2\32\u008e\3\2\2\2"+
		"\34\u00a5\3\2\2\2\36\u00aa\3\2\2\2 \u00ac\3\2\2\2\"\'\5\22\n\2#\'\5\24"+
		"\13\2$\'\5\4\3\2%\'\5\26\f\2&\"\3\2\2\2&#\3\2\2\2&$\3\2\2\2&%\3\2\2\2"+
		"\'*\3\2\2\2(&\3\2\2\2()\3\2\2\2)\3\3\2\2\2*(\3\2\2\2+,\5\6\4\2,-\7\3\2"+
		"\2-/\7\5\2\2.\60\7\6\2\2/.\3\2\2\2/\60\3\2\2\2\60\61\3\2\2\2\61\62\5\n"+
		"\6\2\62\63\7\b\2\2\63\64\5\b\5\2\64\65\7\34\2\2\65@\3\2\2\2\66\67\5\6"+
		"\4\2\678\7\3\2\289\7\5\2\29:\7\7\2\2:;\5\n\6\2;<\7\b\2\2<=\5\b\5\2=>\7"+
		"\34\2\2>@\3\2\2\2?+\3\2\2\2?\66\3\2\2\2@\5\3\2\2\2AB\7\26\2\2B\7\3\2\2"+
		"\2CD\7\25\2\2D\t\3\2\2\2EF\7\31\2\2FG\5\36\20\2GH\7\31\2\2H\13\3\2\2\2"+
		"IJ\7\32\2\2JO\5\16\b\2KL\7\4\2\2LN\5\16\b\2MK\3\2\2\2NQ\3\2\2\2OM\3\2"+
		"\2\2OP\3\2\2\2PR\3\2\2\2QO\3\2\2\2RS\7\33\2\2S]\3\2\2\2TY\5\16\b\2UV\7"+
		"\4\2\2VX\5\16\b\2WU\3\2\2\2X[\3\2\2\2YW\3\2\2\2YZ\3\2\2\2Z]\3\2\2\2[Y"+
		"\3\2\2\2\\I\3\2\2\2\\T\3\2\2\2]\r\3\2\2\2^_\t\2\2\2_\17\3\2\2\2`a\7\32"+
		"\2\2af\7\26\2\2bc\7\4\2\2ce\7\26\2\2db\3\2\2\2eh\3\2\2\2fd\3\2\2\2fg\3"+
		"\2\2\2gi\3\2\2\2hf\3\2\2\2ij\7\33\2\2j\21\3\2\2\2kl\7\t\2\2lm\7\13\2\2"+
		"mn\7\n\2\2no\5\n\6\2op\7\34\2\2p\23\3\2\2\2qr\7\t\2\2rs\7\f\2\2st\7\n"+
		"\2\2tu\5\n\6\2uv\7\34\2\2v\25\3\2\2\2wx\5\6\4\2xy\7\3\2\2yz\5\30\r\2z"+
		"{\7\34\2\2{\27\3\2\2\2|}\7\r\2\2}~\5\f\7\2~\177\7\16\2\2\177\u0081\5\6"+
		"\4\2\u0080\u0082\5\20\t\2\u0081\u0080\3\2\2\2\u0081\u0082\3\2\2\2\u0082"+
		"\u0083\3\2\2\2\u0083\u0084\7\20\2\2\u0084\u0085\5\32\16\2\u0085\31\3\2"+
		"\2\2\u0086\u0087\b\16\1\2\u0087\u0088\7\24\2\2\u0088\u008f\5\32\16\7\u0089"+
		"\u008f\5\34\17\2\u008a\u008b\7\32\2\2\u008b\u008c\5\32\16\2\u008c\u008d"+
		"\7\33\2\2\u008d\u008f\3\2\2\2\u008e\u0086\3\2\2\2\u008e\u0089\3\2\2\2"+
		"\u008e\u008a\3\2\2\2\u008f\u0098\3\2\2\2\u0090\u0091\f\6\2\2\u0091\u0092"+
		"\7\23\2\2\u0092\u0097\5\32\16\7\u0093\u0094\f\5\2\2\u0094\u0095\7\22\2"+
		"\2\u0095\u0097\5\32\16\6\u0096\u0090\3\2\2\2\u0096\u0093\3\2\2\2\u0097"+
		"\u009a\3\2\2\2\u0098\u0096\3\2\2\2\u0098\u0099\3\2\2\2\u0099\33\3\2\2"+
		"\2\u009a\u0098\3\2\2\2\u009b\u009c\5\f\7\2\u009c\u009d\7\21\2\2\u009d"+
		"\u009e\5\6\4\2\u009e\u00a6\3\2\2\2\u009f\u00a0\5\f\7\2\u00a0\u00a1\7\21"+
		"\2\2\u00a1\u00a2\7\32\2\2\u00a2\u00a3\5\30\r\2\u00a3\u00a4\7\33\2\2\u00a4"+
		"\u00a6\3\2\2\2\u00a5\u009b\3\2\2\2\u00a5\u009f\3\2\2\2\u00a6\35\3\2\2"+
		"\2\u00a7\u00ab\5 \21\2\u00a8\u00ab\7\26\2\2\u00a9\u00ab\7\30\2\2\u00aa"+
		"\u00a7\3\2\2\2\u00aa\u00a8\3\2\2\2\u00aa\u00a9\3\2\2\2\u00ab\37\3\2\2"+
		"\2\u00ac\u00ad\t\3\2\2\u00ad!\3\2\2\2\20&(/?OY\\f\u0081\u008e\u0096\u0098"+
		"\u00a5\u00aa";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}