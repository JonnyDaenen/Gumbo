package gumbo.input;

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
		T__0=1, T__1=2, LOAD=3, REL=4, CSV=5, AS=6, SET=7, DIR=8, OUTPUT=9, SCRATCH=10, 
		SELECT=11, FROM=12, WHERE=13, IN=14, OR=15, AND=16, NOT=17, ID=18, FILENAME=19, 
		QUOTE=20, LPAR=21, RPAR=22, SEMICOLON=23, WS=24, SINGLE_LINE_COMMENT=25;
	public static final int
		RULE_script = 0, RULE_input = 1, RULE_relname = 2, RULE_file = 3, RULE_schema = 4, 
		RULE_outputpath = 5, RULE_scratchpath = 6, RULE_select = 7, RULE_gfquery = 8, 
		RULE_expr = 9, RULE_guardedrel = 10, RULE_anystring = 11, RULE_keyword = 12;
	public static final String[] ruleNames = {
		"script", "input", "relname", "file", "schema", "outputpath", "scratchpath", 
		"select", "gfquery", "expr", "guardedrel", "anystring", "keyword"
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
			setState(32);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==SET || _la==ID) {
				{
				setState(30);
				switch ( getInterpreter().adaptivePredict(_input,0,_ctx) ) {
				case 1:
					{
					setState(26);
					outputpath();
					}
					break;
				case 2:
					{
					setState(27);
					scratchpath();
					}
					break;
				case 3:
					{
					setState(28);
					input();
					}
					break;
				case 4:
					{
					setState(29);
					select();
					}
					break;
				}
				}
				setState(34);
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
		public TerminalNode AS() { return getToken(GumboParser.AS, 0); }
		public SchemaContext schema() {
			return getRuleContext(SchemaContext.class,0);
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
		public TerminalNode AS() { return getToken(GumboParser.AS, 0); }
		public SchemaContext schema() {
			return getRuleContext(SchemaContext.class,0);
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
			setState(55);
			switch ( getInterpreter().adaptivePredict(_input,3,_ctx) ) {
			case 1:
				_localctx = new InputRelContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(35);
				relname();
				setState(36);
				match(T__0);
				setState(37);
				match(LOAD);
				setState(39);
				_la = _input.LA(1);
				if (_la==REL) {
					{
					setState(38);
					match(REL);
					}
				}

				setState(41);
				file();
				setState(42);
				match(AS);
				setState(43);
				schema();
				setState(44);
				match(SEMICOLON);
				}
				break;
			case 2:
				_localctx = new InputCsvContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(46);
				relname();
				setState(47);
				match(T__0);
				setState(48);
				match(LOAD);
				setState(49);
				match(CSV);
				setState(50);
				file();
				setState(51);
				match(AS);
				setState(52);
				schema();
				setState(53);
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
			setState(57);
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
		enterRule(_localctx, 6, RULE_file);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(59);
			match(QUOTE);
			setState(60);
			anystring();
			setState(61);
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
		public List<TerminalNode> ID() { return getTokens(GumboParser.ID); }
		public TerminalNode ID(int i) {
			return getToken(GumboParser.ID, i);
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
		enterRule(_localctx, 8, RULE_schema);
		int _la;
		try {
			setState(81);
			switch (_input.LA(1)) {
			case LPAR:
				enterOuterAlt(_localctx, 1);
				{
				setState(63);
				match(LPAR);
				setState(64);
				match(ID);
				setState(69);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(65);
					match(T__1);
					setState(66);
					match(ID);
					}
					}
					setState(71);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(72);
				match(RPAR);
				}
				break;
			case ID:
				enterOuterAlt(_localctx, 2);
				{
				setState(73);
				match(ID);
				setState(78);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(74);
					match(T__1);
					setState(75);
					match(ID);
					}
					}
					setState(80);
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
		enterRule(_localctx, 10, RULE_outputpath);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(83);
			match(SET);
			setState(84);
			match(OUTPUT);
			setState(85);
			match(DIR);
			setState(86);
			file();
			setState(87);
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
		enterRule(_localctx, 12, RULE_scratchpath);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(89);
			match(SET);
			setState(90);
			match(SCRATCH);
			setState(91);
			match(DIR);
			setState(92);
			file();
			setState(93);
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
		enterRule(_localctx, 14, RULE_select);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(95);
			relname();
			setState(96);
			match(T__0);
			setState(97);
			gfquery();
			setState(98);
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
		enterRule(_localctx, 16, RULE_gfquery);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(100);
			match(SELECT);
			setState(101);
			schema();
			setState(102);
			match(FROM);
			setState(103);
			relname();
			setState(104);
			match(WHERE);
			setState(105);
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
		int _startState = 18;
		enterRecursionRule(_localctx, 18, RULE_expr, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(115);
			switch ( getInterpreter().adaptivePredict(_input,7,_ctx) ) {
			case 1:
				{
				_localctx = new NotExprContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(108);
				match(NOT);
				setState(109);
				expr(5);
				}
				break;
			case 2:
				{
				_localctx = new GuardedExprContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(110);
				guardedrel();
				}
				break;
			case 3:
				{
				_localctx = new ParExprContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(111);
				match(LPAR);
				setState(112);
				expr(0);
				setState(113);
				match(RPAR);
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(125);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,9,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(123);
					switch ( getInterpreter().adaptivePredict(_input,8,_ctx) ) {
					case 1:
						{
						_localctx = new AndExprContext(new ExprContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(117);
						if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
						setState(118);
						match(AND);
						setState(119);
						expr(5);
						}
						break;
					case 2:
						{
						_localctx = new OrExprContext(new ExprContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(120);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(121);
						match(OR);
						setState(122);
						expr(4);
						}
						break;
					}
					} 
				}
				setState(127);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,9,_ctx);
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
		enterRule(_localctx, 20, RULE_guardedrel);
		try {
			setState(138);
			switch ( getInterpreter().adaptivePredict(_input,10,_ctx) ) {
			case 1:
				_localctx = new RegularGuardedContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(128);
				schema();
				setState(129);
				match(IN);
				setState(130);
				relname();
				}
				break;
			case 2:
				_localctx = new NestedGuardedContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(132);
				schema();
				setState(133);
				match(IN);
				setState(134);
				match(LPAR);
				setState(135);
				gfquery();
				setState(136);
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
		enterRule(_localctx, 22, RULE_anystring);
		try {
			setState(143);
			switch (_input.LA(1)) {
			case LOAD:
			case REL:
			case CSV:
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
				setState(140);
				keyword();
				}
				break;
			case ID:
				enterOuterAlt(_localctx, 2);
				{
				setState(141);
				match(ID);
				}
				break;
			case FILENAME:
				enterOuterAlt(_localctx, 3);
				{
				setState(142);
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
		enterRule(_localctx, 24, RULE_keyword);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(145);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << LOAD) | (1L << REL) | (1L << CSV) | (1L << AS) | (1L << SET) | (1L << DIR) | (1L << OUTPUT) | (1L << SCRATCH) | (1L << SELECT) | (1L << FROM) | (1L << WHERE) | (1L << IN) | (1L << OR) | (1L << AND) | (1L << NOT))) != 0)) ) {
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
		case 9:
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
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3\33\u0096\4\2\t\2"+
		"\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\3\2\3\2\3\2\3\2\7\2!\n\2\f\2\16\2$\13"+
		"\2\3\3\3\3\3\3\3\3\5\3*\n\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3"+
		"\3\3\3\3\3\3\3\5\3:\n\3\3\4\3\4\3\5\3\5\3\5\3\5\3\6\3\6\3\6\3\6\7\6F\n"+
		"\6\f\6\16\6I\13\6\3\6\3\6\3\6\3\6\7\6O\n\6\f\6\16\6R\13\6\5\6T\n\6\3\7"+
		"\3\7\3\7\3\7\3\7\3\7\3\b\3\b\3\b\3\b\3\b\3\b\3\t\3\t\3\t\3\t\3\t\3\n\3"+
		"\n\3\n\3\n\3\n\3\n\3\n\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\5\13v\n"+
		"\13\3\13\3\13\3\13\3\13\3\13\3\13\7\13~\n\13\f\13\16\13\u0081\13\13\3"+
		"\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\5\f\u008d\n\f\3\r\3\r\3\r\5\r\u0092"+
		"\n\r\3\16\3\16\3\16\2\3\24\17\2\4\6\b\n\f\16\20\22\24\26\30\32\2\3\3\2"+
		"\5\23\u0098\2\"\3\2\2\2\49\3\2\2\2\6;\3\2\2\2\b=\3\2\2\2\nS\3\2\2\2\f"+
		"U\3\2\2\2\16[\3\2\2\2\20a\3\2\2\2\22f\3\2\2\2\24u\3\2\2\2\26\u008c\3\2"+
		"\2\2\30\u0091\3\2\2\2\32\u0093\3\2\2\2\34!\5\f\7\2\35!\5\16\b\2\36!\5"+
		"\4\3\2\37!\5\20\t\2 \34\3\2\2\2 \35\3\2\2\2 \36\3\2\2\2 \37\3\2\2\2!$"+
		"\3\2\2\2\" \3\2\2\2\"#\3\2\2\2#\3\3\2\2\2$\"\3\2\2\2%&\5\6\4\2&\'\7\3"+
		"\2\2\')\7\5\2\2(*\7\6\2\2)(\3\2\2\2)*\3\2\2\2*+\3\2\2\2+,\5\b\5\2,-\7"+
		"\b\2\2-.\5\n\6\2./\7\31\2\2/:\3\2\2\2\60\61\5\6\4\2\61\62\7\3\2\2\62\63"+
		"\7\5\2\2\63\64\7\7\2\2\64\65\5\b\5\2\65\66\7\b\2\2\66\67\5\n\6\2\678\7"+
		"\31\2\28:\3\2\2\29%\3\2\2\29\60\3\2\2\2:\5\3\2\2\2;<\7\24\2\2<\7\3\2\2"+
		"\2=>\7\26\2\2>?\5\30\r\2?@\7\26\2\2@\t\3\2\2\2AB\7\27\2\2BG\7\24\2\2C"+
		"D\7\4\2\2DF\7\24\2\2EC\3\2\2\2FI\3\2\2\2GE\3\2\2\2GH\3\2\2\2HJ\3\2\2\2"+
		"IG\3\2\2\2JT\7\30\2\2KP\7\24\2\2LM\7\4\2\2MO\7\24\2\2NL\3\2\2\2OR\3\2"+
		"\2\2PN\3\2\2\2PQ\3\2\2\2QT\3\2\2\2RP\3\2\2\2SA\3\2\2\2SK\3\2\2\2T\13\3"+
		"\2\2\2UV\7\t\2\2VW\7\13\2\2WX\7\n\2\2XY\5\b\5\2YZ\7\31\2\2Z\r\3\2\2\2"+
		"[\\\7\t\2\2\\]\7\f\2\2]^\7\n\2\2^_\5\b\5\2_`\7\31\2\2`\17\3\2\2\2ab\5"+
		"\6\4\2bc\7\3\2\2cd\5\22\n\2de\7\31\2\2e\21\3\2\2\2fg\7\r\2\2gh\5\n\6\2"+
		"hi\7\16\2\2ij\5\6\4\2jk\7\17\2\2kl\5\24\13\2l\23\3\2\2\2mn\b\13\1\2no"+
		"\7\23\2\2ov\5\24\13\7pv\5\26\f\2qr\7\27\2\2rs\5\24\13\2st\7\30\2\2tv\3"+
		"\2\2\2um\3\2\2\2up\3\2\2\2uq\3\2\2\2v\177\3\2\2\2wx\f\6\2\2xy\7\22\2\2"+
		"y~\5\24\13\7z{\f\5\2\2{|\7\21\2\2|~\5\24\13\6}w\3\2\2\2}z\3\2\2\2~\u0081"+
		"\3\2\2\2\177}\3\2\2\2\177\u0080\3\2\2\2\u0080\25\3\2\2\2\u0081\177\3\2"+
		"\2\2\u0082\u0083\5\n\6\2\u0083\u0084\7\20\2\2\u0084\u0085\5\6\4\2\u0085"+
		"\u008d\3\2\2\2\u0086\u0087\5\n\6\2\u0087\u0088\7\20\2\2\u0088\u0089\7"+
		"\27\2\2\u0089\u008a\5\22\n\2\u008a\u008b\7\30\2\2\u008b\u008d\3\2\2\2"+
		"\u008c\u0082\3\2\2\2\u008c\u0086\3\2\2\2\u008d\27\3\2\2\2\u008e\u0092"+
		"\5\32\16\2\u008f\u0092\7\24\2\2\u0090\u0092\7\25\2\2\u0091\u008e\3\2\2"+
		"\2\u0091\u008f\3\2\2\2\u0091\u0090\3\2\2\2\u0092\31\3\2\2\2\u0093\u0094"+
		"\t\2\2\2\u0094\33\3\2\2\2\16 \")9GPSu}\177\u008c\u0091";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}