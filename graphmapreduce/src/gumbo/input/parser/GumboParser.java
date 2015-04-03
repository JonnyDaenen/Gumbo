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
		AND=17, NOT=18, INT=19, ID=20, FILENAME=21, QUOTE=22, LPAR=23, RPAR=24, 
		SEMICOLON=25, WS=26, SINGLE_LINE_COMMENT=27;
	public static final int
		RULE_script = 0, RULE_input = 1, RULE_relname = 2, RULE_relarity = 3, 
		RULE_file = 4, RULE_schema = 5, RULE_outputpath = 6, RULE_scratchpath = 7, 
		RULE_select = 8, RULE_gfquery = 9, RULE_expr = 10, RULE_guardedrel = 11, 
		RULE_anystring = 12, RULE_keyword = 13;
	public static final String[] ruleNames = {
		"script", "input", "relname", "relarity", "file", "schema", "outputpath", 
		"scratchpath", "select", "gfquery", "expr", "guardedrel", "anystring", 
		"keyword"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'='", "','", null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, "'('", 
		"')'", "';'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, "LOAD", "REL", "CSV", "ARITY", "SET", "DIR", "OUTPUT", 
		"SCRATCH", "SELECT", "FROM", "SATISFYING", "WHERE", "IN", "OR", "AND", 
		"NOT", "INT", "ID", "FILENAME", "QUOTE", "LPAR", "RPAR", "SEMICOLON", 
		"WS", "SINGLE_LINE_COMMENT"
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
			setState(34);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==SET || _la==ID) {
				{
				setState(32);
				switch ( getInterpreter().adaptivePredict(_input,0,_ctx) ) {
				case 1:
					{
					setState(28);
					outputpath();
					}
					break;
				case 2:
					{
					setState(29);
					scratchpath();
					}
					break;
				case 3:
					{
					setState(30);
					input();
					}
					break;
				case 4:
					{
					setState(31);
					select();
					}
					break;
				}
				}
				setState(36);
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
			setState(57);
			switch ( getInterpreter().adaptivePredict(_input,3,_ctx) ) {
			case 1:
				_localctx = new InputRelContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(37);
				relname();
				setState(38);
				match(T__0);
				setState(39);
				match(LOAD);
				setState(41);
				_la = _input.LA(1);
				if (_la==REL) {
					{
					setState(40);
					match(REL);
					}
				}

				setState(43);
				file();
				setState(44);
				match(ARITY);
				setState(45);
				relarity();
				setState(46);
				match(SEMICOLON);
				}
				break;
			case 2:
				_localctx = new InputCsvContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(48);
				relname();
				setState(49);
				match(T__0);
				setState(50);
				match(LOAD);
				setState(51);
				match(CSV);
				setState(52);
				file();
				setState(53);
				match(ARITY);
				setState(54);
				relarity();
				setState(55);
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
			setState(59);
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
			setState(61);
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
			setState(63);
			match(QUOTE);
			setState(64);
			anystring();
			setState(65);
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
		enterRule(_localctx, 10, RULE_schema);
		int _la;
		try {
			setState(85);
			switch (_input.LA(1)) {
			case LPAR:
				enterOuterAlt(_localctx, 1);
				{
				setState(67);
				match(LPAR);
				setState(68);
				match(ID);
				setState(73);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(69);
					match(T__1);
					setState(70);
					match(ID);
					}
					}
					setState(75);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(76);
				match(RPAR);
				}
				break;
			case ID:
				enterOuterAlt(_localctx, 2);
				{
				setState(77);
				match(ID);
				setState(82);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(78);
					match(T__1);
					setState(79);
					match(ID);
					}
					}
					setState(84);
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
		enterRule(_localctx, 12, RULE_outputpath);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(87);
			match(SET);
			setState(88);
			match(OUTPUT);
			setState(89);
			match(DIR);
			setState(90);
			file();
			setState(91);
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
		enterRule(_localctx, 14, RULE_scratchpath);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(93);
			match(SET);
			setState(94);
			match(SCRATCH);
			setState(95);
			match(DIR);
			setState(96);
			file();
			setState(97);
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
		enterRule(_localctx, 16, RULE_select);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(99);
			relname();
			setState(100);
			match(T__0);
			setState(101);
			gfquery();
			setState(102);
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
		public List<SchemaContext> schema() {
			return getRuleContexts(SchemaContext.class);
		}
		public SchemaContext schema(int i) {
			return getRuleContext(SchemaContext.class,i);
		}
		public TerminalNode FROM() { return getToken(GumboParser.FROM, 0); }
		public RelnameContext relname() {
			return getRuleContext(RelnameContext.class,0);
		}
		public TerminalNode SATISFYING() { return getToken(GumboParser.SATISFYING, 0); }
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
		enterRule(_localctx, 18, RULE_gfquery);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(104);
			match(SELECT);
			setState(105);
			schema();
			setState(106);
			match(FROM);
			setState(107);
			relname();
			setState(108);
			match(SATISFYING);
			setState(109);
			schema();
			setState(110);
			match(WHERE);
			setState(111);
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
		int _startState = 20;
		enterRecursionRule(_localctx, 20, RULE_expr, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(121);
			switch ( getInterpreter().adaptivePredict(_input,7,_ctx) ) {
			case 1:
				{
				_localctx = new NotExprContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(114);
				match(NOT);
				setState(115);
				expr(5);
				}
				break;
			case 2:
				{
				_localctx = new GuardedExprContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(116);
				guardedrel();
				}
				break;
			case 3:
				{
				_localctx = new ParExprContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(117);
				match(LPAR);
				setState(118);
				expr(0);
				setState(119);
				match(RPAR);
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(131);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,9,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(129);
					switch ( getInterpreter().adaptivePredict(_input,8,_ctx) ) {
					case 1:
						{
						_localctx = new AndExprContext(new ExprContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(123);
						if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
						setState(124);
						match(AND);
						setState(125);
						expr(5);
						}
						break;
					case 2:
						{
						_localctx = new OrExprContext(new ExprContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_expr);
						setState(126);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(127);
						match(OR);
						setState(128);
						expr(4);
						}
						break;
					}
					} 
				}
				setState(133);
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
		enterRule(_localctx, 22, RULE_guardedrel);
		try {
			setState(144);
			switch ( getInterpreter().adaptivePredict(_input,10,_ctx) ) {
			case 1:
				_localctx = new RegularGuardedContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(134);
				schema();
				setState(135);
				match(IN);
				setState(136);
				relname();
				}
				break;
			case 2:
				_localctx = new NestedGuardedContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(138);
				schema();
				setState(139);
				match(IN);
				setState(140);
				match(LPAR);
				setState(141);
				gfquery();
				setState(142);
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
		enterRule(_localctx, 24, RULE_anystring);
		try {
			setState(149);
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
				setState(146);
				keyword();
				}
				break;
			case ID:
				enterOuterAlt(_localctx, 2);
				{
				setState(147);
				match(ID);
				}
				break;
			case FILENAME:
				enterOuterAlt(_localctx, 3);
				{
				setState(148);
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
		enterRule(_localctx, 26, RULE_keyword);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(151);
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
		case 10:
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
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3\35\u009c\4\2\t\2"+
		"\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\3\2\3\2\3\2\3\2\7\2#\n\2\f\2"+
		"\16\2&\13\2\3\3\3\3\3\3\3\3\5\3,\n\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3"+
		"\3\3\3\3\3\3\3\3\3\3\3\5\3<\n\3\3\4\3\4\3\5\3\5\3\6\3\6\3\6\3\6\3\7\3"+
		"\7\3\7\3\7\7\7J\n\7\f\7\16\7M\13\7\3\7\3\7\3\7\3\7\7\7S\n\7\f\7\16\7V"+
		"\13\7\5\7X\n\7\3\b\3\b\3\b\3\b\3\b\3\b\3\t\3\t\3\t\3\t\3\t\3\t\3\n\3\n"+
		"\3\n\3\n\3\n\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\f\3\f\3\f"+
		"\3\f\3\f\3\f\3\f\3\f\5\f|\n\f\3\f\3\f\3\f\3\f\3\f\3\f\7\f\u0084\n\f\f"+
		"\f\16\f\u0087\13\f\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r\5\r\u0093\n"+
		"\r\3\16\3\16\3\16\5\16\u0098\n\16\3\17\3\17\3\17\2\3\26\20\2\4\6\b\n\f"+
		"\16\20\22\24\26\30\32\34\2\3\4\2\5\16\20\24\u009d\2$\3\2\2\2\4;\3\2\2"+
		"\2\6=\3\2\2\2\b?\3\2\2\2\nA\3\2\2\2\fW\3\2\2\2\16Y\3\2\2\2\20_\3\2\2\2"+
		"\22e\3\2\2\2\24j\3\2\2\2\26{\3\2\2\2\30\u0092\3\2\2\2\32\u0097\3\2\2\2"+
		"\34\u0099\3\2\2\2\36#\5\16\b\2\37#\5\20\t\2 #\5\4\3\2!#\5\22\n\2\"\36"+
		"\3\2\2\2\"\37\3\2\2\2\" \3\2\2\2\"!\3\2\2\2#&\3\2\2\2$\"\3\2\2\2$%\3\2"+
		"\2\2%\3\3\2\2\2&$\3\2\2\2\'(\5\6\4\2()\7\3\2\2)+\7\5\2\2*,\7\6\2\2+*\3"+
		"\2\2\2+,\3\2\2\2,-\3\2\2\2-.\5\n\6\2./\7\b\2\2/\60\5\b\5\2\60\61\7\33"+
		"\2\2\61<\3\2\2\2\62\63\5\6\4\2\63\64\7\3\2\2\64\65\7\5\2\2\65\66\7\7\2"+
		"\2\66\67\5\n\6\2\678\7\b\2\289\5\b\5\29:\7\33\2\2:<\3\2\2\2;\'\3\2\2\2"+
		";\62\3\2\2\2<\5\3\2\2\2=>\7\26\2\2>\7\3\2\2\2?@\7\25\2\2@\t\3\2\2\2AB"+
		"\7\30\2\2BC\5\32\16\2CD\7\30\2\2D\13\3\2\2\2EF\7\31\2\2FK\7\26\2\2GH\7"+
		"\4\2\2HJ\7\26\2\2IG\3\2\2\2JM\3\2\2\2KI\3\2\2\2KL\3\2\2\2LN\3\2\2\2MK"+
		"\3\2\2\2NX\7\32\2\2OT\7\26\2\2PQ\7\4\2\2QS\7\26\2\2RP\3\2\2\2SV\3\2\2"+
		"\2TR\3\2\2\2TU\3\2\2\2UX\3\2\2\2VT\3\2\2\2WE\3\2\2\2WO\3\2\2\2X\r\3\2"+
		"\2\2YZ\7\t\2\2Z[\7\13\2\2[\\\7\n\2\2\\]\5\n\6\2]^\7\33\2\2^\17\3\2\2\2"+
		"_`\7\t\2\2`a\7\f\2\2ab\7\n\2\2bc\5\n\6\2cd\7\33\2\2d\21\3\2\2\2ef\5\6"+
		"\4\2fg\7\3\2\2gh\5\24\13\2hi\7\33\2\2i\23\3\2\2\2jk\7\r\2\2kl\5\f\7\2"+
		"lm\7\16\2\2mn\5\6\4\2no\7\17\2\2op\5\f\7\2pq\7\20\2\2qr\5\26\f\2r\25\3"+
		"\2\2\2st\b\f\1\2tu\7\24\2\2u|\5\26\f\7v|\5\30\r\2wx\7\31\2\2xy\5\26\f"+
		"\2yz\7\32\2\2z|\3\2\2\2{s\3\2\2\2{v\3\2\2\2{w\3\2\2\2|\u0085\3\2\2\2}"+
		"~\f\6\2\2~\177\7\23\2\2\177\u0084\5\26\f\7\u0080\u0081\f\5\2\2\u0081\u0082"+
		"\7\22\2\2\u0082\u0084\5\26\f\6\u0083}\3\2\2\2\u0083\u0080\3\2\2\2\u0084"+
		"\u0087\3\2\2\2\u0085\u0083\3\2\2\2\u0085\u0086\3\2\2\2\u0086\27\3\2\2"+
		"\2\u0087\u0085\3\2\2\2\u0088\u0089\5\f\7\2\u0089\u008a\7\21\2\2\u008a"+
		"\u008b\5\6\4\2\u008b\u0093\3\2\2\2\u008c\u008d\5\f\7\2\u008d\u008e\7\21"+
		"\2\2\u008e\u008f\7\31\2\2\u008f\u0090\5\24\13\2\u0090\u0091\7\32\2\2\u0091"+
		"\u0093\3\2\2\2\u0092\u0088\3\2\2\2\u0092\u008c\3\2\2\2\u0093\31\3\2\2"+
		"\2\u0094\u0098\5\34\17\2\u0095\u0098\7\26\2\2\u0096\u0098\7\27\2\2\u0097"+
		"\u0094\3\2\2\2\u0097\u0095\3\2\2\2\u0097\u0096\3\2\2\2\u0098\33\3\2\2"+
		"\2\u0099\u009a\t\2\2\2\u009a\35\3\2\2\2\16\"$+;KTW{\u0083\u0085\u0092"+
		"\u0097";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}