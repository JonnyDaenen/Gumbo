package gumbo.input.parser;

// Generated from Gumbo.g4 by ANTLR 4.5
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link GumboParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface GumboVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link GumboParser#script}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitScript(GumboParser.ScriptContext ctx);
	/**
	 * Visit a parse tree produced by the {@code InputRel}
	 * labeled alternative in {@link GumboParser#input}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInputRel(GumboParser.InputRelContext ctx);
	/**
	 * Visit a parse tree produced by the {@code InputCsv}
	 * labeled alternative in {@link GumboParser#input}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInputCsv(GumboParser.InputCsvContext ctx);
	/**
	 * Visit a parse tree produced by {@link GumboParser#relname}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRelname(GumboParser.RelnameContext ctx);
	/**
	 * Visit a parse tree produced by {@link GumboParser#file}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFile(GumboParser.FileContext ctx);
	/**
	 * Visit a parse tree produced by {@link GumboParser#schema}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSchema(GumboParser.SchemaContext ctx);
	/**
	 * Visit a parse tree produced by {@link GumboParser#outputpath}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOutputpath(GumboParser.OutputpathContext ctx);
	/**
	 * Visit a parse tree produced by {@link GumboParser#scratchpath}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitScratchpath(GumboParser.ScratchpathContext ctx);
	/**
	 * Visit a parse tree produced by {@link GumboParser#select}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSelect(GumboParser.SelectContext ctx);
	/**
	 * Visit a parse tree produced by {@link GumboParser#gfquery}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitGfquery(GumboParser.GfqueryContext ctx);
	/**
	 * Visit a parse tree produced by the {@code AndExpr}
	 * labeled alternative in {@link GumboParser#expr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAndExpr(GumboParser.AndExprContext ctx);
	/**
	 * Visit a parse tree produced by the {@code GuardedExpr}
	 * labeled alternative in {@link GumboParser#expr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitGuardedExpr(GumboParser.GuardedExprContext ctx);
	/**
	 * Visit a parse tree produced by the {@code ParExpr}
	 * labeled alternative in {@link GumboParser#expr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitParExpr(GumboParser.ParExprContext ctx);
	/**
	 * Visit a parse tree produced by the {@code NotExpr}
	 * labeled alternative in {@link GumboParser#expr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNotExpr(GumboParser.NotExprContext ctx);
	/**
	 * Visit a parse tree produced by the {@code OrExpr}
	 * labeled alternative in {@link GumboParser#expr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOrExpr(GumboParser.OrExprContext ctx);
	/**
	 * Visit a parse tree produced by the {@code RegularGuarded}
	 * labeled alternative in {@link GumboParser#guardedrel}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRegularGuarded(GumboParser.RegularGuardedContext ctx);
	/**
	 * Visit a parse tree produced by the {@code NestedGuarded}
	 * labeled alternative in {@link GumboParser#guardedrel}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNestedGuarded(GumboParser.NestedGuardedContext ctx);
	/**
	 * Visit a parse tree produced by {@link GumboParser#anystring}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAnystring(GumboParser.AnystringContext ctx);
	/**
	 * Visit a parse tree produced by {@link GumboParser#keyword}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitKeyword(GumboParser.KeywordContext ctx);
}