package gumbo.input.parser;

import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.TerminalNode;

import gumbo.input.parser.GumboParser.AndExprContext;
import gumbo.input.parser.GumboParser.GfqueryContext;
import gumbo.input.parser.GumboParser.GuardedExprContext;
import gumbo.input.parser.GumboParser.NestedGuardedContext;
import gumbo.input.parser.GumboParser.NotExprContext;
import gumbo.input.parser.GumboParser.OrExprContext;
import gumbo.input.parser.GumboParser.ParExprContext;
import gumbo.input.parser.GumboParser.RegularGuardedContext;
import gumbo.input.parser.GumboParser.SelectContext;
import gumbo.structures.gfexpressions.GFAndExpression;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.GFExpression;
import gumbo.structures.gfexpressions.GFNotExpression;
import gumbo.structures.gfexpressions.GFOrExpression;

/**
 * Visitor class for the gfquery rules in the gumbo grammar
 * 
 * @author brentchesny
 *
 */
public class GumboGFQueryVisitor extends GumboBaseVisitor<GFExpression> {

	private final static String TEMP_RELNAME = "Temp";
	private int _currentID;
	private ArrayList<String> _relations;
	
	public GumboGFQueryVisitor(ArrayList<String> inputrelations) {
		_currentID = 0;
		
		_relations = new ArrayList<String>();
		_relations.addAll(inputrelations);
	}	
	
	@Override
	public GFExpression visitSelect(SelectContext ctx) {
		GFExpression expr = this.visit(ctx.gfquery());
		
		if (!(expr instanceof GFExistentialExpression))
			throw new ParseCancellationException("Select statement is not a GFExistentialExpression!");
		
		GFExistentialExpression e = (GFExistentialExpression) expr;
	
		return new GFExistentialExpression(e.getGuard(), e.getChild(), new GFAtomicExpression(ctx.relname().getText(), e.getOutputRelation().getVars().clone()));
	}
	
	/**
	 * @see gumbo.input.parser.GumboBaseVisitor#visitGfquery(gumbo.input.parser.GumboParser.GfqueryContext)
	 */
	@Override
	public GFExpression visitGfquery(GfqueryContext ctx) {
		
		// output relation
		String outputName = TEMP_RELNAME + _currentID;
		_currentID++;	
		
		ArrayList<String> vars = new ArrayList<String>();
		for (TerminalNode var : ctx.schema(0).ID()) {
			vars.add(var.getText());
		}
		String[] outputVars = new String[vars.size()];
		outputVars = vars.toArray(outputVars);
		GFAtomicExpression output = new GFAtomicExpression(outputName, outputVars);
		
		// guard
		String guardName = ctx.relname().getText();
		if (!_relations.contains(guardName))
			throw new ParseCancellationException("Unknown relation name on line " + ctx.relname().getStart().getLine() + ".");
		vars.clear();
		for (TerminalNode var : ctx.schema(1).ID()) {
			vars.add(var.getText());
		}
		String[] guardVars = new String[vars.size()];
		guardVars = vars.toArray(guardVars);
		GFAtomicExpression guard = new GFAtomicExpression(guardName, guardVars);
		
		// child expression
		GFExpression child = this.visit(ctx.expr());
		
		return new GFExistentialExpression(guard, child, output);
	}
	
	@Override
	public GFExpression visitNotExpr(NotExprContext ctx) {
		return new GFNotExpression(this.visit(ctx.expr()));
	}
	
	@Override
	public GFExpression visitAndExpr(AndExprContext ctx) {
		return new GFAndExpression(this.visit(ctx.expr(0)), this.visit(ctx.expr(1)));
	}
	
	@Override
	public GFExpression visitOrExpr(OrExprContext ctx) {
		return new GFOrExpression(this.visit(ctx.expr(0)), this.visit(ctx.expr(1)));
	}
	
	@Override
	public GFExpression visitGuardedExpr(GuardedExprContext ctx) {
		return this.visit(ctx.guardedrel());
	}
	
	@Override
	public GFExpression visitParExpr(ParExprContext ctx) {
		return this.visit(ctx.expr());
	}
	
	@Override
	public GFExpression visitRegularGuarded(RegularGuardedContext ctx) {
		String guardedName = ctx.relname().getText();
		ArrayList<String> vars = new ArrayList<String>();
		for (TerminalNode var : ctx.schema().ID()) {
			vars.add(var.getText());
		}
		String[] guardedVars = new String[vars.size()];
		guardedVars = vars.toArray(guardedVars);
		
		return new GFAtomicExpression(guardedName, guardedVars);
	}
	
	@Override
	public GFExpression visitNestedGuarded(NestedGuardedContext ctx) {
		GFExpression nested = this.visit(ctx.gfquery());
		
		
		
		return nested;
	}
	
}
