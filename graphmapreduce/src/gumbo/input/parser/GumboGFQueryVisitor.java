package gumbo.input.parser;

import gumbo.input.parser.GumboParser.AndExprContext;
import gumbo.input.parser.GumboParser.GfqueryContext;
import gumbo.input.parser.GumboParser.GuardedExprContext;
import gumbo.input.parser.GumboParser.NestedGuardedContext;
import gumbo.input.parser.GumboParser.NotExprContext;
import gumbo.input.parser.GumboParser.OrExprContext;
import gumbo.input.parser.GumboParser.ParExprContext;
import gumbo.input.parser.GumboParser.RegularGuardedContext;
import gumbo.input.parser.GumboParser.SelectContext;
import gumbo.input.parser.GumboParser.SelectorContext;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFAndExpression;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.GFExpression;
import gumbo.structures.gfexpressions.GFNotExpression;
import gumbo.structures.gfexpressions.GFOrExpression;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Stack;

import org.antlr.v4.runtime.misc.ParseCancellationException;

/**
 * Visitor class for the gfquery rules in the gumbo grammar
 * 
 * @author brentchesny
 *
 */
public class GumboGFQueryVisitor extends GumboBaseVisitor<GFExpression> {

	private final static String TEMP_RELNAME = "Temp";
	private int _currentID;
	private ArrayList<RelationSchema> _relations;
	private Stack<ArrayList<String> > _guardVars;
	
	public GumboGFQueryVisitor(ArrayList<RelationSchema> inputrelations) {
		_currentID = 0;
		
		_guardVars = new Stack<>();
		_relations = new ArrayList<RelationSchema>();
		_relations.addAll(inputrelations);
	}	
	
	@Override
	public GFExpression visitSelect(SelectContext ctx) {
		GFExpression expr = this.visit(ctx.gfquery());
		
		if (!(expr instanceof GFExistentialExpression))
			throw new ParseCancellationException("Select statement is not a GFExistentialExpression!");
		
		GFExistentialExpression e = (GFExistentialExpression) expr;
		
		_relations.add(e.getOutputSchema());
	
		return new GFExistentialExpression(e.getGuard(), e.getChild(), new GFAtomicExpression(ctx.relname().getText(), e.getOutputRelation().getVars().clone()));
	}
	
	/**
	 * @see gumbo.input.parser.GumboBaseVisitor#visitGfquery(gumbo.input.parser.GumboParser.GfqueryContext)
	 */
	@Override
	public GFExpression visitGfquery(GfqueryContext ctx) {
		ArrayList<String> vars = null;
		
		// guard
		vars = new ArrayList<String>();
		String guardName = ctx.relname().getText();
		if (!relationKnown(guardName))
			throw new ParseCancellationException("Unknown relation name on line " + ctx.relname().getStart().getLine() + ".");
		
		_guardVars.push(vars);
		String[] guardVars = getRelationSchema(guardName).getFields().clone();
		_guardVars.push(new ArrayList<>(Arrays.asList(guardVars)));
		GFAtomicExpression guard = new GFAtomicExpression(guardName, guardVars);
		
		// output relation
		String outputName = TEMP_RELNAME + _currentID;
		_currentID++;	
		vars = new ArrayList<String>();
		for (SelectorContext var : ctx.schema().selector()) {
			if (var.getText().contains("$")) {
				int index = Integer.parseInt(var.getText().substring(1));
				if (index < 0 || index > guardVars.length - 1)
					throw new ParseCancellationException("Selector index out of range on line " + ctx.relname().getStart().getLine() + ": index " + index + ".");
				vars.add(guardVars[index]);
			} else {
				vars.add(var.getText());		
			}
		}
		String[] outputVars = new String[vars.size()];
		outputVars = vars.toArray(outputVars);
		GFAtomicExpression output = new GFAtomicExpression(outputName, outputVars);
		
		// child expression
		GFExpression child = this.visit(ctx.expr());
		
		_guardVars.pop();
		
		return new GFExistentialExpression(guard, child, output);
	}

	
	private RelationSchema getRelationSchema(String relname) {
		for (RelationSchema schema : _relations) {
			if (schema.getName().equals(relname))
				return schema;
		}
		
		return null;
	}

	private boolean relationKnown(String relname) {
		for (RelationSchema schema : _relations) {
			if (schema.getName().equals(relname))
				return true;
		}
		
		return false;
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
		ArrayList<String> guardVars = _guardVars.peek();
		for (SelectorContext var : ctx.schema().selector()) {
			if (var.getText().contains("$")) {
				int index = Integer.parseInt(var.getText().substring(1));
				if (index < 0 || index > guardVars.size() - 1)
					throw new ParseCancellationException("Selector index out of range on line " + ctx.relname().getStart().getLine() + ".");
				vars.add(guardVars.get(index));
			} else {
				vars.add(var.getText());		
			}
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
