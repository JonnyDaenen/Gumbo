package gumbo.input.parser;

import gumbo.input.GumboQuery;
import gumbo.input.parser.antlr.GumboBaseVisitor;
import gumbo.input.parser.antlr.GumboParser.InputContext;
import gumbo.input.parser.antlr.GumboParser.OutputpathContext;
import gumbo.input.parser.antlr.GumboParser.ScratchpathContext;
import gumbo.input.parser.antlr.GumboParser.ScriptContext;
import gumbo.input.parser.antlr.GumboParser.SelectContext;
import gumbo.structures.gfexpressions.GFExpression;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;

/**
 * Visitor class for the script rule in the gumbo grammar
 * 
 * @author brentchesny
 *
 */
public class GumboScriptVisitor extends GumboBaseVisitor<GumboQuery> {

	/**
	 * @see gumbo.input.parser.antlr.GumboBaseVisitor#visitScript(gumbo.input.parser.antlr.GumboParser.ScriptContext)
	 */
	@Override
	public GumboQuery visitScript(ScriptContext ctx) {
		GumboQuery gq = new GumboQuery();
		
		GumboPathVisitor pathvisitor = new GumboPathVisitor();
		
		// output path
		List<OutputpathContext> outputpaths = ctx.outputpath();
		gq.setOutput(pathvisitor.visitOutputpath(outputpaths.get(outputpaths.size() - 1))); // set output path according to last output path statement in the script
		
		// scratch path
		List<ScratchpathContext> scratchpaths = ctx.scratchpath();
		gq.setScratch(pathvisitor.visitScratchpath(scratchpaths.get(scratchpaths.size() - 1))); // set scratch path according to last scratch path statement in the script
		
		// input relations
		GumboInputVisitor inputvisitor = new GumboInputVisitor();
		for (InputContext input : ctx.input()) {
			inputvisitor.visit(input);
		}
		gq.setInputs(inputvisitor.getRelationFileMapping());
		
		// select queries
		GumboGFQueryVisitor queryvisitor = new GumboGFQueryVisitor(inputvisitor.getInputRelations());
		Collection<GFExpression> queryset = new HashSet<>();
		for (SelectContext select : ctx.select()) {
			queryset.add(queryvisitor.visit(select));
		}
		gq.setQueries(queryset);
		
		return gq;
	}
	
}
