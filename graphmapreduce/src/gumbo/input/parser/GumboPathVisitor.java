package gumbo.input.parser;

import gumbo.input.parser.antlr.GumboBaseVisitor;
import gumbo.input.parser.antlr.GumboParser.OutputpathContext;
import gumbo.input.parser.antlr.GumboParser.ScratchpathContext;

import org.apache.hadoop.fs.Path;

/**
 * Visitor class for the output and scratch path rules in the Gumbo grammar
 * 
 * @author brentchesny
 *
 */
public class GumboPathVisitor extends GumboBaseVisitor<Path> {

	/**
	 * @see gumbo.input.parser.antlr.GumboBaseVisitor#visitOutputpath(gumbo.input.parser.antlr.GumboParser.OutputpathContext)
	 */
	@Override
	public Path visitOutputpath(OutputpathContext ctx) {
		return new Path(ctx.file().anystring().getText());		
	}
	
	/**
	 * @see gumbo.input.parser.antlr.GumboBaseVisitor#visitScratchpath(gumbo.input.parser.antlr.GumboParser.ScratchpathContext)
	 */
	@Override
	public Path visitScratchpath(ScratchpathContext ctx) {
		return new Path(ctx.file().anystring().getText());		
	}
	
}
