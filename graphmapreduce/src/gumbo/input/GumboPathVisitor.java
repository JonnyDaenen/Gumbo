package gumbo.input;

import gumbo.input.GumboParser.OutputpathContext;
import gumbo.input.GumboParser.ScratchpathContext;

import org.apache.hadoop.fs.Path;

/**
 * Visitor class for the output and scratch path rules in the Gumbo grammar
 * 
 * @author brentchesny
 *
 */
public class GumboPathVisitor extends GumboBaseVisitor<Path> {

	/**
	 * @see gumbo.input.GumboBaseVisitor#visitOutputpath(gumbo.input.GumboParser.OutputpathContext)
	 */
	@Override
	public Path visitOutputpath(OutputpathContext ctx) {
		return new Path(ctx.file().anystring().getText());		
	}
	
	/**
	 * @see gumbo.input.GumboBaseVisitor#visitScratchpath(gumbo.input.GumboParser.ScratchpathContext)
	 */
	@Override
	public Path visitScratchpath(ScratchpathContext ctx) {
		return new Path(ctx.file().anystring().getText());		
	}
	
}
