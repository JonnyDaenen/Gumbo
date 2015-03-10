package gumbo.input.parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.hadoop.fs.Path;

import gumbo.compiler.filemapper.InputFormat;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.input.parser.GumboParser.InputCsvContext;
import gumbo.input.parser.GumboParser.InputRelContext;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFAtomicExpression;

/**
 * Visitor class for the input rules in the gumbo grammar
 * 
 * @author brentchesny
 *
 */
public class GumboInputVisitor extends GumboBaseVisitor<String> {
	
	private RelationFileMapping _rm;
	private Map<String, GFAtomicExpression> _inputRelations;
	
	/**
	 * Constructor method
	 */
	public GumboInputVisitor() {
		_rm = new RelationFileMapping();
		_inputRelations = new HashMap<String, GFAtomicExpression>();
	}

	/**
	 * @see gumbo.input.parser.GumboBaseVisitor#visitInputCsv(gumbo.input.parser.GumboParser.InputCsvContext)
	 */
	@Override
	public String visitInputCsv(InputCsvContext ctx) {
		
		String relname = ctx.relname().getText();
		int arity = ctx.schema().ID().size();
		RelationSchema schema = new RelationSchema(relname, arity);
		
		Path path = new Path(ctx.file().anystring().getText());
		
		_rm.addPath(schema, path, InputFormat.CSV);
		
		ArrayList<String> vars = new ArrayList<String>();
		for (TerminalNode var : ctx.schema().ID()) {
			vars.add(var.getText());
		}
		String[] varArray = new String[vars.size()];
		varArray = vars.toArray(varArray);
		_inputRelations.put(relname, new GFAtomicExpression(relname, varArray));
		
		return relname;
	}
	
	/**
	 * @see gumbo.input.parser.GumboBaseVisitor#visitInputRel(gumbo.input.parser.GumboParser.InputRelContext)
	 */
	@Override
	public String visitInputRel(InputRelContext ctx) {
		String relname = ctx.relname().getText();
		int arity = ctx.schema().ID().size();
		RelationSchema schema = new RelationSchema(relname, arity);
		
		Path path = new Path(ctx.file().anystring().getText());
		
		_rm.addPath(schema, path, InputFormat.REL);
		
		ArrayList<String> vars = new ArrayList<String>();
		for (TerminalNode var : ctx.schema().ID()) {
			vars.add(var.getText());
		}
		String[] varArray = new String[vars.size()];
		varArray = vars.toArray(varArray);
		_inputRelations.put(relname, new GFAtomicExpression(relname, varArray));
		
		return relname;
	}
	
	/**
	 * Getter for the relationfilemapping
	 * @return Returns the relationfilemapping
	 */
	public RelationFileMapping getRelationFileMapping() {
		return _rm;
	}
	
	public Map<String, GFAtomicExpression> getInputRelations() {
		return _inputRelations;
	}
	
}
