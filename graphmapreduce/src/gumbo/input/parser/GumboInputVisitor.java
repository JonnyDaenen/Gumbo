package gumbo.input.parser;

import gumbo.compiler.filemapper.InputFormat;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.input.parser.antlr.GumboBaseVisitor;
import gumbo.input.parser.antlr.GumboParser.InputArityContext;
import gumbo.input.parser.antlr.GumboParser.InputSchemaContext;
import gumbo.structures.data.RelationSchema;

import java.util.ArrayList;

import org.apache.hadoop.fs.Path;

/**
 * Visitor class for the input rules in the gumbo grammar
 * 
 * @author brentchesny
 *
 */
public class GumboInputVisitor extends GumboBaseVisitor<String> {
	
	private RelationFileMapping _rm;
	private ArrayList<RelationSchema> _inputRelations;
	
	/**
	 * Constructor method
	 */
	public GumboInputVisitor() {
		_rm = new RelationFileMapping();
		_inputRelations = new ArrayList<RelationSchema>();
	}
	
	@Override
	public String visitInputArity(InputArityContext ctx) {
		String relname = ctx.relname().getText();
		int arity = Integer.parseInt(ctx.relarity().INT().getText());
		RelationSchema schema = new RelationSchema(relname, arity);
		
		Path path = new Path(ctx.file().anystring().getText());
		
		InputFormat format = ctx.format() == null || ctx.format().getText().equals("CSV") ? InputFormat.CSV : InputFormat.REL;
		
		_rm.addPath(schema, path, format);
		_inputRelations.add(schema);
		
		return relname;
	}
	
	@Override
	public String visitInputSchema(InputSchemaContext ctx) {
		String relname = ctx.relname().getText();
		
		int arity = ctx.loadschema().ID().size();
		String[] fields = new String[arity];
		for (int i = 0; i < arity; i++) {
			fields[i] = ctx.loadschema().ID().get(i).getText();
		}
		RelationSchema schema = new RelationSchema(relname, fields);
		
		Path path = new Path(ctx.file().anystring().getText());
		
		InputFormat format = ctx.format() == null || ctx.format().getText().equals("CSV") ? InputFormat.CSV : InputFormat.REL;
		
		_rm.addPath(schema, path, format);
		_inputRelations.add(schema);
		
		return relname;
	}

	
	/**
	 * Getter for the relationfilemapping
	 * @return Returns the relationfilemapping
	 */
	public RelationFileMapping getRelationFileMapping() {
		return _rm;
	}
	
	public ArrayList<RelationSchema> getInputRelations() {
		return _inputRelations;
	}
	
}
