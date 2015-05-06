package gumbo.input.parser;

import java.util.ArrayList;

import org.apache.hadoop.fs.Path;

import gumbo.compiler.filemapper.InputFormat;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.input.parser.GumboParser.InputCsvContext;
import gumbo.input.parser.GumboParser.InputRelContext;
import gumbo.structures.data.RelationSchema;

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

	/**
	 * @see gumbo.input.parser.GumboBaseVisitor#visitInputCsv(gumbo.input.parser.GumboParser.InputCsvContext)
	 */
	@Override
	public String visitInputCsv(InputCsvContext ctx) {
		
		String relname = ctx.relname().getText();
		int arity = Integer.parseInt(ctx.relarity().INT().getText());
		RelationSchema schema = new RelationSchema(relname, arity);
		
		Path path = new Path(ctx.file().anystring().getText());
		
		_rm.addPath(schema, path, InputFormat.CSV);
		_inputRelations.add(schema);
		
		return relname;
	}
	
	/**
	 * @see gumbo.input.parser.GumboBaseVisitor#visitInputRel(gumbo.input.parser.GumboParser.InputRelContext)
	 */
	@Override
	public String visitInputRel(InputRelContext ctx) {
		String relname = ctx.relname().getText();
		int arity = Integer.parseInt(ctx.relarity().INT().getText());
		RelationSchema schema = new RelationSchema(relname, arity);
		
		Path path = new Path(ctx.file().anystring().getText());
		
		_rm.addPath(schema, path, InputFormat.REL);
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
