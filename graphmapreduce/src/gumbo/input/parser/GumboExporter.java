package gumbo.input.parser;

import gumbo.compiler.filemapper.InputFormat;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.generator.GFGenerator;
import gumbo.generator.GFGeneratorException;
import gumbo.generator.QueryType;
import gumbo.input.GumboQuery;
import gumbo.input.parser.GumboScriptFileParser.GumboParseException;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFAndExpression;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.GFExpression;
import gumbo.structures.gfexpressions.GFNotExpression;
import gumbo.structures.gfexpressions.GFOrExpression;
import gumbo.structures.gfexpressions.GFVisitor;
import gumbo.structures.gfexpressions.GFVisitorException;

import java.io.IOException;
import java.util.Stack;

import org.apache.hadoop.fs.Path;

/**
 * Class that can be used to export a GumboQuery to a script
 * @author brentchesny
 *
 */
public class GumboExporter implements GFVisitor<String> {
	
	private Stack<GFAtomicExpression> _guards;
	
	public GumboExporter() {
		_guards = new Stack<GFAtomicExpression>();
	}
	
	public String export(GumboQuery query) {
		String script = "";
		
		script += exportOutputDir(query.getOutput());
		script += exportScratchDir(query.getScratch());
		script += System.lineSeparator();
		
		script += exportLoads(query);
		script += System.lineSeparator();
		
		script += exportQueries(query);
		script += System.lineSeparator();
		
		return script;
	}
	
	private String exportQueries(GumboQuery query) {
		String script = "";
		
		for (GFExpression gf : query.getQueries()) {
			if (!(gf instanceof GFExistentialExpression))
				continue;
			
			script += exportGFExistential((GFExistentialExpression) gf);
		}

		return script;
	}
	
	private String exportGFExistential(GFExistentialExpression gfe) {		
		String out = gfe.getOutputRelation().getName() + " = ";
		String indent = "";
		for (int i = 0; i < out.length(); i++) 
			indent += " ";
		
		// select clause
		String select = "SELECT ";
		String selectschema = "";
		for (String field : gfe.getOutputSchema().getFields()) {
			selectschema += ", $" + field.substring(1);
		}
		select += selectschema.substring(2) + System.lineSeparator();
		
		// from clause
		String from = "FROM " + gfe.getGuard().getName() + System.lineSeparator();
		_guards.push(gfe.getGuard());
		
		// where clause
		String where = "WHERE ";
		try {
			where += gfe.getChild().accept(this);
		} catch (GFVisitorException e) {
			e.printStackTrace();
		}
		where += ";" + System.lineSeparator();
				
		String script = out + select;
		script += indent + from;
		script += indent + where;
		
		script += System.lineSeparator();
		script += System.lineSeparator();
		
		_guards.pop();
		
		return script;
	}


	private String exportLoads(GumboQuery query) {
		RelationFileMapping rfm = query.getInputs();
		
		String script = "";
		
		for (RelationSchema schema : rfm.getSchemas()) {
			for (Path path : rfm.getPaths(schema)) {
				String format = rfm.getInputFormat(schema) == InputFormat.CSV ? "CSV" : "REL";
				script += schema.getName() + " = LOAD " + format + " '" + path.toString() + "' ARITY " + schema.getNumFields() + ";" + System.lineSeparator();
				break;
			}
		}
		
		return script;
	}

	private String exportOutputDir(Path path) {
		return "SET OUTPUT DIR '" + path.toString() + "';" + System.lineSeparator();
	}
	
	private String exportScratchDir(Path path) {
		return "SET SCRATCH DIR '" + path.toString() + "';" + System.lineSeparator();
	}

	private int getPositionInGuard(String field) {
		int index = 0;
		for (String var : _guards.peek().getVars()) {
			if (var.equals(field))
				return index;
			index++;
		}
		
		return -1;
	}

	@Override
	public String visit(GFExpression e) throws GFVisitorException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String visit(GFAtomicExpression e) throws GFVisitorException {		
		String schema = "";
		for (String var : e.getVars()) 
			schema += ", $" + getPositionInGuard(var);

		return schema.substring(2) + " IN " + e.getName();
	}

	@Override
	public String visit(GFAndExpression e) throws GFVisitorException {
		return "(" + e.getChild1().accept(this) + " AND " + e.getChild2().accept(this) + ")";
	}

	@Override
	public String visit(GFOrExpression e) throws GFVisitorException {
		return "(" + e.getChild1().accept(this) + " OR " + e.getChild2().accept(this) + ")";
	}

	@Override
	public String visit(GFNotExpression e) throws GFVisitorException {
		return "(NOT " + e.getChild().accept(this) + ")";
	}

	@Override
	public String visit(GFExistentialExpression e) throws GFVisitorException {
		return null;
	}

	public static void main(String[] args) {
		try {
			
			GFGenerator generator1 = new GFGenerator();
			generator1.addGuardRelation("R", 10, "input/experiments/EXP_008/R", InputFormat.CSV);
			generator1.addGuardedRelation("S", 1, "input/experiments/EXP_008/S", InputFormat.CSV);
			generator1.addQuery(QueryType.AND, 10);
			generator1.addQuery(QueryType.NEGATED_OR, 4);
			GumboQuery query1 = generator1.generate("GeneratorTest");
			System.out.println(query1);
			
			System.out.println("\n\n\n---------------------------------\n\n\n");
			
			GumboExporter exporter = new GumboExporter();
			System.out.println(exporter.export(query1));
			
			System.out.println("\n\n\n---------------------------------\n\n\n");

			GFGenerator generator2 = new GFGenerator();
			generator2.addGuardRelation("R", 3, "input/experiments/EXP_008/R", InputFormat.CSV);
			generator2.addGuardedRelation("S", 1, "input/experiments/EXP_008/S", InputFormat.CSV);
			generator2.addGuardedRelation("T", 1, "input/experiments/EXP_008/T", InputFormat.CSV);
			generator2.addGuardedRelation("U", 1, "input/experiments/EXP_008/U", InputFormat.CSV);
			generator2.addUniqueQuery(); 
			GumboQuery query2 = generator2.generate("UniqueQuery");
			System.out.println(query2);
			
			System.out.println("\n\n\n---------------------------------\n\n\n");
			
			System.out.println(exporter.export(query2));
			
			System.out.println("\n\n\n---------------------------------\n\n\n");

			GumboScriptFileParser parser = new GumboScriptFileParser();
			System.out.println(exporter.export(parser.parse("queries/grammartest.gumbo")));

			
		} catch (GFGeneratorException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (GumboParseException e) {
			e.printStackTrace();
		}
	}
}
