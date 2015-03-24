package gumbo.convertors;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.fs.Path;

import gumbo.compiler.filemapper.InputFormat;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.generator.GFGenerator;
import gumbo.generator.QueryType;
import gumbo.input.GumboQuery;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFAndExpression;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.GFExpression;
import gumbo.structures.gfexpressions.GFNotExpression;
import gumbo.structures.gfexpressions.GFOrExpression;
import gumbo.structures.gfexpressions.GFVisitor;
import gumbo.structures.gfexpressions.GFVisitorException;

/**
 * Class that can be used to convert GF queries to Pig Latin scripts
 * 
 * @author brentchesny
 *
 */
public class GFPigConverter implements GFVisitor<String> {
	
	/**
	 * Converts a GumboQuery into an equivalent Pig Latin script
	 * @param query The query to convert
	 * @return An equivalent Pig Latin script
	 * @throws GFConversionException
	 * @throws GFVisitorException
	 */
	public String convert(GumboQuery query) throws GFConversionException, GFVisitorException {
		String script = "";
		
		script += loadRelations(query.getInputs());
		
		for (GFExpression gfe : query.getQueries()) {
			script += convert((GFExistentialExpression) gfe, query.getInputs());
		}
		
		script += storeResults(query.getQueries(), query.getOutput());
		
		return script;
	}

	/**
	 * Creates the load statements for the given input relations
	 * @param inputs A RelationFileMapping containing input relations
	 * @return Pig load statements for the input relations
	 */
	private String loadRelations(RelationFileMapping inputs) {
		String load = "";
		
		for (RelationSchema schema : inputs.getSchemas()) {
			Path path = (Path) inputs.getPaths(schema).toArray()[0];
			String pigschema = schema.toString().substring(schema.getName().length());
			
			load += schema.getName() + " = LOAD '" + path.toString() + "' USING PigStorage(',') AS " + pigschema + ";" + System.lineSeparator();
		}
		
		load += System.lineSeparator();
		
		return load;
	}
	
	/**
	 * Creates the store statements for a set of queries
	 * @param queries The queries to store the results from
	 * @param output The path where the output should be stored
	 * @return Pig store statements for the results of the queries
	 */
	private String storeResults(Collection<GFExpression> queries, Path output) {
		String store = "";
		
		for (GFExpression gfe : queries) {
			GFExistentialExpression expr = (GFExistentialExpression) gfe;
			
			store += "STORE " + expr.getOutputRelation().getName() + " INTO '" + output.toString() + "/pig/" + expr.getOutputRelation().getName() + "' USING PigStorage(',');" + System.lineSeparator();
		}
		
		return store;
	}

	/**
	 * Creates a pig script to evaluate a basic GF expression
	 * @param gfe The basic GF expression to evaluate
	 * @param rfm The RelationFileMapping relevant to this query
	 * @return A pig script to evaluate the query
	 * @throws GFConversionException
	 * @throws GFVisitorException
	 */
	public String convert(GFExistentialExpression gfe, RelationFileMapping rfm) throws GFConversionException, GFVisitorException {
		if (!gfe.isBasicGF())
			throw new GFConversionException("The given GFExpression was not a basic expression");
		
		String outname = gfe.getOutputRelation().getName();
		String guardname = gfe.getGuard().getName();
		String guardschema = gfe.getGuard().toString().substring(guardname.length());
		List<String> childIds = new ArrayList<>();
		int counter = 1;
		
		String query = "";

		for (GFAtomicExpression child : gfe.getGuardedRelations()) {
			String guardedGroupSchema = getGuardedGroupSchema(child.getName(), rfm);
			String guardGroupSchema = child.toString().substring(child.getName().length());
			String childId = removeTokens(child.toString());
			childIds.add(childId);

			query += outname + "_A" + counter + " = COGROUP " + guardname + " BY " + guardGroupSchema + ", " + child.getName() + " BY " + guardedGroupSchema + ";" + System.lineSeparator();
			query += outname + "_B" + counter + " = FOREACH " + outname + "_A" + counter + " GENERATE flatten(" + guardname + "), (IsEmpty(" + child.getName() + ")? false : true) as " + childId + ";" + System.lineSeparator();
			query += System.lineSeparator();
			
			counter++;
		}
		
		query += outname + "_X = COGROUP ";
		for (int i = 1; i < counter; i++) {
			query += outname + "_B" + i + " by " + guardschema;
			if (i != counter - 1)
				query += ", ";
		}
		query += ";" + System.lineSeparator();
		
		query += outname + "_Y = FOREACH " + outname + "_X GENERATE group";
		for (int i = 1; i < counter; i++) {
			query += ", flatten(" + outname + "_B" + i + "." + childIds.get(i-1) + ")";
		}
		query += ";" + System.lineSeparator();
		
		String boolexpr = gfe.getChild().accept(this);
		query += outname + " = FILTER " + outname + "_Y BY " + boolexpr + ";" + System.lineSeparator();
		query += System.lineSeparator();
				
		return query;
	}

	/**
	 * Creates an alias for a relation schema by removing the parentheses and commas
	 * @param string A relation schema
	 * @return An alias for the given relation schema
	 */
	private String removeTokens(String string) {
		return string.replace("(", "").replace(")", "").replace(",", "");
	}


	/**
	 * Returns the schema by which a guarded relation should be grouped in the pig script
	 * @param name The name of the guarded relation
	 * @param rfm The RelationFileMapping relevant to the query
	 * @return The schema by which a guarded relation should be grouped in the pig script
	 * @throws GFConversionException
	 */
	private String getGuardedGroupSchema(String name, RelationFileMapping rfm) throws GFConversionException {
		String schema = null;
		
		for (RelationSchema rs : rfm.getSchemas()) {
			if (rs.getName() == name) {
				schema = rs.toString().substring(name.length());
				break;
			}
		}
		
		if (schema == null)
			throw new GFConversionException("No schema found with name: " + name);
		
		return schema;
	}


	@Override
	public String visit(GFExpression e) throws GFVisitorException {
		throw new GFVisitorException("Unknown expression type!");
	}


	@Override
	public String visit(GFAtomicExpression e) throws GFVisitorException {
		return removeTokens(e.toString());
	}


	@Override
	public String visit(GFAndExpression e) throws GFVisitorException {
		return "(" + e.getChild1().accept(this) + " and " + e.getChild2().accept(this) + ")";
	}


	@Override
	public String visit(GFOrExpression e) throws GFVisitorException {
		return "(" + e.getChild1().accept(this) + " or " + e.getChild2().accept(this) + ")";
	}


	@Override
	public String visit(GFNotExpression e) throws GFVisitorException {
		return "(not " + e.getChild().accept(this) + ")";
	}


	@Override
	public String visit(GFExistentialExpression e) throws GFVisitorException {
		throw new GFVisitorException("GFExistentialExpression encountered in basic GF expression!");
	}

	// for testing purposes only
	public static void main(String[] args) {
		try {
			GFGenerator generator = new GFGenerator();
			generator.addGuardRelation("R", 10, "input/experiments/EXP_008/R", InputFormat.CSV);
			generator.addGuardedRelation("S", 1, "input/experiments/EXP_008/S", InputFormat.CSV);
			generator.addQuery(QueryType.AND, 2);
			GumboQuery gq = generator.generate("GeneratorTest");
			
			GFPigConverter converter = new GFPigConverter();
			String pig = converter.convert(gq);
			System.out.println(pig);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
