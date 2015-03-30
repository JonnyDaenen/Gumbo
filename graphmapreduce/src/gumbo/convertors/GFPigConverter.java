package gumbo.convertors;

import java.util.Collection;

import org.apache.hadoop.fs.Path;

import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.input.GumboQuery;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.GFExpression;

/**
 * A class that can be used to convert GF queries into aquivalent Pig Latin scripts
 * 
 * @author brentchesny
 *
 */
public abstract class GFPigConverter {
	
	/**
	 * Converts a GumboQuery into an equivalent Pig Latin script
	 * @param query The query to convert
	 * @return An equivalent Pig Latin script
	 * @throws GFConversionException
	 */
	public String convert(GumboQuery query) throws GFConversionException {
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
	 */
	public abstract String convert(GFExistentialExpression gfe, RelationFileMapping rfm) throws GFConversionException; 
}
