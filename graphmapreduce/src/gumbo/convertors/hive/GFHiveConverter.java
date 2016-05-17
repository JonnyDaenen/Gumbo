package gumbo.convertors.hive;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.fs.Path;

import gumbo.compiler.calculations.BGFE2CUConverter;
import gumbo.compiler.calculations.BasicGFCalculationUnit;
import gumbo.compiler.calculations.CalculationUnit;
import gumbo.compiler.decomposer.GFDecomposer;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.compiler.linker.CULinker;
import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.compiler.partitioner.CalculationPartitioner;
import gumbo.compiler.partitioner.PartitionedCUGroup;
import gumbo.compiler.partitioner.UnitPartitioner;
import gumbo.convertors.GFConversionException;
import gumbo.convertors.GFConverter;
import gumbo.input.GumboQuery;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFExistentialExpression;

/**
 * A class that can be used to convert GF queries into equivalent Hive queries
 * 
 * @author brentchesny
 *
 */
public abstract class GFHiveConverter implements GFConverter {
	
	protected Vector<RelationSchema> _outSchemas;
	
	/**
	 * Converts a GumboQuery into an equivalent Pig Latin script
	 * @param query The query to convert
	 * @return An equivalent Pig Latin script
	 * @throws GFConversionException
	 */
	public String convert(GumboQuery query) throws GFConversionException {
		_outSchemas = new Vector<RelationSchema>();
		
		String script = "";
		
		script += loadRelations(query.getInputs());

		List<CalculationUnitGroup> partitions = prepare(query).getBottomUpList();
		List<GFExistentialExpression> queries = new ArrayList<GFExistentialExpression>();
		for (CalculationUnitGroup partition : partitions) {
			for (CalculationUnit cu : partition.getCalculations()) {
				BasicGFCalculationUnit bcu = (BasicGFCalculationUnit) cu;
				
				script += createResultTable(bcu.getBasicExpression(), query.getOutput());
				script += convert(bcu.getBasicExpression(), query.getInputs());
				
				queries.add(bcu.getBasicExpression());
				_outSchemas.add(bcu.getBasicExpression().getOutputSchema());
			}
		}
		
		script += cleanup(query.getInputs());
				
		return script;
	}
	
	private PartitionedCUGroup prepare(GumboQuery query) throws GFConversionException {
		try {
			GFDecomposer decomposer = new GFDecomposer();
			BGFE2CUConverter converter = new BGFE2CUConverter();
			CULinker linker = new CULinker();
			CalculationPartitioner partitioner = new UnitPartitioner();
			
			// decompose expressions into basic ones
			Set<GFExistentialExpression> bgfes = decomposer.decomposeAll(query.getQueries());
			// CUConverter 
			Map<RelationSchema, BasicGFCalculationUnit> cus = converter.createCalculationUnits(bgfes);
			// CULinker 
			CalculationUnitGroup dag = linker.createDAG(cus);
			// partition
			PartitionedCUGroup pdag = partitioner.partition(dag, null);
			
			return pdag;
		} catch(Exception e) {
			throw new GFConversionException("There was an error preparing the queries.", e);
		}
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
			String tableschema = generateTableSchema(schema);
			
			load += "CREATE EXTERNAL TABLE " + schema.getName() + " " + tableschema + System.lineSeparator();
			load += "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','" + System.lineSeparator();
			load += "LOCATION '" + path.toString() + "';" + System.lineSeparator();
			load += System.lineSeparator();
		}
				
		return load;
	}
	
	private String cleanup(RelationFileMapping inputs) {
		String clean = "";
		
		for (RelationSchema schema : inputs.getSchemas()) {			
			clean += "DROP TABLE IF EXISTS " + schema.getName() + ";" + System.lineSeparator();
		}
				
		return clean;
	}
	
	/**
	 * Creates the result table for a query
	 * @param queries The queries to store the results from
	 * @param output The path where the output should be stored
	 * @return Pig store statements for the results of the queries
	 */
	private String createResultTable(GFExistentialExpression query, Path output) {
		String store = "";
		
		RelationSchema outschema = query.getOutputRelation().getRelationSchema();

		store += "CREATE TABLE " + outschema.getName() + " " + generateTableSchema(outschema) + System.lineSeparator();
		store += "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','" + System.lineSeparator();
		store += "LOCATION '" + output.toString() + "/hive/" + outschema.getName() + "';" + System.lineSeparator();
		store += System.lineSeparator();
		
		return store;
	}
	
	/**
	 * Generates a hive table schema for a given relationschema
	 * @param schema A relationshema
	 * @return Hive table schema
	 */
	private String generateTableSchema(RelationSchema schema) {
		String tableschema = "";
		
		for (String var : schema.getFields()) {
			tableschema += ", " + var + " " + "STRING";
		}
		
		return "(" + tableschema.substring(2) + ")";
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
