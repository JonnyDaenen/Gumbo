package gumbo.convertors.pig;

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
import gumbo.input.GumboQuery;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFExistentialExpression;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.fs.Path;

/**
 * A class that can be used to convert GF queries into aquivalent Pig Latin scripts
 * 
 * @author brentchesny
 *
 */
public abstract class GFPigConverter {
	
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
				script += convert(bcu.getBasicExpression(), query.getInputs());
				queries.add(bcu.getBasicExpression());
				_outSchemas.add(bcu.getBasicExpression().getOutputSchema());
				
			}
		}
		
		script += storeResults(queries, query.getOutput());
		
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
	private String storeResults(Collection<GFExistentialExpression> queries, Path output) {
		String store = "";
		
		for (GFExistentialExpression expr : queries) {
			//GFExistentialExpression expr = (GFExistentialExpression) gfe;
			
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
