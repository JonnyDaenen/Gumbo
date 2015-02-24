/**
 * Created: 22 Aug 2014
 */
package gumbo.engine.spark.converter;

import gumbo.compiler.GumboPlan;
import gumbo.compiler.calculations.BasicGFCalculationUnit;
import gumbo.compiler.calculations.CalculationUnit;
import gumbo.compiler.filemapper.FileManager;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.engine.settings.AbstractExecutorSettings;
import gumbo.engine.spark.mrcomponents.GFSparkMapper1Guard;
import gumbo.engine.spark.mrcomponents.GFSparkMapper1Guarded;
import gumbo.engine.spark.mrcomponents.GFSparkReducer1;
import gumbo.engine.spark.mrcomponents.GFSparkReducer2;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.io.GFPrefixSerializer;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations.GFOperationInitException;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;


/**
 * A converter for transforming a {@link GumboPlan} into a Spark RDD objects.
 * 
 * 
 * @author Jonny Daenen
 * 
 */
public class GumboSparkConverter {


	public class ConversionException extends Exception {

		private static final long serialVersionUID = 1L;


		public ConversionException(String msg) {
			super(msg);
		}


		public ConversionException(String msg, Exception e) {
			super(msg,e);
		}
	}

	private GFPrefixSerializer serializer;
	private FileManager fileManager;

	private String queryName;
	private AbstractExecutorSettings settings;


	private static final Log LOG = LogFactory.getLog(GumboSparkConverter.class);

	/**
	 * 
	 */
	public GumboSparkConverter(String queryName, FileManager fileManager, AbstractExecutorSettings settings) {
		this.queryName = queryName;
		this.fileManager = fileManager;
		this.settings = settings;
		this.serializer = new GFPrefixSerializer();
	}


	public Map<RelationSchema, JavaRDD<String>> convert(CalculationUnitGroup cug) throws ConversionException {


		try {
			// extract file mapping where input paths are made specific
			// TODO also filter out non-related relations
			RelationFileMapping mapping = null; // TODO extractor.extractFileMapping(fileManager);
			// wrapper object for expressions
			ExpressionSetOperations eso;

			eso = new ExpressionSetOperations(extractExpressions(cug),mapping);



			/* ROUND 1: MAP */

			// assemble guard input dataset
			JavaPairRDD<String, String> guardInput = getGuardInput(eso);

			// perform map1 on guard
			JavaPairRDD<String, String> mappedGuard = guardInput.flatMapToPair(new GFSparkMapper1Guard(eso,settings));

			// assemble guarded input dataset
			JavaRDD<String> guardedInput = getGuardedInput(eso);

			// perform map1 on guarded
			JavaPairRDD<String, String> mappedGuarded = guardedInput.flatMapToPair(new GFSparkMapper1Guarded(eso,settings));

			// combine both results (bag union @see JavaRDD#union)
			JavaPairRDD<String, String> union = mappedGuard.union(mappedGuarded);

			/* ROUND 1: REDUCE */

			// group them
			JavaPairRDD<String, Iterable<String>> grouped = union.groupByKey();

			// perform reduce1
			JavaPairRDD<String,String> round1out = grouped.flatMapToPair(new GFSparkReducer1(eso,settings));

			/* ROUND 2: REDUCE */

			// re-add guardInput
			JavaPairRDD<String, String> round2in = round1out.union(guardInput);

			// group again
			JavaPairRDD<String, Iterable<String>> grouped2 = round2in.groupByKey();

			// perform reduce 2
			JavaRDD<Tuple2<String, String>> round2out = grouped2.flatMap(new GFSparkReducer2(eso,settings));

			// split into multiple relations
			Map<RelationSchema, JavaRDD<String>> rddmap = unravel(cug,round2out);

			// FUTURE persist?


			// write output to files
			writeOut(rddmap);


			return rddmap;
		} catch (GFOperationInitException e) {
			LOG.error("Problem during Spark execution: " + e.getMessage());
			e.printStackTrace();
			throw new ConversionException("Problem during Spark execution.", e);
		} 

	}




	/**
	 * Writes out the tuples for each {@link RelationSchema} to the location
	 * specified in the {@link FileManager}.
	 * 
	 * @param rddmap mapping from schemas to RDDs
	 */
	private void writeOut(Map<RelationSchema, JavaRDD<String>> rddmap) {

		RelationFileMapping filemapping = fileManager.getOutFileMapping();

		// for each relationschema (key)
		for (RelationSchema rs : rddmap.keySet()) {
			// lookup output folder in filemanager
			Path[] paths = filemapping.getPaths(rs).toArray(new Path[0]);
			// TODO check if it is only one path
			Path outfile = paths[0];

			// save data
			rddmap.get(rs).saveAsTextFile(outfile.toString());

			// TODO adjust file mapping?
		}

	}


	/**
	 * Separates a set of outputs by their relation schema.
	 * 
	 * @param cug the partition
	 * @param round2out the total output
	 * 
	 * @return a mapping between the output schemas and RDDs
	 */
	private Map<RelationSchema, JavaRDD<String>> unravel(CalculationUnitGroup cug, JavaRDD<Tuple2<String, String>> round2out) {

		HashMap<RelationSchema, JavaRDD<String>> outputMap = new HashMap<>();

		// for each output relationschema in the partition
		for (RelationSchema rs : cug.getOutputRelations()) {
			final String rsstring = rs.toString();

			// filter only output tuples having this rs as the key
			JavaRDD<Tuple2<String, String>> currentRdd = round2out.filter(new Function<Tuple2<String,String>, Boolean>() {
				private static final long serialVersionUID = 1L;
				@Override
				public Boolean call(Tuple2<String, String> v1) throws Exception {
					return v1._1().equals(rsstring);
				}});

			// remove key
			JavaRDD<String> currentValueRdd = currentRdd.map(new Function<Tuple2<String,String>, String>() {
				private static final long serialVersionUID = 1L;

				@Override
				public String call(Tuple2<String, String> v1) throws Exception {
					return v1._2;
				}
			});

			// couple relationschema to RDD
			outputMap.put(rs, currentValueRdd);

		}

		return outputMap;
	}




	/**
	 * @param cug
	 * @return
	 */
	private JavaRDD<String> getGuardedInput(ExpressionSetOperations eso) {
		// TODO implement

		// get guarded CSV files
		Set<Path> csvPaths = eso.getGuardedCsvPaths();

		// read them all

		// augment them with relation annotation


		// get guarded Rel files
		Set<Path> relPaths = eso.getGuardedRelPaths();

		// read them all


		// Take union

		// TODO are guard files already removed by eso?

		return null;
	}


	/**
	 * @param eso
	 * @return
	 */
	private JavaPairRDD<String, String> getGuardInput(ExpressionSetOperations eso) {
		// TODO implement
		
		// OPTIMIZE maybe it is possible to do the mapping here already
		// using specific mapper, suited for the formulas
		// this way, we do not throw everything onto one big pile...

		// get guard CSV files
		Set<Path> csvPaths = eso.getGuardedCsvPaths();

		// read them all

		// augment them with relation annotation


		// get guard Rel files
		Set<Path> relPaths = eso.getGuardedRelPaths();

		// read them all


		// Take union

		// key is file offset + TODO file id
		// value is line

		return null;
	}


	/**
	 * Extracts the GF expressions from the group.
	 * @param cug
	 * @return
	 */
	private Collection<GFExistentialExpression> extractExpressions(CalculationUnitGroup cug) {

		Set<GFExistentialExpression> set = new HashSet<>();

		for(CalculationUnit cu : cug) {
			BasicGFCalculationUnit bcu = (BasicGFCalculationUnit) cu;
			set.add(bcu.getBasicExpression());
		}
		return set;
	}

	/**
	 * @param cug
	 * @return
	 */
	private String getName(CalculationUnitGroup cug, int round) {
		String name = "";
		for (RelationSchema rs : cug.getOutputRelations() ) {
			// TODO sort
			name += rs.getName()+"+";
		}
		return queryName + "_"+ name + "_"+round;
	}




}
