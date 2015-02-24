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
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;


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


	public JavaRDD<String> convert(CalculationUnitGroup cug) throws ConversionException {


		try {
			// extract file mapping where input paths are made specific
			// TODO also filter out non-related relations
			RelationFileMapping mapping = null; // TODO extractor.extractFileMapping(fileManager);
			// wrapper object for expressions
			ExpressionSetOperations eso;

			eso = new ExpressionSetOperations(extractExpressions(cug),mapping);



			/* ROUND 1 */

			// assemble guard input dataset
			JavaRDD<String> guardInput = getGuardInput(cug);
			// perform map1 on guard
			JavaPairRDD<String, String> mappedGuard = guardInput.flatMapToPair(new GFSparkMapper1Guard(eso,settings));

			// assemble guarded input dataset
			JavaRDD<String> guardedInput = getGuardedInput(cug);

			// perform map1 on guarded
			JavaPairRDD<String, String> mappedGuarded = guardedInput.flatMapToPair(new GFSparkMapper1Guarded(eso,settings));

			// combine both results (bag union @see JavaRDD#union)
			JavaPairRDD<String, String> union = mappedGuard.union(mappedGuarded);

			// group them
			JavaPairRDD<String, Iterable<String>> grouped = union.groupByKey();

			// perform reduce1
			JavaPairRDD<String,String> round1out = grouped.flatMapToPair(new GFSparkReducer1(eso,settings));

			/* ROUND 2 */

			// group again
			JavaPairRDD<String, Iterable<String>> grouped2 = round1out.groupByKey();

			// perform reduce 2, 
			// TODO maybe return multiple RDDS?
			JavaRDD<String> round2out = grouped2.flatMap(new GFSparkReducer2(eso,settings));

			// TODO split into multiple relations

			// FUTURE persist?


			// write output to files
			//		TODO round2out.saveAsTextFile(outfile.toString());


			return round2out;
		} catch (GFOperationInitException e) {
			LOG.error("Problem during Spark execution: " + e.getMessage());
			e.printStackTrace();
			throw new ConversionException("Problem during Spark execution.", e);
		} 

	}




	/**
	 * @param cug
	 * @return
	 */
	private JavaRDD<String> getGuardedInput(CalculationUnitGroup cug) {
		// TODO implement
		return null;
	}


	/**
	 * @param cug
	 * @return
	 */
	private JavaRDD<String> getGuardInput(CalculationUnitGroup cug) {
		// TODO implement
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
