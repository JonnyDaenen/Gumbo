/**
 * Created: 22 Aug 2014
 */
package gumbo.engine.spark.converter;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import gumbo.compiler.GumboPlan;
import gumbo.compiler.calculations.BasicGFCalculationUnit;
import gumbo.compiler.calculations.CalculationUnit;
import gumbo.compiler.filemapper.FileManager;
import gumbo.compiler.filemapper.InputFormat;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.engine.general.settings.AbstractExecutorSettings;
import gumbo.engine.general.utils.FileMappingExtractor;
import gumbo.engine.spark.mrcomponents.GFSparkMapper1Guard;
import gumbo.engine.spark.mrcomponents.GFSparkMapper1Guarded;
import gumbo.engine.spark.mrcomponents.GFSparkReducer1;
import gumbo.engine.spark.mrcomponents.GFSparkReducer2;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations.GFOperationInitException;
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

	private FileManager fileManager;
	private FileMappingExtractor extractor;

	private String queryName;
	private AbstractExecutorSettings settings;

	private JavaSparkContext ctx;
	private Configuration hadoopConf;

	private static final Log LOG = LogFactory.getLog(GumboSparkConverter.class);

	/**
	 * @param conf 
	 * 
	 */
	public GumboSparkConverter(String queryName, FileManager fileManager, AbstractExecutorSettings settings, JavaSparkContext ctx, Configuration conf) {
		this.queryName = queryName;
		this.fileManager = fileManager;
		this.settings = settings;
		this.ctx = ctx;
		this.extractor = new FileMappingExtractor();
		this.hadoopConf = conf;
	}


	public Map<RelationSchema, JavaRDD<String>> convert(CalculationUnitGroup cug) throws ConversionException {


		try {
			// extract file mapping where input paths are made specific
			// FUTURE also filter out non-related relations
			RelationFileMapping mapping = extractor.extractFileMapping(fileManager);
			// wrapper object for expressions
			ExpressionSetOperations eso;

			eso = new ExpressionSetOperations(extractExpressions(cug),extractExpressions(cug),mapping); // FIXME second group is wrong



			/* ROUND 1: MAP */
			/* ------------ */

			// assemble guard input dataset

			LOG.info("I/O: loading guard");
			JavaPairRDD<String, String> guardInput = getGuardInput(eso);
//			guardInput.saveAsTextFile("debug/2");

			// perform map1 on guard
			LOG.info("Map1: performing map1 on guard");
			JavaPairRDD<String, String> mappedGuard = guardInput.flatMapToPair(new GFSparkMapper1Guard(eso,settings));
//			mappedGuard.saveAsTextFile("debug/3");
			LOG.info("Map1: finished map1 on guard");

			// assemble guarded input dataset
			LOG.info("I/O: loading guarded");
			JavaRDD<String> guardedInput = getGuardedInput(eso);
//			guardedInput.saveAsTextFile("debug/5");

			// perform map1 on guarded
			LOG.info("Map1: performing map1 on guarded");
			JavaPairRDD<String, String> mappedGuarded = guardedInput.flatMapToPair(new GFSparkMapper1Guarded(eso,settings));
//			mappedGuarded.saveAsTextFile("debug/6");
			LOG.info("Map1: finished map1 on guarded");

			// combine both results (bag union @see JavaRDD#union)
			LOG.info("Map1: combining output");
			JavaPairRDD<String, String> union = mappedGuard.union(mappedGuarded);
//			union.saveAsTextFile("debug/7");
			LOG.info("Map1: combined output");



			/* ROUND 1: REDUCE */
			/* --------------- */

			// group them
			LOG.info("Reduce1: starting grouping");
			JavaPairRDD<String, Iterable<String>> grouped = union.groupByKey();
//			grouped.saveAsTextFile("debug/8");
			LOG.info("Reduce1: finished grouping");

			// perform reduce1
			LOG.info("Reduce1: starting reduce1");
			JavaPairRDD<String,String> round1out = grouped.flatMapToPair(new GFSparkReducer1(eso,settings));
//			round1out.saveAsTextFile("debug/9");
			LOG.info("Reduce1: finished reduce1");



			/* ROUND 2: REDUCE */
			/* --------------- */

			// re-add guardInput
			LOG.info("Reduce2: starting union");
			JavaPairRDD<String, String> round2in = round1out.union(guardInput);
//			round2in.saveAsTextFile("debug/10");
			LOG.info("Reduce2: finished union");

			// group again
			LOG.info("Reduce2: starting grouping");
			JavaPairRDD<String, Iterable<String>> grouped2 = round2in.groupByKey();
//			grouped2.saveAsTextFile("debug/11");
			LOG.info("Reduce2: finished grouping");

			// perform reduce 2
			LOG.info("Reduce2: starting reduce2");
			JavaRDD<Tuple2<String, String>> round2out = grouped2.flatMap(new GFSparkReducer2(eso,settings));
//			round2out.saveAsTextFile("debug/12");
			LOG.info("Reduce2: starting reduce2");

			// split into multiple relations
			LOG.info("Reduce2: unravelling output");
			Map<RelationSchema, JavaRDD<String>> rddmap = unravel(cug,round2out);

			// FUTURE persist?


			// write output to files
			LOG.info("Reduce2: writing output");
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
			JavaRDD<Tuple2<String, String>> currentRdd = round2out.filter(new Unravel1(rsstring));

			// remove key
			JavaRDD<String> currentValueRdd = currentRdd.map(new Unravel2());

			// couple relationschema to RDD
			outputMap.put(rs, currentValueRdd);

		}

		return outputMap;
	}

	static class Unravel1 implements  Function<Tuple2<String,String>, Boolean> {
		private static final long serialVersionUID = 1L;
		private String schema;

		public Unravel1(String schema) {
			this.schema = schema;
		}

		@Override
		public Boolean call(Tuple2<String, String> v1) throws Exception {
			return v1._1().equals(schema); 
		}}

	static class Unravel2 implements Function<Tuple2<String,String>, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String call(Tuple2<String, String> v1) throws Exception {
			return v1._2;
		}
	}



	/**
	 * Collects all guarded input tuples into one RDD.
	 * 
	 * @param eso
	 * 
	 * @return
	 */
	private JavaRDD<String> getGuardedInput(ExpressionSetOperations eso) {

		JavaRDD<String> totalInput = null; // ctx.emptyRDD();

		// get guarded CSV files
		Set<GFAtomicExpression> guardeds = eso.getGuardedsAll();
		Set<Path> consideredPaths = new HashSet<>();

		// for each guarded atom
		for (GFAtomicExpression atom : guardeds) {

			// get its schema
			RelationSchema schema = atom.getRelationSchema();

			// get the relational paths
			for (Path path : eso.getFileMapping().getPaths(schema)){

				if (!consideredPaths.contains(path)) {
					// load them
					JavaRDD<String> newData = ctx.textFile(path.toString());

					// add relation info when it is a CSV-file
					if (eso.getFileMapping().getInputFormat(schema) == InputFormat.CSV) {
						newData = addSchema(schema, newData);
					}

					// put it together with previous data
					if (totalInput == null)
						totalInput = newData;
					else
						totalInput = totalInput.union(newData);

					consideredPaths.add(path);
				}
			}



		}

		return totalInput;
	}


	/**
	 * Adds schema information to the csv value in a String RDD.
	 * 
	 * @return the RDD augmented with schema information
	 */
	private JavaRDD<String> addSchema(RelationSchema schema, JavaRDD<String> input) {
		final String name = schema.getName();
		JavaRDD<String> output = input.map(new F3(name));
		return output;
	}

	static class F3 implements Function<String, String> {
		private static final long serialVersionUID = 1L;

		private String name;

		public F3(String name) {
			this.name = name;
		}

		@Override
		public String call(String v1) throws Exception {
			return  name+"(" + v1 + ")"; // FIXME
		}
	}

	/**
	 * Adds schema information to the csv value in a key-value pair RDD.
	 * 
	 * @return the RDD augmented with schema information
	 */
	private JavaPairRDD<String, String> addSchema(RelationSchema schema, JavaPairRDD<String, String> input) {
		final String name = schema.getName();
		JavaPairRDD<String, String> output = input.mapToPair(new F2(name));
		return output;
	}

	static class F2 implements PairFunction<Tuple2<String,String>, String, String> {
		private static final long serialVersionUID = 1L;
		private String name;

		public F2(String name) {
			this.name = name;
		}
		@Override
		public Tuple2<String, String> call(Tuple2<String, String> t) throws Exception {
			//			System.out.println(t);
			return new Tuple2<String, String>(t._1, name+"(" + t._2 + ")");// OPTIMIZE do we need to create a new object? maybe reuse parameter?
		}// FIXME
	};



	/**
	 * @param eso
	 * @return
	 */
	private JavaPairRDD<String, String> getGuardInput(ExpressionSetOperations eso) {
		// TODO implement

		// OPTIMIZE maybe it is possible to do the mapping here already
		// using specific mapper, suited for the formulas
		// this way, we do not throw everything onto one big pile...


		JavaPairRDD<String, String> totalInput = null; // CLEAN how to initialize this?

		// get guarded CSV files
		Set<GFAtomicExpression> guards = eso.getGuardsAll();

		// for each guarded atom
		for (GFAtomicExpression atom : guards) {

			// get its schema
			RelationSchema schema = atom.getRelationSchema();

			// get the paths
			for (Path path : eso.getFileMapping().getPaths(schema)){

				// load them
				JavaPairRDD<String, String> newData = readGuardFile(path);

				// add relation info when it is a CSV-file
				if (eso.getFileMapping().getInputFormat(schema) == InputFormat.CSV) {
					newData = addSchema(schema, newData);
				}

				// put it together with previous data
				if (totalInput == null) {
					totalInput = newData;
				} else {
					totalInput = totalInput.union(newData);
				}
			}



		}

		return totalInput;

	}




	/**
	 * Reads a guard file and outputs (byte offset,line) tuples.
	 * The byte offset indicates where in the file the line starts
	 * 
	 * @return contents of a guard file together with file offset
	 */
	private JavaPairRDD<String,String> readGuardFile(Path p) {

		final String fileID = p.getName(); // FIXME we need real file id! this is too long


		// load using hadoop
		JavaPairRDD<LongWritable, Text> rdd = ctx.newAPIHadoopFile(p.toString(), TextInputFormat.class, LongWritable.class, Text.class, hadoopConf);

		// convert to Java types
		JavaPairRDD<String, String> rdd2 = rdd.mapToPair(new Hadoop2JavaConverter(fileID));

		return rdd2;
	}

	static class Hadoop2JavaConverter implements PairFunction<Tuple2<LongWritable,Text>, String, String> {
		private static final long serialVersionUID = 1L;
		private String fileID;

		public Hadoop2JavaConverter(String fileID) {
			this.fileID = fileID;
		}

		@Override
		public Tuple2<String, String> call(Tuple2<LongWritable, Text> t) throws Exception {
			return new Tuple2<String, String>(t._1.get() + fileID ,new String(t._2.getBytes(),0,t._2.getLength()));
		}
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
