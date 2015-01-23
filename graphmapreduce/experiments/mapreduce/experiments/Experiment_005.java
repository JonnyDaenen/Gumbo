/**
 * Created: 12 Jan 2015
 */
package mapreduce.experiments;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import mapreduce.experiments.profiling.Profiling_q4;
import mapreduce.guardedfragment.executor.hadoop.HadoopExecutor;
import mapreduce.guardedfragment.planner.GFMRPlanner;
import mapreduce.guardedfragment.planner.GFMRPlannerException;
import mapreduce.guardedfragment.planner.partitioner.UnitPartitioner;
import mapreduce.guardedfragment.planner.structures.InputFormat;
import mapreduce.guardedfragment.planner.structures.MRPlan;
import mapreduce.guardedfragment.planner.structures.RelationFileMapping;
import mapreduce.guardedfragment.planner.structures.data.RelationSchema;
import mapreduce.guardedfragment.structure.gfexpressions.GFExistentialExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFExpression;
import mapreduce.guardedfragment.structure.gfexpressions.io.DeserializeException;
import mapreduce.guardedfragment.structure.gfexpressions.io.GFPrefixSerializer;

import org.apache.hadoop.fs.Path;

/**
 * @author jonny
 *
 */
public class Experiment_005 {

	public static void main(String[] args) throws DeserializeException, GFMRPlannerException, IllegalArgumentException,
	InterruptedException {

		// ClassLoader cl = ClassLoader.getSystemClassLoader();
		//
		// URL[] urls = ((URLClassLoader)cl).getURLs();
		//
		// for(URL url: urls){
		// System.out.println(url.getFile());
		// }

		if (args.length == 0) {
			System.out.println("Please provide a input pattern as argument");
			System.exit(0);
		}

		// files & folders
		String input = args[0]; // "./input/q4/1e04/*.rel";
		String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
		String output = "./output/" + Experiment_005.class.getSimpleName() + "/" + timeStamp;
		String scratch = "./scratch/" + Experiment_005.class.getSimpleName() + "/" + timeStamp;

		RelationSchema schemaR = new RelationSchema("R", 5);
		RelationSchema schemaS = new RelationSchema("S", 1);
		RelationFileMapping files = new RelationFileMapping();
		if (input.equals("thinking")) {
			files.addPath(schemaR, new Path("data/R"));
			files.setFormat(schemaR,InputFormat.CSV);
			files.addPath(schemaS, new Path("data/S"));
			files.setFormat(schemaS,InputFormat.CSV);
		} else if (!input.equals("rel")) {
			//	files.setDefaultPath(new Path(input));
			System.out.println("Using csv files.");
			files.addPath(schemaR, new Path("./input/experiments/EXP_005/R"));
			files.setFormat(schemaR,InputFormat.CSV);
			files.addPath(schemaS, new Path("./input/experiments/EXP_005/S"));
			files.setFormat(schemaS,InputFormat.CSV);
		} else {
			//	files.setDefaultPath(new Path(input));
			System.out.println("Using rel files.");
			files.addPath(schemaR, new Path("./input/q4/1e04/R_6e04x4e00_func-seqclone.rel"));
			files.addPath(schemaS, new Path("./input/q4/1e04/S2_3e04x1e00_func-non_mod_2.rel"));
		}
		// query

		Set<String> queries = new HashSet<String>();
		queries.add("#Out(x1,x2,x3,x4,x5)&R(x1,x2,x3,x4,x5)|&S(x1)&!S(x2)&!S(x3)&!S(x4)!S(x5)|&!S(x1)&S(x2)&!S(x3)&!S(x4)!S(x5)|&!S(x1)&!S(x2)&S(x3)&!S(x4)!S(x5)|&!S(x1)&!S(x2)&!S(x3)&S(x4)!S(x5)|&!S(x1)&!S(x2)&!S(x3)&!S(x4)S(x5)|&S(x1)&S(x2)&S(x3)&!S(x4)!S(x5)|&S(x1)&S(x2)&!S(x3)&S(x4)!S(x5)|&S(x1)&S(x2)&!S(x3)&!S(x4)S(x5)|&S(x1)&!S(x2)&S(x3)&S(x4)!S(x5)|&S(x1)&!S(x2)&S(x3)&!S(x4)S(x5)|&S(x1)&!S(x2)&!S(x3)&S(x4)S(x5)|&!S(x1)&S(x2)&S(x3)&S(x4)!S(x5)|&!S(x1)&S(x2)&S(x3)&!S(x4)S(x5)|&!S(x1)&S(x2)&!S(x3)&S(x4)S(x5)|&!S(x1)&!S(x2)&S(x3)&S(x4)S(x5)&S(x1)&S(x2)&S(x3)&S(x4)S(x5)");
		//queries.add("#Out2(x2)&R(x2,x3,x4,x5)&!S2(x2)S2(x2)");

		// parse query
		GFPrefixSerializer parser = new GFPrefixSerializer();

		Collection<GFExpression> gfes1 = parser.deserialize(queries);
		Collection<GFExistentialExpression> gfes = new HashSet<GFExistentialExpression>();
		for (GFExpression gfExpression : gfes1) {
			gfes.add((GFExistentialExpression) gfExpression);
		}

		// plan
		// GFMRPlanner planner = new GFMRPlanner(new HeightPartitioner());
		GFMRPlanner planner = new GFMRPlanner(new UnitPartitioner());
		MRPlan plan = planner.createPlan(gfes, files, new Path(output), new Path(scratch));

		// print plan in text
		System.out.println(plan);

		// print plan in dot
		// System.out.println(plan.toDot());

		// Thread.sleep(15000);
		// execute plan
		long startTime = System.nanoTime();

		HadoopExecutor hExecutor = new HadoopExecutor();
		hExecutor.execute(plan);
		// SparkExecutor sExecutor = new SparkExecutor();
		// sExecutor.execute(plan);

		long endTime = System.nanoTime();

		long duration = (endTime - startTime) / 1000000;
		System.out.println(duration);

	}

}
