/**
 * Created: 12 Jan 2015
 */
package gumbo.experiments;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import gumbo.compiler.filemapper.InputFormat;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.input.GumboQuery;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFExpression;
import gumbo.structures.gfexpressions.io.DeserializeException;
import gumbo.structures.gfexpressions.io.GFPrefixSerializer;

/**
 * @author jonny
 *
 */
public class Experiment_005 extends Experiment {



	public Experiment_005() {
		// TODO Auto-generated constructor stub
	}
	
	/* (non-Javadoc)
	 * @see mapreduce.experiments.Experiment#getQuery(java.lang.String[])
	 */
	@Override
	public GumboQuery getQuery(String[] args) {
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

		Collection<GFExpression> gfes1;
		try {
			gfes1 = parser.deserialize(queries);
			return new GumboQuery(this.getClass().getSimpleName(),gfes1, files, new Path(output), new Path(scratch));
		} catch (DeserializeException e) {
			e.printStackTrace();
		}
		
		return null;

		
		
	}
	
	public static void main(String[] args) throws Exception {
		// Let ToolRunner handle generic command-line options 
		int res = ToolRunner.run(new Configuration(), new ExperimentRunner(Experiment_005.class), args);

		System.exit(res);
	}

}
