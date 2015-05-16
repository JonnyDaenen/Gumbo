/**
 * Created: 12 Jan 2015
 */
package gumbo.experiments;

import gumbo.compiler.filemapper.InputFormat;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.input.GumboQuery;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFExpression;
import gumbo.structures.gfexpressions.io.DeserializeException;
import gumbo.structures.gfexpressions.io.GFPrefixSerializer;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author jonny
 *
 */
public class Experiment_009 extends Experiment {


	public GumboQuery getQuery(String[] args) {

		// ClassLoader cl = ClassLoader.getSystemClassLoader();
		//
		// URL[] urls = ((URLClassLoader)cl).getURLs();
		//
		// for(URL url: urls){
		// System.out.println(url.getFile());
		// }

		if (args.length < 2) {
			System.out.println("Please provide a input pattern as argument, together with the arity");
			System.exit(0);
		}

		// files & folders
		String input = args[0]; // "./input/q4/1e04/*.rel";
		int arity = Integer.parseInt(args[1]); // "./input/q4/1e04/*.rel";
		String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
		String output = "./output/" + Experiment_009.class.getSimpleName() + "/" + timeStamp;
		String scratch = "./scratch/" + Experiment_009.class.getSimpleName() + "/" + timeStamp;

		RelationSchema schemaR = new RelationSchema("R", arity);
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
			files.addPath(schemaR, new Path("./input/experiments/EXP_008/R"));
			files.setFormat(schemaR,InputFormat.CSV);
			files.addPath(schemaS, new Path("./input/experiments/EXP_008/S"));
			files.setFormat(schemaS,InputFormat.CSV);
		} else {
			//	files.setDefaultPath(new Path(input));
			System.out.println("Using rel files.");
			files.addPath(schemaR, new Path("./input/q4/1e04/R_6e04x4e00_func-seqclone.rel"));
			files.addPath(schemaS, new Path("./input/q4/1e04/S2_3e04x1e00_func-non_mod_2.rel"));
		}
		// query

		Set<String> queries = new HashSet<String>();
		switch(arity) {
		case 1:
			queries.add("#Out(x1)&R(x1)!S(x1)");
			break;
		case 2:
			queries.add("#Out(x1,x2)&R(x1,x2)&!S(x1)!S(x2)");
			break;
		case 3:
			queries.add("#Out(x1,x2,x3)&R(x1,x2,x3)&!S(x1)&!S(x2)!S(x3)");
			break;
		case 4:
			queries.add("#Out(x1,x2,x3,x4)&R(x1,x2,x3,x4)&!S(x1)&!S(x2)&!S(x3)!S(x4)");
			break;
		case 5:
			queries.add("#Out(x1,x2,x3,x4,x5)&R(x1,x2,x3,x4,x5)&!S(x1)&!S(x2)&!S(x3)&!S(x4)!S(x5)");
			break;
		case 6:
			queries.add("#Out(x1,x2,x3,x4,x5,x6)&R(x1,x2,x3,x4,x5,x6)&!S(x1)&!S(x2)&!S(x3)&!S(x4)&!S(x5)!S(x6)");
			break;
		case 7:
			queries.add("#Out(x1,x2,x3,x4,x5,x6,x7)&R(x1,x2,x3,x4,x5,x6,x7)&!S(x1)&!S(x2)&!S(x3)&!S(x4)&!S(x5)&!S(x6)!S(x7)");
			break;
		case 8:
			queries.add("#Out(x1,x2,x3,x4,x5,x6,x7,x8)&R(x1,x2,x3,x4,x5,x6,x7,x8)&!S(x1)&!S(x2)&!S(x3)&!S(x4)&!S(x5)&!S(x6)&!S(x7)!S(x8)");
			break;
		case 9:
			queries.add("#Out(x1,x2,x3,x4,x5,x6,x7,x8,x9)&R(x1,x2,x3,x4,x5,x6,x7,x8,x9)&!S(x1)&!S(x2)&!S(x3)&!S(x4)&!S(x5)&!S(x6)&!S(x7)&!S(x8)!S(x9)");
			break;
		case 10:
			queries.add("#Out(x1,x2,x3,x4,x5,x6,x7,x8,x9,x10)&R(x1,x2,x3,x4,x5,x6,x7,x8,x9,x10)&!S(x1)&!S(x2)&!S(x3)&!S(x4)&!S(x5)&!S(x6)&!S(x7)&!S(x8)&!S(x9)!S(x10)");
			break;
		default:
			System.exit(1);
		}

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
		int res = ToolRunner.run(new Configuration(), new ExperimentRunner(Experiment_009.class), args);

		System.exit(res);
	}

}
