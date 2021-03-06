/**
 * Created: 10 Sep 2014
 */
package gumbo.experiments.profiling;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.Path;

import gumbo.compiler.filemapper.InputFormat;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.experiments.Experiment;
import gumbo.input.GumboQuery;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFExpression;
import gumbo.structures.gfexpressions.io.DeserializeException;
import gumbo.structures.gfexpressions.io.GFPrefixSerializer;

/**
 * @author Jonny Daenen
 * 
 */
public class Profiling_q4 extends Experiment {

	public GumboQuery getQuery(String[] args) {

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
		String output = "./output/" + Profiling_q4.class.getSimpleName() + "/" + timeStamp;
		String scratch = "./scratch/" + Profiling_q4.class.getSimpleName() + "/" + timeStamp;

		RelationSchema schemaR = new RelationSchema("R", 4);
		RelationSchema schemaS2 = new RelationSchema("S2", 1);
		RelationSchema schemaS3 = new RelationSchema("S3", 1);
		RelationSchema schemaS4 = new RelationSchema("S4", 1);
		RelationSchema schemaS5 = new RelationSchema("S5", 1);
		RelationFileMapping files = new RelationFileMapping();
		if (input.equals("cloudera")) {
			files.addPath(schemaR, new Path("./data/q4/1e06/R_6e06x4e00_func-seqclone.rel"));
			files.addPath(schemaS2, new Path("./data/q4/1e06/S2_3e06x1e00_func-non_mod_2.rel"));
			files.addPath(schemaS3, new Path("./data/q4/1e06/S3_4e06x1e00_func-non_mod_3.rel"));
			files.addPath(schemaS4, new Path("./data/q4/1e06/S4_4e06x1e00_func-non_mod_4.rel"));
			files.addPath(schemaS5, new Path("./data/q4/1e06/S5_5e06x1e00_func-non_mod_5.rel"));
		} else if (input.equals("thinking")) {
			files.addPath(schemaR, new Path("dataG/R_6e7"));
			files.addPath(schemaS2, new Path("dataG/S2_3e7"));
			files.addPath(schemaS3, new Path("dataG/S3_2e7"));
			files.addPath(schemaS4, new Path("dataG/S4_15e6"));
			files.addPath(schemaS5, new Path("dataG/S5_12e6"));
		} else if (!input.equals("rel")) {
//			files.setDefaultPath(new Path(input));
			System.out.println("Using csv files.");
			files.addPath(schemaR, new Path("./input/q4/1e04/R_6e04x4e00_func-seqclone.csv"));
			files.setFormat(schemaR,InputFormat.CSV);
			files.addPath(schemaS2, new Path("./input/q4/1e04/S2_3e04x1e00_func-non_mod_2.csv"));
			files.setFormat(schemaS2,InputFormat.CSV);
			files.addPath(schemaS3, new Path("./input/q4/1e04/S3_4e04x1e00_func-non_mod_3.csv"));
			files.setFormat(schemaS3,InputFormat.CSV);
			files.addPath(schemaS4, new Path("./input/q4/1e04/S4_4e04x1e00_func-non_mod_4.csv"));
			files.setFormat(schemaS4,InputFormat.CSV);
			files.addPath(schemaS5, new Path("./input/q4/1e04/S5_5e04x1e00_func-non_mod_5.csv"));
			files.setFormat(schemaS5,InputFormat.CSV);
		} else {
//			files.setDefaultPath(new Path(input));
			System.out.println("Using rel files.");
			files.addPath(schemaR, new Path("./input/q4/1e04/R_6e04x4e00_func-seqclone.rel"));
			files.addPath(schemaS2, new Path("./input/q4/1e04/S2_3e04x1e00_func-non_mod_2.rel"));
			files.addPath(schemaS3, new Path("./input/q4/1e04/S3_4e04x1e00_func-non_mod_3.rel"));
			files.addPath(schemaS4, new Path("./input/q4/1e04/S4_4e04x1e00_func-non_mod_4.rel"));
			files.addPath(schemaS5, new Path("./input/q4/1e04/S5_5e04x1e00_func-non_mod_5.rel"));
		}
		// query

		Set<String> queries = new HashSet<String>();
		queries.add("#Out(x2)&R(x2,x3,x4,x5)&!S2(x2)&!S3(x3)&!S4(x4)!S5(x5)");
//		queries.add("#Out2(x2)&R(x2,x3,x4,x5)&!S2(x2)S2(x2)");

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

}
