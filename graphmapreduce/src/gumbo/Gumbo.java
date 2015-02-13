/**
 * Created: 09 Feb 2015
 */
package gumbo;

import gumbo.compiler.GFCompiler;
import gumbo.compiler.GumboPlan;
import gumbo.compiler.filemapper.InputFormat;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.engine.hadoop.HadoopEngine;
import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
import gumbo.engine.settings.ExecutorSettings;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFExpression;
import gumbo.structures.gfexpressions.io.GFPrefixSerializer;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Jonny Daenen
 *
 */
public class Gumbo extends Configured implements Tool {


	public int run(String[] args) throws Exception {
		// Configuration processed by ToolRunner
		Configuration conf = getConf(); 
		
		HadoopExecutorSettings settings = new HadoopExecutorSettings(conf);
		settings.loadDefaults();
		settings.turnOffOptimizations();
		
		for (Entry<String, String> entry: conf) {
			if(entry.getKey().contains("gumbo")) {
			System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
			}
		}



		// parse query
		GFPrefixSerializer parser = new GFPrefixSerializer();
		String query = "{#Out(x1,x2,x3,x4,x5,x6,x7,x8,x9,x10)&R(x1,x2,x3,x4,x5,x6,x7,x8,x9,x10)&!S(x1)&!S(x2)&!S(x3)&!S(x4)&!S(x5)&!S(x6)&!S(x7)&!S(x8)&!S(x9)!S(x10)}";
		Set<GFExpression> expressions = parser.deserializeSet(query);

		// IO paths
		RelationFileMapping input = new RelationFileMapping();
		input.addPath(new RelationSchema("R",10), new Path("input/experiments/EXP_008/R"), InputFormat.CSV);
		input.addPath(new RelationSchema("S",1), new Path("input/experiments/EXP_008/S"), InputFormat.CSV);

		String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
		Path output = new Path("output/gumbov2/"+timeStamp);
		Path scratch = new Path("scratch/gumbov2/"+timeStamp);



		GFCompiler compiler = new GFCompiler();


		String name = "Gumbov2Query";
		GumboPlan plan = compiler.createPlan(name, expressions, input, output, scratch);

		System.out.println(plan);

		HadoopEngine engine = new HadoopEngine();
		engine.executePlan(plan,conf);

		return 0;
	}


	public static void main(String[] args) throws Exception {
		// Let ToolRunner handle generic command-line options 
		int res = ToolRunner.run(new Configuration(), new Gumbo(), args);

		System.exit(res);
	}



}
