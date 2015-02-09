/**
 * Created: 09 Feb 2015
 */
package gumbo;

import java.util.Set;

import gumbo.compiler.GFCompiler;
import gumbo.compiler.GumboPlan;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.compiler.structures.data.RelationSchema;
import gumbo.guardedfragment.gfexpressions.GFExpression;
import gumbo.guardedfragment.gfexpressions.io.GFPrefixSerializer;

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

		// parse query
		GFPrefixSerializer parser = new GFPrefixSerializer();
		String query = "{#Out(x1,x2,x3,x4,x5,x6,x7,x8,x9,x10)&R(x1,x2,x3,x4,x5,x6,x7,x8,x9,x10)&!S(x1)&!S(x2)&!S(x3)&!S(x4)&!S(x5)&!S(x6)&!S(x7)&!S(x8)&!S(x9)!S(x10)}";
		Set<GFExpression> expressions = parser.deserializeSet(query);
		
		RelationFileMapping input = new RelationFileMapping();
		input.addPath(new RelationSchema("R",10), new Path("input/experiments/EXP_008/R"));
		input.addPath(new RelationSchema("S",1), new Path("input/experiments/EXP_008/S"));
		Path output = new Path("output/gumbov2");
		Path scratch = new Path("scratch/gumbov2");
		
		GFCompiler compiler = new GFCompiler();
		GumboPlan plan = compiler.createPlan(expressions, input, output, scratch);

		System.out.println(plan);
		
		return 0;
	}

	/**
	 * @param string
	 * @return
	 */
	private String readFile(String string) {
		// TODO Auto-generated method stub
		return null;
	}

	public static void main(String[] args) throws Exception {
		// Let ToolRunner handle generic command-line options 
		int res = ToolRunner.run(new Configuration(), new Gumbo(), args);

		System.exit(res);
	}



}
