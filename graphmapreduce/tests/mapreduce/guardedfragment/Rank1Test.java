/**
 * Created: 24 Apr 2014
 */
package mapreduce.guardedfragment;

import static org.junit.Assert.fail;
import gumbo.compiler.GFCompiler;
import gumbo.compiler.GumboPlan;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.compiler.partitioner.HeightPartitioner;
import gumbo.compiler.structures.MRPlan;
import gumbo.compiler.structures.data.RelationSchema;
import gumbo.engine.hadoop.HadoopEngine;
import gumbo.engine.hadoop.HadoopExecutor;
import gumbo.guardedfragment.gfexpressions.GFExistentialExpression;
import gumbo.guardedfragment.gfexpressions.GFExpression;
import gumbo.guardedfragment.gfexpressions.io.GFPrefixSerializer;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * @author Jonny Daenen
 * 
 */
public class Rank1Test {

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	GFPrefixSerializer serializer;

	@Before
	public void setUp() throws Exception {
		serializer = new GFPrefixSerializer();
	}

	@After
	public void tearDown() throws Exception {
	}

	/**
	 * When multiple guarded match the guard tuple.
	 */
	@Test
	public void Reducer1Multimatch() throws Exception {
		GFExpression gfe = serializer.deserialize("#O(x)&G(x,y,z)&S(x,z)!S(y,z)");

		Set<String> dataS = new HashSet<String>();
		Set<String> dataG = new HashSet<String>();
		dataG.add("G(1,2,3)");
		dataS.add("S(1,3)");


		Set<String> result = doTest(gfe, dataG,dataS,"O");

		//		System.out.println(result);

		if(result.size() != 1 || !result.contains("O(1)"))
			fail("expected {O(1)}");

	}

	/**
	 * Test what happens when the guard appears as guarded as well.
	 */
	@Test
	public void guardAndGuarded() throws Exception {

		GFExpression gfe = serializer.deserialize("#O(x,y)&G(x,y)G(x,x)");

		Set<String> data = new HashSet<String>();
		data.add("G(1,1)");
		data.add("G(1,2)");


		Set<String> result = doTest(gfe, data,null,"O");
		//		System.out.println(result);

		if(result.size() != 2 || !result.contains("O(1,2)") || !result.contains("O(1,1)"))
			fail("expected {O(1,1), O(1,2)}");
	}

	/**
	 * Test what happens when the guard appears as guarded as well.
	 */
	@Test
	public void guardAndGuarded2() throws Exception {

		GFExpression gfe = serializer.deserialize("#O(x,y)&G(x,y)!G(x,x)");

		Set<String> dataG = new HashSet<String>();
		dataG.add("G(1,1)");
		dataG.add("G(1,2)");
		dataG.add("G(2,1)");


		Set<String> result = doTest(gfe, dataG, null, "O");
		//		System.out.println(result);

		System.out.println(result);
		if(result.size() != 1 || result.contains("O(1,2)") || result.contains("O(1,1)") || !result.contains("O(2,1)"))
			fail("expected {O(2,1)}");
	}

	private Set<String> doTest(GFExpression gfe, Set<String> dataG, Set<String> dataS, String relname) throws Exception {

		// create tmp folders
		File inputG = folder.newFolder("inputG");
		File inputS = folder.newFolder("inputS");
		File scratch = folder.newFolder("scratch");
		File output = folder.newFolder("output");

		//		File output = new File("/Users/jonny/Code/hadoop/workspace/graphmapreduce/output/testnow");

		// create input file with data
		File inputfileG = new File(inputG.getAbsolutePath() + "/inputG.txt");
		File inputfileS = new File(inputS.getAbsolutePath() + "/inputS.txt");
		if (dataG != null)
			Files.write(inputfileG.toPath(), dataG, StandardCharsets.US_ASCII, StandardOpenOption.CREATE_NEW);
		if (dataS != null)
			Files.write(inputfileS.toPath(), dataS, StandardCharsets.US_ASCII, StandardOpenOption.CREATE_NEW);


		RelationFileMapping rfm = new RelationFileMapping();
		rfm.setDefaultPath(new Path(inputG.getAbsolutePath()));
		rfm.addPath(new RelationSchema("G",2), new Path(inputfileG.getAbsolutePath()));
		rfm.addPath(new RelationSchema("G",3), new Path(inputfileG.getAbsolutePath()));
		rfm.addPath(new RelationSchema("S",1), new Path(inputfileS.getAbsolutePath()));
		rfm.addPath(new RelationSchema("S",2), new Path(inputfileS.getAbsolutePath()));

		// initialize planner
		//		GFMRPlanner planner = new GFMRPlanner(input.getAbsolutePath(), output.getAbsolutePath()+"/test",
		//				scratch.getAbsolutePath());

		// create plan and execute
		//		MRPlan plan = planner.convert(gfe.getSubExistentialExpression(1));
		//		plan.execute();

		GFCompiler compiler = new GFCompiler(new HeightPartitioner());
		GumboPlan plan = compiler.createPlan((GFExistentialExpression)gfe, rfm, new Path(output.getAbsolutePath()), new Path(scratch.getAbsolutePath()));
		
		HadoopEngine engine = new HadoopEngine();
		engine.executePlan(plan);

		// TODO we need a method to request the output files/directory of a relation

		System.out.println(plan);

		// read output file
		//		File outputfile = new File(output.getAbsolutePath() + "/test/part-r-00000");
		File outputfile = new File(output.getAbsolutePath() + "/OUT_0_"+relname+"/"+relname+"/"+relname+"-r-00000");
		List<String> out = Files.readAllLines(outputfile.toPath(), StandardCharsets.US_ASCII);

		return new HashSet<String>(out);
	}


}
