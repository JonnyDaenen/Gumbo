/**
 * Created: 24 Apr 2014
 */
package mapreduce.guardedfragment;

import static org.junit.Assert.fail;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import mapreduce.guardedfragment.executor.hadoop.HadoopExecutor;
import mapreduce.guardedfragment.planner.partitioner.HeightPartitioner;
import mapreduce.guardedfragment.planner.structures.MRPlan;
import mapreduce.guardedfragment.planner.structures.RelationFileMapping;
import mapreduce.guardedfragment.structure.gfexpressions.GFExistentialExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFExpression;
import mapreduce.guardedfragment.structure.gfexpressions.io.GFPrefixSerializer;

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
		
		Set<String> data = new HashSet<String>();
		data.add("G(1,2,3)");
		data.add("S(1,3)");

	
		Set<String> result = doTest(gfe, data,"O1");
		
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

	
		Set<String> result = doTest(gfe, data,"O2");
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
		
		Set<String> data = new HashSet<String>();
		data.add("G(1,1)");
		data.add("G(1,2)");
		data.add("G(2,1)");

	
		Set<String> result = doTest(gfe, data,"O2");
//		System.out.println(result);
		
		System.out.println(result);
		if(result.size() != 1 || result.contains("O(1,2)") || result.contains("O(1,1)") || !result.contains("O(2,1)"))
			fail("expected {O(2,1)}");
	}

	private Set<String> doTest(GFExpression gfe, Set<String> data, String relname) throws Exception {
		
		// create tmp folders
		File input = folder.newFolder("input");
		File scratch = folder.newFolder("scratch");
		File output = folder.newFolder("output");

//		File output = new File("/Users/jonny/Code/hadoop/workspace/graphmapreduce/output/testnow");

		// create input file with data
		File inputfile = new File(input.getAbsolutePath() + "/input.txt");
		Files.write(inputfile.toPath(), data, StandardCharsets.US_ASCII, StandardOpenOption.CREATE_NEW);


		RelationFileMapping rfm = new RelationFileMapping();
		rfm.setDefaultPath(new Path(input.getAbsolutePath()));
		
		// initialize planner
//		GFMRPlanner planner = new GFMRPlanner(input.getAbsolutePath(), output.getAbsolutePath()+"/test",
//				scratch.getAbsolutePath());

		// create plan and execute
//		MRPlan plan = planner.convert(gfe.getSubExistentialExpression(1));
//		plan.execute();
		
		mapreduce.guardedfragment.planner.GFMRPlanner planner2 = new mapreduce.guardedfragment.planner.GFMRPlanner(new HeightPartitioner());
		MRPlan plan2 = planner2.createPlan((GFExistentialExpression)gfe, rfm , new Path(output.getAbsolutePath()), new Path(scratch.getAbsolutePath()));
//		plan2.execute();
		
		HadoopExecutor executor = new HadoopExecutor();
		executor.execute(plan2);
		
		// TODO we need a method to request the output files/directory of a relation
		
		System.out.println(plan2.getOutputFolder());
		
		// read output file
//		File outputfile = new File(output.getAbsolutePath() + "/test/part-r-00000");
		File outputfile = new File(output.getAbsolutePath() + "/OUT_1_"+relname+"/"+relname+"/"+relname+"-r-00000");
		List<String> out = Files.readAllLines(outputfile.toPath(), StandardCharsets.US_ASCII);
		
		return new HashSet<String>(out);
	}

	
}
