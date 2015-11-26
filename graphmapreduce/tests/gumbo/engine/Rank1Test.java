/**
 * Created: 24 Apr 2014
 */
package gumbo.engine;

import static org.junit.Assert.fail;
import gumbo.compiler.GFCompiler;
import gumbo.compiler.GumboPlan;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.compiler.partitioner.HeightPartitioner;
import gumbo.engine.general.settings.AbstractExecutorSettings;
import gumbo.engine.hadoop.HadoopEngine;
import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
import gumbo.input.GumboQuery;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.GFExpression;
import gumbo.structures.gfexpressions.io.GFPrefixSerializer;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * @author Jonny Daenen
 * 
 */
@RunWith(Parameterized.class)  
public class Rank1Test {

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();


	GFPrefixSerializer serializer;
	Configuration conf;

	@Before
	public void setUp() throws Exception {
		serializer = new GFPrefixSerializer();
	}

	public Rank1Test(Configuration conf, String descr) {
		this.conf = conf;
	}

	@Parameters(name = "{index}: conf:{1}")
	public static Collection<Object[]> getOnOffConfigurations() {


		Collection<Object[]> confs = new HashSet<Object[]>();

		Configuration on = new Configuration();
		(new HadoopExecutorSettings(on)).loadDefaults();
		(new HadoopExecutorSettings(on)).turnOnOptimizations();

		Configuration off = new Configuration();
		(new HadoopExecutorSettings(off)).loadDefaults();
		(new HadoopExecutorSettings(off)).turnOffOptimizations();

		Object [] onconf = {on, "optOn"};
		Object [] offconf = {off, "optOff"};

		confs.add(onconf);
		confs.add(offconf);

		// add solo-opts
		String [] keys = AbstractExecutorSettings.getAllKeys().toArray(new String [0]);
		for (String key : keys) {
			Configuration oneOn = new Configuration();
			HadoopExecutorSettings hc = new HadoopExecutorSettings(oneOn);

			hc.loadDefaults();
			hc.turnOffOptimizations();
			hc.setBooleanProperty(key, true);

			Object [] oneOnTest = {hc.getConf(), key};
//			if (key.contains("Group"))
//			confs.add(oneOnTest);
		}
		
		return confs;

	}


	private Set<Configuration> allConfigurations() {

		String [] keys = (String[]) AbstractExecutorSettings.getAllKeys().toArray();
		return generateAllConfsRec(keys,0);

	}

	/**
	 * Creates all possible settings for the optimizations.
	 */
	private Set<Configuration> generateAllConfsRec(String[] keys, int i) {

		if (i >= keys.length) {
			HashSet<Configuration> confs = new HashSet<Configuration>();
			confs.add(new Configuration());
			return new HashSet<>();
		} else {

			HashSet<Configuration> confs = new HashSet<Configuration>();

			Set<Configuration> otherConfs = generateAllConfsRec(keys, i+1);
			for (Configuration conf : otherConfs) {
				// add true and false version of this settings
				Configuration confTrue = new Configuration(conf);
				Configuration confFalse = new Configuration(conf);

				confTrue.setBoolean(keys[i], true);
				confFalse.setBoolean(keys[i], false);

				confs.add(confTrue);
				confs.add(confFalse);
			}

		}

		return null;
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

		Set<String> result = doTest(gfe, dataG,dataS,new RelationSchema("O",1),conf);

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


		Set<String> result = doTest(gfe, data,null,new RelationSchema("O",2),conf);
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

		Set<String> result = doTest(gfe, dataG, null, new RelationSchema("O",2),conf);
		//		System.out.println(result);

		System.out.println(result);
		if(result.size() != 1 || result.contains("O(1,2)") || result.contains("O(1,1)") || !result.contains("O(2,1)"))
			fail("expected {O(2,1)}");

	}

	private Set<String> doTest(GFExpression gfe, Set<String> dataG, Set<String> dataS, RelationSchema outSchema, Configuration conf) throws Exception {

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
		GumboQuery query = new GumboQuery(this.getClass().getSimpleName(), (GFExistentialExpression)gfe, rfm, new Path(output.getAbsolutePath()), new Path(scratch.getAbsolutePath()));
		GumboPlan plan = compiler.createPlan(query);

		HadoopEngine engine = new HadoopEngine();
		engine.executePlan(plan,conf);

		// TODO we need a method to request the output files/directory of a relation

		System.out.println(plan);

		Set<Path> outpaths = plan.getFileManager().getOutFileMapping().getPaths(outSchema);
		System.out.println(plan.getFileManager().getOutFileMapping());
		System.out.println(outSchema);
		System.out.println(outpaths);
		Path p = outpaths.iterator().next();
		String path = p.toString() + "/" + outSchema.getName()+"-r-00000";


		// read output file
		//		File outputfile = new File(output.getAbsolutePath() + "/test/part-r-00000");
		File outputfile = new File(path);
		List<String> out = Files.readAllLines(outputfile.toPath(), StandardCharsets.US_ASCII);



		return new HashSet<String>(out);
	}


}
