/**
 * Created: 09 Feb 2015
 */
package gumbo.compiler;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import gumbo.compiler.calculations.BGFE2CUConverter;
import gumbo.compiler.decomposer.GFDecomposer;
import gumbo.compiler.filemapper.FileMapper;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.compiler.linker.CULinker;
import gumbo.compiler.partitioner.CalculationPartitioner;
import gumbo.compiler.partitioner.UnitPartitioner;
import gumbo.input.GumboQuery;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFAndExpression;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.GFExpression;

/**
 * @author Jonny Daenen
 *
 */
public class CompilerTester {

	protected static GFDecomposer decomposer = new GFDecomposer();
	protected static BGFE2CUConverter converter = new BGFE2CUConverter();
	protected static CULinker linker = new CULinker();
	protected static FileMapper filemapper = new FileMapper();
	protected static CalculationPartitioner partitioner = new UnitPartitioner();

	static GFAtomicExpression atomR = new GFAtomicExpression("R", "x","y");
	static GFAtomicExpression atomS = new GFAtomicExpression("S", "x");
	static GFAtomicExpression atomQ = new GFAtomicExpression("Q", "y", "z");
	static GFAtomicExpression atomT = new GFAtomicExpression("T", "y");


	static GFAtomicExpression out2 = new GFAtomicExpression("Out2", "x");
	static GFAtomicExpression out2b = new GFAtomicExpression("Out2b", "x");
	static GFAtomicExpression out1 = new GFAtomicExpression("Out1", "x");
	static GFAtomicExpression out3 = new GFAtomicExpression("Out3", "x");

	@Before
	public void initialize() {
	}


	protected static GFExpression getQuery2() {
		return new GFExistentialExpression(atomR, new GFAndExpression(new GFExistentialExpression(atomQ, atomT, out2b), atomS), out2);
	}

	protected static GFExpression getQuery1() {
		return new GFExistentialExpression(atomR, new GFAndExpression(atomT, atomS), out1);
	}

	protected static GFExpression getQuery3() {
		return new GFAndExpression(atomT, atomS);
	}



	@Test
	public void testCompiler() {
		GFCompiler compiler = new GFCompiler();
		
		Set<GFExpression> exps = new HashSet<>();
		exps.add(CompilerTester.getQuery1());
		exps.add(CompilerTester.getQuery2());
		exps.add(CompilerTester.getQuery3());
		
		RelationFileMapping rfm = new RelationFileMapping();
		rfm.addPath(new RelationSchema("R",2), new Path("in/R1"));
		rfm.addPath(new RelationSchema("R",2), new Path("in/R2")); 
		rfm.addPath(new RelationSchema("R",2), new Path("in/R3"));
		rfm.addPath(new RelationSchema("R",2), new Path("in/R3")); // ignore copies
		rfm.addPath(new RelationSchema("Q",2), new Path("in/Q"));
		rfm.addPath(new RelationSchema("S",1), new Path("in/S"));
		rfm.addPath(new RelationSchema("T",1), new Path("in/T"));
		rfm.addPath(new RelationSchema("UNKNOWN",1), new Path("in/UNKNOWN"));
		
		try {
			GumboQuery query = new GumboQuery(this.getClass().getSimpleName(),exps, rfm, new Path("out"), new Path("Scratch"));
			GumboPlan plan = compiler.createPlan(query);
			
			assertNotNull(plan.fileManager);
			assertNotNull(plan.partitions);
			
			assertEquals(3, plan.partitions.getNumPartitions());
			assertEquals(3, plan.partitions.getCalculationUnits().size());
			assertEquals(10, plan.fileManager.getAllPaths().size());
			
		} catch (IllegalArgumentException | GFCompilerException e) {
			// TODO make exception tests
			fail();
		} 
	}

}
