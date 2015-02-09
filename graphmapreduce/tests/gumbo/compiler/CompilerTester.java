/**
 * Created: 09 Feb 2015
 */
package gumbo.compiler;


import gumbo.compiler.calculations.BGFE2CUConverter;
import gumbo.compiler.calculations.BasicGFCalculationUnit;
import gumbo.compiler.decomposer.GFDecomposer;
import gumbo.compiler.filemapper.FileManager;
import gumbo.compiler.filemapper.FileMapper;
import gumbo.compiler.linker.CULinker;
import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.compiler.partitioner.CalculationPartitioner;
import gumbo.compiler.partitioner.PartitionedCalculationUnitGroup;
import gumbo.compiler.partitioner.UnitPartitioner;
import gumbo.compiler.structures.data.RelationSchema;
import gumbo.guardedfragment.gfexpressions.GFAndExpression;
import gumbo.guardedfragment.gfexpressions.GFAtomicExpression;
import gumbo.guardedfragment.gfexpressions.GFExistentialExpression;
import gumbo.guardedfragment.gfexpressions.GFExpression;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

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
	public void testCUConverter() {

		// same amount of expressions


	}


	@Test
	public void testCULinker() {

		// number of edges is ok

		// number of roots

		// number of leafs


	}

	@Test
	public void testFileMapper() {

		// number of different files

	}


	@Test
	public void testUnitPartitioner() {

		// number of partitions
		// different partition sizes

	}

	public void testCompilerParts() {
		// decompose expressions into basic ones
		//		Set<GFExistentialExpression> bgfes = decomposer.decomposeAll(expressions);
		//
		//		// CUConverter 
		//
		//		Map<RelationSchema, BasicGFCalculationUnit> cus = converter.createCalculationUnits(bgfes);
		//
		//
		//		// CULinker 
		//		CalculationUnitGroup dag = linker.createDAG(cus);
		//
		//
		//		// intitial file mappings 
		//
		//		FileManager fm = filemapper.expandFileMapping(infiles, outdir, scratchdir, dag);
		//
		//		// partition
		//		PartitionedCalculationUnitGroup pdag = partitioner.partition(dag,fm);
		//
		//
		//		GumboPlan plan = new GumboPlan("GumboQuery",pdag,fm);
	}

}
