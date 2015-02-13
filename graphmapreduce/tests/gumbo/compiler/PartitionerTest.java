/**
 * Created: 09 Feb 2015
 */
package gumbo.compiler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import gumbo.compiler.calculations.BasicGFCalculationUnit;
import gumbo.compiler.calculations.CalculationUnitException;
import gumbo.compiler.decomposer.GFDecomposerException;
import gumbo.compiler.filemapper.FileManager;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.compiler.partitioner.PartitionedCUGroup;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.GFExpression;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 * @author Jonny Daenen
 *
 */
public class PartitionerTest {



	@Test
	public void testPartitioner() {
		Set<GFExpression> exps;

		//
		//		// rank1
		//		exps = new HashSet<>();
		//		exps.add(CompilerTester.getQuery1());
		//		testConverter(exps,1);
		//
		//		// rank 2: decomposes into pieces
		//		exps = new HashSet<>();
		//		exps.add(CompilerTester.getQuery2());
		//		testConverter(exps,2);
		//
		//
		//		// filters out non-basic expressions
		//		exps = new HashSet<>();
		//		exps.add(CompilerTester.getQuery3());
		//		testConverter(exps,0);
		//		
		// all
		exps = new HashSet<>();
		exps.add(CompilerTester.getQuery1());
		exps.add(CompilerTester.getQuery2());
		exps.add(CompilerTester.getQuery3());
		testPartitioner(exps);


	}


	private void testPartitioner(Set<GFExpression> exps) {

		try {
			Set<GFExistentialExpression> result1 = CompilerTester.decomposer.decomposeAll(exps);
			Map<RelationSchema, BasicGFCalculationUnit> result2 = CompilerTester.converter.createCalculationUnits(result1);
			CalculationUnitGroup result3 = CompilerTester.linker.createDAG(result2);
			RelationFileMapping rfm = new RelationFileMapping();
			rfm.addPath(new RelationSchema("R",2), new Path("in/R1"));
			rfm.addPath(new RelationSchema("R",2), new Path("in/R2")); 
			rfm.addPath(new RelationSchema("R",2), new Path("in/R3"));
			rfm.addPath(new RelationSchema("R",2), new Path("in/R3")); // ignore copies
			rfm.addPath(new RelationSchema("Q",2), new Path("in/Q"));
			rfm.addPath(new RelationSchema("S",1), new Path("in/S"));
			rfm.addPath(new RelationSchema("T",1), new Path("in/T"));
			rfm.addPath(new RelationSchema("UNKNOWN",1), new Path("in/UNKNOWN"));
			FileManager result4 = CompilerTester.filemapper.createFileMapping(rfm, new Path("out"), new Path("scratch"), result3);

			PartitionedCUGroup result5 = CompilerTester.partitioner.partition(result3, result4);
			
			assertTrue(result5.allLevelsUsed());
			assertTrue(result5.checkDependencies());
			
			assertEquals(3,result5.getCalculationUnits().size());
			
			assertEquals(1,result5.getPartition(0).size());
			assertEquals(1,result5.getPartition(1).size());
			assertEquals(1,result5.getPartition(2).size());
			
			assertEquals(3,result5.getNumPartitions());
			
			

		} catch (GFDecomposerException e) {
			fail();
		} catch (CalculationUnitException e) {
			fail();
		}

	}


}
