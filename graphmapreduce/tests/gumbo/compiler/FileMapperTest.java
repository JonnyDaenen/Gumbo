/**
 * Created: 09 Feb 2015
 */
package gumbo.compiler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import gumbo.compiler.calculations.BGFE2CUConverter;
import gumbo.compiler.calculations.BasicGFCalculationUnit;
import gumbo.compiler.calculations.CalculationUnitException;
import gumbo.compiler.decomposer.GFDecomposer;
import gumbo.compiler.decomposer.GFDecomposerException;
import gumbo.compiler.filemapper.FileManager;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.compiler.structures.data.RelationSchema;
import gumbo.guardedfragment.gfexpressions.GFExistentialExpression;
import gumbo.guardedfragment.gfexpressions.GFExpression;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 * @author Jonny Daenen
 *
 */
public class FileMapperTest {



	@Test
	public void testFileMapper() {
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
		testFileMapper(exps);


	}


	private void testFileMapper(Set<GFExpression> exps) {

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
			FileManager result4 = CompilerTester.filemapper.expandFileMapping(rfm, new Path("out"), new Path("scratch"), result3);

			assertEquals(0, result4.getTempPaths().size());
			assertEquals(3, result4.getOutPaths().size()); // out1, out2, out2b
			// FUTURE remove unused? :
			assertEquals(7, result4.getInDirs().size()); // 4 - 1 (multi) + 1 + 1 + 1 + 1 = 6
			
			// union: all must be unique
			assertEquals(10, result4.getAllPaths().size());
			
			// intersection: must be empty
			Set<Path> intersection = new HashSet<Path>(result4.getInDirs());
			intersection.retainAll(result4.getTempPaths());
			intersection.retainAll(result4.getOutPaths());
			assertEquals(0, intersection.size());
			

		} catch (GFDecomposerException e) {
			fail();
		} catch (CalculationUnitException e) {
			fail();
		}

	}


}
