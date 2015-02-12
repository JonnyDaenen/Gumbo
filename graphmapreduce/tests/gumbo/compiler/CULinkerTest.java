/**
 * Created: 09 Feb 2015
 */
package gumbo.compiler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import gumbo.compiler.calculations.BasicGFCalculationUnit;
import gumbo.compiler.calculations.CalculationUnitException;
import gumbo.compiler.decomposer.GFDecomposerException;
import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.compiler.structures.data.RelationSchema;
import gumbo.guardedfragment.gfexpressions.GFExistentialExpression;
import gumbo.guardedfragment.gfexpressions.GFExpression;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

/**
 * @author Jonny Daenen
 *
 */
public class CULinkerTest {


	
	@Test
	public void testCULinker() {
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
		testLinker(exps);


	}


	private void testLinker(Set<GFExpression> exps) {
		
		try {
			Set<GFExistentialExpression> result1 = CompilerTester.decomposer.decomposeAll(exps);
			 Map<RelationSchema, BasicGFCalculationUnit> result2 = CompilerTester.converter.createCalculationUnits(result1);
			 CalculationUnitGroup result3 = CompilerTester.linker.createDAG(result2);
			 
			assertEquals(3,result3.getCalculations().size());
			assertEquals(1,result3.getAllDependencies().size()); // 1 dependency (internal in this case)
			assertEquals(2,result3.getHeight());
			assertEquals(4,result3.getInputRelations().size());
			assertEquals(1,result3.getIntermediateRelations().size()); // Out2b
			assertEquals(2,result3.getLeafs().size());
			assertEquals(2,result3.getOutputRelations().size()); // Out1, Out2
			assertEquals(2,result3.getRoots().size());
			
		} catch (GFDecomposerException e) {
			fail();
		} catch (CalculationUnitException e) {
			fail();
		}
		
	}


}
