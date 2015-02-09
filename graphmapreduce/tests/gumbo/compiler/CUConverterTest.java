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
public class CUConverterTest {


	
	@Test
	public void testCUConverter() {
		Set<GFExpression> exps;
		

		// rank1
		exps = new HashSet<>();
		exps.add(CompilerTester.getQuery1());
		testConverter(exps,1);

		// rank 2: decomposes into pieces
		exps = new HashSet<>();
		exps.add(CompilerTester.getQuery2());
		testConverter(exps,2);


		// filters out non-basic expressions
		exps = new HashSet<>();
		exps.add(CompilerTester.getQuery3());
		testConverter(exps,0);
		
		// all
		exps = new HashSet<>();
		exps.add(CompilerTester.getQuery1());
		exps.add(CompilerTester.getQuery2());
		exps.add(CompilerTester.getQuery3());
		testConverter(exps,3);


	}


	private void testConverter(Set<GFExpression> exps, int keys) {
		
		try {
			Set<GFExistentialExpression> result1 = CompilerTester.decomposer.decomposeAll(exps);
			 Map<RelationSchema, BasicGFCalculationUnit> result2 = CompilerTester.converter.createCalculationUnits(result1);
			assertEquals(keys,result2.keySet().size());
			assertEquals(keys,result2.values().size());
		} catch (GFDecomposerException e) {
			fail();
		} catch (CalculationUnitException e) {
			fail();
		}
		
	}


}
