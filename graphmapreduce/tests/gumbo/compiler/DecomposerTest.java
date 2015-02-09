/**
 * Created: 09 Feb 2015
 */
package gumbo.compiler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import gumbo.compiler.decomposer.GFDecomposer;
import gumbo.compiler.decomposer.GFDecomposerException;
import gumbo.guardedfragment.gfexpressions.GFExistentialExpression;
import gumbo.guardedfragment.gfexpressions.GFExpression;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

/**
 * @author Jonny Daenen
 *
 */
public class DecomposerTest {

	GFDecomposer decomposer = new GFDecomposer();
	
	@Test
	public void testDecomposer() {
		Set<GFExpression> exps;
		

		// rank1
		exps = new HashSet<>();
		exps.add(CompilerTester.getQuery1());
		testDecomposer(exps,1);

		// rank 2: decomposes into pieces
		exps = new HashSet<>();
		exps.add(CompilerTester.getQuery2());
		testDecomposer(exps,2);


		// filters out non-basic expressions
		exps = new HashSet<>();
		exps.add(CompilerTester.getQuery3());
		testDecomposer(exps,0);
		
		// all
		exps = new HashSet<>();
		exps.add(CompilerTester.getQuery1());
		exps.add(CompilerTester.getQuery2());
		exps.add(CompilerTester.getQuery3());
		testDecomposer(exps,3);


	}


	private void testDecomposer(Set<GFExpression> exps, int i) {
		
		try {
			Set<GFExistentialExpression> result = decomposer.decomposeAll(exps);
			assertEquals(i,result.size());
		} catch (GFDecomposerException e) {
			fail();
		}
		
	}


}
