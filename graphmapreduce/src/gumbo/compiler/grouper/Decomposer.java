package gumbo.compiler.grouper;

import gumbo.compiler.calculations.BasicGFCalculationUnit;
import gumbo.compiler.calculations.CalculationUnit;
import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;

import java.util.HashSet;
import java.util.Set;

/**
 * Component used to decompose basic expressions into binary semijoins.
 * @author Jonny Daenen
 *
 */
public class Decomposer {

	/**
	 * Decomposes all basic GF calculations in a partition into a set of 
	 * Guard-guarded atom semijoins. Note that each pair appears at most once in the set.
	 * 
	 * @param partition the partition that contains the calculations to decompose
	 * @return decomposition in binary guarded semijoins
	 */
	public Set<GuardedSemiJoinCalculation> decompose(CalculationUnitGroup partition) {

		HashSet<GuardedSemiJoinCalculation> result = new HashSet<>();
		
		// create all guard-guarded pairs
		for (CalculationUnit c: partition.getCalculations()) {
			
			// extract basic expression
			BasicGFCalculationUnit bc = (BasicGFCalculationUnit) c;
			GFExistentialExpression e = bc.getBasicExpression();
			
			// make combinations of guard atom with all guarded atoms
			if(e.isBasicGF()) {
				GFAtomicExpression guard = e.getGuard();
				for (GFAtomicExpression guarded : e.getGuardedRelations()) {
					result.add(new GuardedSemiJoinCalculation(guard,guarded));
				}
			}
			
		}

		return result;
	}

}
