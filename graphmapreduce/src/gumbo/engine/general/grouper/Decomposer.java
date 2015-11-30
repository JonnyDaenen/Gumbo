package gumbo.engine.general.grouper;

import gumbo.compiler.calculations.BasicGFCalculationUnit;
import gumbo.compiler.calculations.CalculationUnit;
import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.engine.general.grouper.structures.CalculationGroup;
import gumbo.engine.general.grouper.structures.GuardedSemiJoinCalculation;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;

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
	public CalculationGroup decompose(CalculationUnitGroup partition) {
		

		// null to indicate the group contains all relevant expressions by itself
		CalculationGroup result = new CalculationGroup(null);
		
		// create all guard-guarded pairs
		for (CalculationUnit c: partition.getCalculations()) {
			
			// extract basic expression
			BasicGFCalculationUnit bc = (BasicGFCalculationUnit) c;
			GFExistentialExpression e = bc.getBasicExpression();
			
			// make combinations of guard atom with all guarded atoms
			if(e.isBasicGF()) {
				GFAtomicExpression guard = e.getGuard();
				for (GFAtomicExpression guarded : e.getGuardedAtoms()) {
					result.add(new GuardedSemiJoinCalculation(guard,guarded));
				}
			}
			
		}

		return result;
	}

}
