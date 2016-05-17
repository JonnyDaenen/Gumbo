package gumbo.engine.general.grouper;

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import gumbo.compiler.calculations.BasicGFCalculationUnit;
import gumbo.compiler.calculations.CalculationUnit;
import gumbo.compiler.calculations.CalculationUnitException;
import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.engine.general.grouper.policies.GroupingPolicy;
import gumbo.engine.general.grouper.structures.CalculationGroup;
import gumbo.engine.general.grouper.structures.GuardedSemiJoinCalculation;
import gumbo.structures.gfexpressions.GFExistentialExpression;


/**
 * Extracts semi-joins from a BSGF query and groups them together.
 * 
 * @author Jonny Daenen
 *
 */
public class Grouper {


	private static final Log LOG = LogFactory.getLog(Grouper.class);

	protected Decomposer decomposer;
	protected GroupingPolicy policy;


	public Grouper(GroupingPolicy policy) {
		this.decomposer = new Decomposer();
		this.policy = policy;
	}


	/**
	 * Adds grouping to one partition.
	 * @param partition
	 * @return
	 * @throws GroupingException 
	 */
	public List<CalculationGroup> group(CalculationUnitGroup partition) throws GroupingException {

		// decompose
		CalculationGroup semijoins = decomposer.decompose(partition);

		LOG.info("Decomposition complete: " + semijoins );

		// apply grouping using the policy
		List<CalculationGroup> groupedSJ = policy.group(semijoins);

		LOG.info("Grouping complete: " + groupedSJ.size() + " group(s)" );
		LOG.info("Grouping: " +  groupedSJ);

		return groupedSJ;

	}


}
