package gumbo.engine.general.grouper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.engine.general.grouper.costmodel.GumboCostModel;
import gumbo.engine.general.grouper.costmodel.IOCostModel;
import gumbo.engine.general.grouper.costmodel.MRSettings;
import gumbo.engine.general.grouper.costmodel.PaperCostModel;
import gumbo.engine.general.grouper.policies.AllGrouper;
import gumbo.engine.general.grouper.policies.BestCostBasedGrouper;
import gumbo.engine.general.grouper.policies.CostBasedGrouper;
import gumbo.engine.general.grouper.policies.GuardedAtomGrouper;
import gumbo.engine.general.grouper.policies.NoneGrouper;
import gumbo.engine.general.settings.AbstractExecutorSettings;
import gumbo.engine.hadoop.reporter.RelationTupleSampleContainer;

public class GrouperFactory {


	private static final Log LOG = LogFactory.getLog(GrouperFactory.class);

	public static Grouper createGrouper(GroupingPolicies p, RelationFileMapping rfm, AbstractExecutorSettings systemSettings, RelationTupleSampleContainer samples) {

		Grouper g = null;

		LOG.info("Creating a grouper with policy " + p);
		switch (p) {
		case ALLGROUP:
			g = new Grouper(new AllGrouper());
			break;
			
		case NONEGROUP:
			g = new Grouper(new NoneGrouper());
			break;

		case COSTGROUP_GUMBO:
			g = new Grouper(new CostBasedGrouper(rfm, new GumboCostModel(new MRSettings(systemSettings)), systemSettings, samples));
			break;

		case COSTGROUP_PAPER:
			g = new Grouper(new CostBasedGrouper(rfm, new PaperCostModel(new MRSettings(systemSettings)), systemSettings, samples));
			break;

		case COSTGROUP_IO:
			g = new Grouper(new CostBasedGrouper(rfm, new IOCostModel(), systemSettings, samples));
			break;
			
		case BESTCOSTGROUP_GUMBO:
			g = new Grouper(new BestCostBasedGrouper(rfm, new GumboCostModel(new MRSettings(systemSettings)), systemSettings, samples));
			break;

		case BESTCOSTGROUP_PAPER:
			g = new Grouper(new BestCostBasedGrouper(rfm, new PaperCostModel(new MRSettings(systemSettings)), systemSettings, samples));
			break;

		case BESTCOSTGROUP_IO:
			g = new Grouper(new BestCostBasedGrouper(rfm, new IOCostModel(), systemSettings, samples));
			break;
			
		case GUARDEDATOMGROUP:
			g = new Grouper(new GuardedAtomGrouper());
			break;
		default:
			LOG.warn("Grouping policy not found, turning off grouping.");
			g = new Grouper(new NoneGrouper());
		}


		return g;

	}

	public static Grouper createGrouper(RelationFileMapping rfm, AbstractExecutorSettings systemSettings, RelationTupleSampleContainer samples) {

		String policyName = systemSettings.getProperty(AbstractExecutorSettings.mapOutputGroupingPolicy);
		GroupingPolicies policy = Enum.valueOf(GroupingPolicies.class, policyName);
		
		return createGrouper(policy, rfm, systemSettings, samples);
	}

}
