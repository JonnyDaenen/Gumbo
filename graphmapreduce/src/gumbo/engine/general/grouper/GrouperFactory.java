package gumbo.engine.general.grouper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.engine.general.grouper.costmodel.GumboCostModel;
import gumbo.engine.general.grouper.costmodel.IOCostModel;
import gumbo.engine.general.grouper.costmodel.MRSettings;
import gumbo.engine.general.grouper.costmodel.PaperCostModel;
import gumbo.engine.general.grouper.policies.AllGrouper;
import gumbo.engine.general.grouper.policies.CostBasedGrouper;
import gumbo.engine.general.grouper.policies.NoneGrouper;
import gumbo.engine.general.settings.AbstractExecutorSettings;
import gumbo.engine.hadoop.converter.GumboHadoopConverter;

public class GrouperFactory {


	private static final Log LOG = LogFactory.getLog(GrouperFactory.class);

	public static Grouper createGrouper(GroupingPolicies p, RelationFileMapping rfm, AbstractExecutorSettings systemSettings) {

		Grouper g = null;

		switch (p) {
		case ALLGROUP:
			g = new Grouper(new AllGrouper());

		case NONEGROUP:
			g = new Grouper(new NoneGrouper());

		case COSTGROUP_GUMBO:
			g = new Grouper(new CostBasedGrouper(rfm, new GumboCostModel(new MRSettings(systemSettings)), systemSettings));

		case COSTGROUP_PAPER:
			g = new Grouper(new CostBasedGrouper(rfm, new PaperCostModel(new MRSettings(systemSettings)), systemSettings));

		case COSTGROUP_IO:
			g = new Grouper(new CostBasedGrouper(rfm, new IOCostModel(), systemSettings));

		default:
			LOG.info("Grouping policy not found, turning off grouping.");
			g = new Grouper(new NoneGrouper());
		}

		LOG.info("Setting grouping policy to " + p);

		return g;

	}

	public static Grouper createGrouper(RelationFileMapping rfm, AbstractExecutorSettings systemSettings) {

		String policyName = systemSettings.getProperty(AbstractExecutorSettings.mapOutputGroupingPolicy);
		GroupingPolicies policy = Enum.valueOf(GroupingPolicies.class, policyName);
		
		return createGrouper(policy, rfm, systemSettings);
	}

}
