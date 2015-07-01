package gumbo.engine.hadoop.converter;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.engine.general.grouper.costmodel.CostSheet;
import gumbo.engine.general.grouper.policies.KeyGrouper;
import gumbo.engine.general.grouper.structures.CalculationGroup;
import gumbo.engine.general.grouper.structures.GuardedSemiJoinCalculation;
import gumbo.engine.hadoop.reporter.RelationReport;
import gumbo.engine.hadoop.reporter.RelationReporter;
import gumbo.engine.hadoop.reporter.RelationSampleContainer;
import gumbo.engine.hadoop.reporter.RelationSampler;
import gumbo.engine.settings.AbstractExecutorSettings;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations.GFOperationInitException;
import gumbo.utils.estimation.SamplingException;


/**
 * Round 1 cost model.
 * 
 * @author Jonny Daenen
 *
 */
public class HadoopCostSheet implements CostSheet {

	private static final Log LOG = LogFactory.getLog(HadoopCostSheet.class);


	private Map<RelationSchema, RelationReport> reports;
	private CalculationGroup group;
	private RelationSampler sampler;
	private RelationFileMapping mapping;
	private AbstractExecutorSettings settings;
	RelationSampleContainer samples;

	public HadoopCostSheet(RelationFileMapping mapping, AbstractExecutorSettings settings) {
		sampler = new RelationSampler(mapping);
		this.mapping = mapping;
		this.settings = settings;
	}

	@Override
	public long getRelationInputTuples(RelationSchema rs) {
		return reports.get(rs).getEstInputTuples();
	}

	@Override
	public long getRelationInputBytes(RelationSchema rs) {
		return reports.get(rs).getNumInputBytes();
	}

	@Override
	public long getTotalInputBytes() {

		long output = 0;
		for (RelationSchema rs : group.getAllSchemas()) {
			output += reports.get(rs).getNumInputBytes();
		}

		return output;
	}

	@Override
	public long getRelationIntermediateTuples(RelationSchema rs) {
		return reports.get(rs).getEstIntermTuples();
	}

	@Override
	public long getRelationIntermediateBytes(RelationSchema rs) {
		return reports.get(rs).getEstIntermBytes();
	}

	@Override
	public long getTotalIntermediateBytes() {
		long output = 0;
		for (RelationSchema rs : group.getAllSchemas()) {
			output += reports.get(rs).getEstIntermBytes();
		}

		return output;
	}


	@Override
	public double getLocalReadCost() {
		return 1;
	}

	@Override
	public double getLocalWriteCost() {
		return 1;
	}

	@Override
	public double getDFSReadCost() {
		return 2;
	}

	@Override
	public double getDFSWriteCost() {
		return 3; // TODO take in replication factor into account
	}

	@Override
	public double getTransferCost() {
		return 2;
	}



	@Override
	public int getNumMappers() {
		// TODO
		// per input file
		// size / hdfs block size

		return (int)Math.max(1,Math.ceil(getTotalInputBytes() / (128 * 1024 * 1024))); // 128MB
	}

	@Override
	public int getMapMergeOrder() {
		// TODO read from config
		return 10;
	}

	@Override
	public long getMapSortBuffer() {
		// TODO read from config file
		return 100*1024*1024; //100mb
	}

	@Override
	public int getNumReducers() {
		// TODO use setting for this
		return (int)Math.max(1,Math.ceil(getTotalIntermediateBytes() / (128 * 1024 * 1024))); // 128MB TODO 1 GB?
	}

	@Override
	public int getReduceMergeOrder() {
		// TODO read from config
		return 10;
	}

	@Override
	public long getReduceSortBuffer() {
		// TODO read from config file
		return 100*1024*1024; //100mb
	}

	@Override
	public void initialize(CalculationGroup group) {
		this.group = group;
		try {
			if (samples == null) 
				samples = sampler.sample(10, 4 * 1024);
			
			RelationReporter rr = new RelationReporter(samples, mapping, settings);


			Collection<GFExistentialExpression> calcs = new HashSet<>();

			for (GuardedSemiJoinCalculation element : group.getAll()) {
				calcs.add(element.getExpression());
			}


			reports = rr.generateReports(calcs);
//
//			for (RelationReport report : reports.values())
//				LOG.info(report);

		} catch (SamplingException | GFOperationInitException e){
			e.printStackTrace();
			// TODO handle exception
		}

	}

}
