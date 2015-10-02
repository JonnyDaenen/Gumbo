package gumbo.engine.hadoop.converter;

import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.engine.general.grouper.costmodel.CostSheet;
import gumbo.engine.general.grouper.sample.RelationSampleContainer;
import gumbo.engine.general.grouper.sample.RelationSampler;
import gumbo.engine.general.grouper.structures.CalculationGroup;
import gumbo.engine.general.grouper.structures.GuardedSemiJoinCalculation;
import gumbo.engine.general.settings.AbstractExecutorSettings;
import gumbo.engine.hadoop.reporter.RelationReport;
import gumbo.engine.hadoop.reporter.RelationReporter;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations.GFOperationInitException;
import gumbo.utils.estimation.SamplingException;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Round 1 cost model.
 * 
 * @author Jonny Daenen
 *
 */
public class HadoopCostSheet implements CostSheet {

	private static final Log LOG = LogFactory.getLog(HadoopCostSheet.class);


	protected Map<RelationSchema, RelationReport> reports;
	protected CalculationGroup group;
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
		return 0.001;
//		return 1;
	}

	@Override
	public double getLocalWriteCost() {
		return 0.001;
//		return 1;
	}

	@Override
	public double getDFSReadCost() {
		return 0.059;
//		return 2;
	}

	@Override
	public double getDFSWriteCost() {
		return 0.334; // TODO take in replication factor into account
//		return 3; // TODO take in replication factor into account
	}

	@Override
	public double getTransferCost() {
		return 0.005;
//		return 2;
	}
	
	@Override
	public double getSortCost() {
		return 0.052;
	}



	@Override
	public int getNumMappers() {
		// TODO
		// per input file
		// size / hdfs block size

		return (int)Math.max(1,Math.ceil(getTotalInputBytes() / (double)(128 * 1024 * 1024))); // 128MB
	}
	
	@Override
	public int getNumMappers(RelationSchema rs) {
		return (int)Math.max(1,Math.ceil(getRelationInputBytes(rs) / (double)(128 * 1024 * 1024))); // 128MB
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
		return (int)Math.max(1,Math.ceil(getTotalIntermediateBytes() / (settings.getNumProperty(AbstractExecutorSettings.REDUCER_SIZE_MB)*1024*1024)));
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
	public void initialize(CalculationGroup group, Collection<GFExistentialExpression> expressions) {
		
		// TODO move expression set to constructor?
		this.group = group;
		try {
			// FIXME are these samples ok for new runs?
			if (samples == null) {
				LOG.info("Sampling...");
				samples = sampler.sample();
				LOG.info("Samples taken.");
			}
			
			RelationReporter rr = new RelationReporter(samples, mapping, settings);


			Collection<GFExistentialExpression> calcs = new HashSet<>();

			for (GuardedSemiJoinCalculation element : group.getAll()) {
				calcs.add(element.getExpression());
			}


			reports = rr.generateReports(calcs, expressions);
			//
			//			for (RelationReport report : reports.values())
			//				LOG.info(report);

		} catch (SamplingException | GFOperationInitException e){
			e.printStackTrace();
			// TODO handle exception
		}

	}

	

}
