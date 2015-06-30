package gumbo.engine.hadoop.converter;

import java.util.Map;

import gumbo.engine.general.grouper.costmodel.CostSheet;
import gumbo.engine.general.grouper.structures.CalculationGroup;
import gumbo.engine.general.grouper.structures.GuardedSemiJoinCalculation;
import gumbo.engine.hadoop.reporter.RelationReport;
import gumbo.engine.hadoop.reporter.RelationReporter;
import gumbo.structures.data.RelationSchema;

public class HadoopCostSheet implements CostSheet {
	
	
	
	private Map<RelationSchema, RelationReport> reports;
	private CalculationGroup group;

	public HadoopCostSheet(Map<RelationSchema, RelationReport> reports) {
		this.reports = reports;
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

		return (int)(getTotalInputBytes() / (128 * 1024 * 1024)); // 128MB
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
		return (int)(getTotalIntermediateBytes() / (128 * 1024 * 1024)); // 128MB TODO 1 GB?
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
		
	}

}
