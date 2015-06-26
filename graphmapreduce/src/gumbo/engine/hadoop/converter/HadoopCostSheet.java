package gumbo.engine.hadoop.converter;

import gumbo.engine.general.grouper.costmodel.CostSheet;
import gumbo.engine.general.grouper.structures.CalculationGroup;
import gumbo.structures.data.RelationSchema;

public class HadoopCostSheet implements CostSheet {
	
	InputEstimator input;
	IntermediateEstimator interm;
	
	public HadoopCostSheet(InputEstimator input, IntermediateEstimator interm) {
		this.input = input;
		this.interm = interm;
	}

	@Override
	public long getRelationInputTuples(RelationSchema rs) {
		return input.estNumTuples(rs);
	}

	@Override
	public long getRelationInputBytes(RelationSchema rs) {
		return input.getNumBytes(rs);
	}

	@Override
	public long getTotalInputBytes(CalculationGroup group) {
		return input.getTotalBytes(group);
	}

	@Override
	public long getRelationIntermediateTuples(RelationSchema rs,
			CalculationGroup group) {
		return interm.estNumTuples(rs,group);
	}

	@Override
	public long getRelationIntermediateBytes(RelationSchema rs,
			CalculationGroup group) {
		return interm.estNumBytes(rs,group);
	}

	@Override
	public long getTotalIntermediateBytes(CalculationGroup group) {
		return interm.getTotalBytes(group);
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
	public int getNumMappers(CalculationGroup group) {
		// TODO
		// per input file
		// size / hdfs block size

		return (int)(getTotalInputBytes(group) / (128 * 1024 * 1024)); // 128MB
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
	public int getNumReducers(CalculationGroup group) {
		// TODO use setting for this
		return (int)(getTotalIntermediateBytes(group) / (128 * 1024 * 1024)); // 128MB TODO 1 GB?
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

}
