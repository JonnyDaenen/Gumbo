package gumbo.engine.general.grouper.costmodel;

import gumbo.engine.general.grouper.structures.CalculationGroup;
import gumbo.structures.data.RelationSchema;

public interface CostSheet {
	
	
	long getRelationInputTuples(RelationSchema rs);
	long getRelationInputBytes(RelationSchema rs);
	long getTotalInputBytes(CalculationGroup group);
	
	long getRelationIntermediateTuples(RelationSchema rs, CalculationGroup group);
	long getRelationIntermediateBytes(RelationSchema rs, CalculationGroup group);
	long getTotalIntermediateBytes(CalculationGroup group);
	
	double getLocalReadCost();
	double getLocalWriteCost();
	double getDFSReadCost();
	double getDFSWriteCost();
	double getTransferCost();
	
	int getNumMappers(CalculationGroup group);
	int getMapMergeOrder();
	long getMapSortBuffer();
	
	int getNumReducers(CalculationGroup group);
	int getReduceMergeOrder();
	long getReduceSortBuffer();
	


}
