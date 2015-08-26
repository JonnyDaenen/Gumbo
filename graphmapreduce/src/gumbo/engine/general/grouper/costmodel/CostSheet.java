package gumbo.engine.general.grouper.costmodel;

import gumbo.engine.general.grouper.structures.CalculationGroup;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFExistentialExpression;

import java.util.Collection;

public interface CostSheet {
	
	
	void initialize(CalculationGroup group, Collection<GFExistentialExpression> expressions);
	
	long getRelationInputTuples(RelationSchema rs);
	long getRelationInputBytes(RelationSchema rs);
	long getTotalInputBytes();
	
	long getRelationIntermediateTuples(RelationSchema rs);
	long getRelationIntermediateBytes(RelationSchema rs);
	long getTotalIntermediateBytes();
	
	double getLocalReadCost();
	double getLocalWriteCost();
	double getDFSReadCost();
	double getDFSWriteCost();
	double getTransferCost();
	
	int getNumMappers();
	int getNumMappers(RelationSchema rs);
	int getMapMergeOrder();
	long getMapSortBuffer();
	
	int getNumReducers();
	int getReduceMergeOrder();
	long getReduceSortBuffer();

	double getSortCost();

	


}
