package gumbo.engine.general.grouper.costmodel;

import gumbo.engine.general.grouper.structures.CalculationGroup;
import gumbo.structures.data.RelationSchema;

public interface CostSheet {
	
	
	int getRelationInputTuples(RelationSchema rs);
	int getRelationInputBytes(RelationSchema rs);
	int getRelationInputPages(RelationSchema rs);
	
	int getRelationIntermediateTuples(RelationSchema rs, CalculationGroup group);
	int getRelationIntermediateBytes(RelationSchema rs, CalculationGroup group);
	int getRelationIntermediatePages(RelationSchema rs, CalculationGroup group);
	
	int getLocalReadCost();
	int getLocalWriteCost();
	
	int getDFSReadCost();
	int getDFSWriteCost();
	
	int getTransferCost();
	
	int getMergeOrder();
	int getNumMappers();
	int getMapMergeOrder();
	int getMapSortBuffer();
	int getNumReducers();
	


}
