package gumbo.compiler.grouper.costmodel;

import gumbo.structures.data.RelationSchema;

public interface CostSheet {
	
	
	int getRelationInputTuples(RelationSchema rs);
	int getRelationInputBytes(RelationSchema rs);
	int getRelationInputPages(RelationSchema rs);
	
	int getRelationIntermediateTuples(RelationSchema rs);
	int getRelationIntermediateBytes(RelationSchema rs);
	int getRelationIntermediatePages(RelationSchema rs);
	
	int getLocalReadCost();
	int getLocalWriteCost();
	
	int getDFSReadCost();
	int getDFSWriteCost();
	
	int getTransferCost();
	
	int getMergeOrder();
	


}
