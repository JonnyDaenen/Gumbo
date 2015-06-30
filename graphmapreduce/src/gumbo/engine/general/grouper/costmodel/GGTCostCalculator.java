package gumbo.engine.general.grouper.costmodel;

import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import gumbo.compiler.GFCompiler;
import gumbo.engine.general.grouper.structures.CalculationGroup;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFAtomicExpression;

public class GGTCostCalculator {

	private static final Log LOG = LogFactory.getLog(GGTCostCalculator.class); 

	private CostSheet cs;


	public GGTCostCalculator(CostSheet cs) {
		this.cs = cs;
	}


	public double calculateCost(CalculationGroup group) {
		
		cs.initialize(group);

		Collection<GFAtomicExpression> guards = group.getGuardsDistinct();
		//		List<GFAtomicExpression> guards = group.getGuardList();

		Collection<GFAtomicExpression> guardeds = group.getGuardedsDistinct();


		long intermediate = 0;

		// intermediate output tuples for guards
		for (GFAtomicExpression guard : guards) {
			RelationSchema guardSchema = guard.getRelationSchema();
			intermediate += cs.getRelationIntermediateBytes(guardSchema);
		}

		// intermediate output tuples for guarded
		for (GFAtomicExpression guarded : guardeds) {
			RelationSchema guardedSchema = guarded.getRelationSchema();
			intermediate += cs.getRelationIntermediateBytes(guardedSchema);
		}






		return determineMapCost(guards, guardeds, intermediate) + determineReduceCost(guards, guardeds, intermediate);
	}


	private double determineReduceCost(Collection<GFAtomicExpression> guards, Collection<GFAtomicExpression> guardeds, long intermediate) {
		
		double reduceCost = 0;
		long numReducers = cs.getNumReducers();
		double rwCost = cs.getLocalReadCost() + cs.getLocalWriteCost();

		// shuffle (transfer)
		reduceCost += intermediate * cs.getTransferCost();


		// merge
		// calculate number of pieces one reducer has to process
		int mergeOrderR = cs.getReduceMergeOrder();
		long sortBufferR = cs.getReduceSortBuffer();
		long piecesR = (long) Math.ceil((intermediate/numReducers)/sortBufferR);

		// for a given order and pieces, calculate the number of rounds

		long levelsR = (long) (Math.ceil(Math.log10(piecesR) / Math.log10(mergeOrderR)) - 1);

		// each round, the entire intermediate data is read/written to/from disk
		// (spread across the cluster of course)
		reduceCost +=  levelsR * intermediate * rwCost;

		// final merge read
		reduceCost += intermediate * cs.getLocalReadCost();

		// DFS write
		reduceCost += intermediate * cs.getDFSWriteCost();

		return reduceCost;
	}


	private double determineMapCost(Collection<GFAtomicExpression> guards, Collection<GFAtomicExpression> guardeds, long intermediate) {
		double mapCost = 0;

		// local (!) guard input read
		for (GFAtomicExpression guard : guards) {
			RelationSchema guardSchema = guard.getRelationSchema();
			mapCost += cs.getLocalReadCost() * cs.getRelationInputBytes(guardSchema);
		}

		// local (!) guarded input read
		for (GFAtomicExpression guarded : guardeds) {
			RelationSchema guardedSchema = guarded.getRelationSchema();
			mapCost += cs.getLocalReadCost() * cs.getRelationInputBytes(guardedSchema);
		}




		double rwCost = cs.getLocalReadCost() + cs.getLocalWriteCost();
		int numMappers = cs.getNumMappers();
		int mergeOrderM = cs.getMapMergeOrder();
		long sortBufferM = cs.getMapSortBuffer();

		// calculate number of pieces one mapper has to process
		long piecesM = Math.round((intermediate/numMappers)/sortBufferM);

		// for a given order and pieces, calculate the number of rounds

		long levelsM = (long) Math.ceil(Math.log10(piecesM) / Math.log10(mergeOrderM));

		// each round, the entire intermediate data is read/written to/from disk
		// (spread across the cluster of course)
		mapCost +=  levelsM * intermediate * rwCost;

		return mapCost;
	}

}
