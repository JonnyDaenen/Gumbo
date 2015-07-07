package gumbo.engine.general.grouper.costmodel;

import gumbo.engine.general.grouper.structures.CalculationGroup;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFAtomicExpression;

import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GGTCostCalculator implements CostCalculator{

	private static final Log LOG = LogFactory.getLog(GGTCostCalculator.class); 

	private CostSheet cs;


	public GGTCostCalculator(CostSheet cs) {
		this.cs = cs;
	}


	public double calculateCost(CalculationGroup group) {
		
		cs.initialize(group);

		Collection<RelationSchema> schemas = group.getAllSchemas();


		long intermediate = 0;

		// intermediate output tuples 
		for (RelationSchema schema : schemas) {
			intermediate += cs.getRelationIntermediateBytes(schema);
		}
		

		double totalCost =  determineMapCost(schemas, intermediate) + determineReduceCost(intermediate);
		
//		System.out.println("Total cost:" + totalCost);
		return totalCost;
	}


	private double determineReduceCost(long intermediate) {
		
		double reduceCost = 0;
		long numReducers = cs.getNumReducers();
//		System.out.println("NUMR" + numReducers);
		double rwCost = cs.getLocalReadCost() + cs.getLocalWriteCost();

		// shuffle (transfer)
		reduceCost += intermediate * cs.getTransferCost();


		// merge
		// calculate number of pieces one reducer has to process
		int mergeOrderR = cs.getReduceMergeOrder();
		long sortBufferR = cs.getReduceSortBuffer();
		long piecesR = (long) Math.max(1, Math.ceil(((double)intermediate/numReducers)/sortBufferR));
//		System.out.println("REDPieces: " + piecesR);
		
		// for a given order and pieces, calculate the number of rounds

		long levelsR = (long) Math.max(0, (Math.ceil(Math.log10(piecesR) / Math.log10(mergeOrderR)))-1);
		
//		System.out.println("REDLevels: " + levelsR);
		
		// each round, the entire intermediate data is read/written to/from disk
		// (spread across the cluster of course)
		reduceCost += levelsR * intermediate * rwCost;
//		reduceCost += numReducers * piecesR  * sortBufferR * cs.getSortCost(); // NEW!
		// final merge read
		reduceCost += intermediate * cs.getLocalReadCost();
		// DFS write
//		reduceCost += cs.getNumOutputTuples() * cs.getDFSWriteCost();
		
//		System.out.println("RED final: " + reduceCost);
		
		return reduceCost;
	}


	private double determineMapCost(Collection<RelationSchema> schemas, long intermediate) {
		double mapCost = 0;

		// local (!) guard input read
		for (RelationSchema rs : schemas) {
			mapCost += cs.getLocalReadCost() * cs.getRelationInputBytes(rs);
			mapCost += calculateSortCost(rs);
		}

		
//		System.out.println("MAP final: " + mapCost);
		
		return mapCost;
	}


	private double calculateSortCost(RelationSchema rs) {

		long intermediate = cs.getRelationIntermediateBytes(rs);
		
//		System.out.println("Calculation map output for " + rs);
		
		double rwCost = cs.getLocalReadCost() + cs.getLocalWriteCost();
		
		int numMappers = cs.getNumMappers(rs);
//		System.out.println("NUMM" + numMappers);
		int mergeOrderM = cs.getMapMergeOrder();
		long sortBufferM = cs.getMapSortBuffer();

		// calculate number of pieces one mapper has to process
		long piecesM = (long)Math.max(1, Math.ceil(((double)intermediate/numMappers)/sortBufferM));
//		System.out.println("MAPPieces: " + piecesM);
		
		// for a given order and pieces, calculate the number of rounds

		long levelsM = (long) Math.max(1, Math.ceil(Math.log10(piecesM) / Math.log10(mergeOrderM)));

//		System.out.println("MAPLevels: " + levelsM);
		
		// each round, the entire intermediate data is read/written to/from disk
		// (spread across the cluster of course)
		return levelsM * intermediate * rwCost;
		
	}

}
