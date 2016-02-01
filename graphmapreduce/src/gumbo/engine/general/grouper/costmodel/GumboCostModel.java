package gumbo.engine.general.grouper.costmodel;

import gumbo.engine.general.grouper.sample.SimulatorReport;
import gumbo.engine.general.grouper.structures.CalculationGroup;
import gumbo.structures.gfexpressions.io.Pair;

public class GumboCostModel implements CostModel {

	
	private MRSettings settings;

	public GumboCostModel(MRSettings settings) {
		this.settings = settings;
	}
	
	@Override
	public double calculateCost(CalculationGroup job) {
		

		double total_interm = job.getGuardOutBytes() + job.getGuardedOutBytes();
		double total_input = job.getGuardInBytes() + job.getGuardedInBytes();
		double guard_interm = job.getGuardOutBytes();

		return getMapCost(job) + getReduceCost(total_input, total_interm, guard_interm);
	}


	public double getMapCost(CalculationGroup job) {
		return getMapGuardCost(job) + getMapGuardedCost(job);
	}

	public double getReduceCost(double total_input, double total_interm, double guard_interm) {
				
		// convert to MegaBytes
		guard_interm /= (1024*1024);
		total_interm /= (1024*1024);
		total_input /= (1024*1024);

		// transfer cost
		double transfer_cost = total_interm * settings.getTransferCost();

		// transfer startup penalty
		double red_tasks = Math.ceil((float)total_interm / settings.getRedChunkSizeMB());
		double map_tasks = Math.ceil((float)total_input / settings.getMapChunkSizeMB());
		double penalty_cost = red_tasks * map_tasks * settings.getTransferPenaltyCost();
//		penalty_cost;

		// merge cost
		int red_inmem_correction = 0;
		double red_pieces = Math.max(1, settings.getRedChunkSizeMB() / (float)settings.getRedSortBufferMB());
		double red_merge_levels = Math.log(red_pieces)/ Math.log(settings.getRedMergeFactor()) + red_inmem_correction;
		double merge_cost = red_merge_levels * (total_interm) * (settings.getLocalReadCost() + settings.getLocalWriteCost());

		// reduce cost
		double reduce_cost = 0.5 * guard_interm * settings.getReduceCost();
		//reduce_cost += guard_interm * mr_settings.cost_hdfs_w
//		reduce_cost = 0;
		System.out.println("Transfer:" + transfer_cost);
		System.out.println("Penalty:" + penalty_cost);
		System.out.println("Merge:" + merge_cost);
		System.out.println("Reduce:" + reduce_cost);
		System.out.println("RED:" + (transfer_cost + penalty_cost + merge_cost + reduce_cost));
		return transfer_cost + penalty_cost + merge_cost + reduce_cost;
	}

	public double getMapGuardCost(CalculationGroup job) {
		return getMapCost(job.getGuardInBytes(), job.getGuardOutBytes());
	}

	public double getMapGuardedCost(CalculationGroup job) {
		return getMapCost(job.getGuardedInBytes(), job.getGuardedOutBytes());
	}

	protected double getMapCost(long inputBytes, long intermedateBytes){

		// convert to MegaBytes
		double input = inputBytes / (double)(1024*1024);
		double intermediate = intermedateBytes / (double)(1024*1024);

		// read cost
		double read_cost = settings.getLocalReadCost() * input;

		// sort cost
		double mappers = Math.ceil((double)input / settings.getMapChunkSizeMB());
		double one_map_output_size = (double)intermediate / mappers;
		double one_map_sort_chunks = Math.max(1,one_map_output_size / settings.getMapSplitBufferMB());
		System.out.println("Map out:" + one_map_output_size);
		System.out.println("Mappers:" + mappers);
		System.out.println("Map chunks:" + one_map_sort_chunks);
		double sort_cost = one_map_sort_chunks * settings.getMapChunkSizeMB() * settings.getSortCost();
		sort_cost = 0;

		// merge cost
		double map_merge_levels = Math.ceil(Math.log10(one_map_sort_chunks)/Math.log10(settings.getMapMergeFactor()));
		System.out.println("Map merge levels:" + map_merge_levels);
		double merge_cost = map_merge_levels * intermediate * (settings.getLocalReadCost() + settings.getLocalWriteCost());

		// store cost
		double store_cost = intermediate * settings.getLocalWriteCost();

		System.out.println("Read:" + read_cost);
		System.out.println("Sort:" + sort_cost);
		System.out.println("Merge:" + merge_cost);
		System.out.println("Store:" + store_cost);
		System.out.println("MAP:" + (read_cost + sort_cost + merge_cost + store_cost));
		return read_cost + sort_cost + merge_cost + store_cost;
	}

	@Override
	public double calculateCost(SimulatorReport report) {
		
		double total_interm = report.getGuardOutBytes() + report.getGuardedOutBytes();
		double total_input = report.getGuardInBytes() + report.getGuardedInBytes();
		double guard_interm = report.getGuardOutBytes();

		return getMapCost(report) + getReduceCost(total_input, total_interm, guard_interm);
	}

	private double getMapCost(SimulatorReport report) {
		double total = 0;
		for (Pair<Long, Long> p: report.getGuardDetails()) {
			total += getMapCost(p.fst, p.snd);
		}
		
		for (Pair<Long, Long> p: report.getGuardedDetails()) {
			total += getMapCost(p.fst, p.snd);
		}
		
		return total;
	}



}
