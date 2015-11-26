package gumbo.engine.general.grouper.costmodel;

import gumbo.engine.general.grouper.structures.CalculationGroup;

public class GumboCostModel implements CostModel {

	
	private MRSettings settings;

	public GumboCostModel(MRSettings settings) {
		this.settings = settings;
	}
	
	@Override
	public double calculateCost(CalculationGroup job) {

		return getMapCost(job) + getReduceCost(job);
	}


	public double getMapCost(CalculationGroup job) {
		return getMapGuardCost(job) + getMapGuardedCost(job);
	}

	public double getReduceCost(CalculationGroup job) {
		double total_interm = job.getGuardOutBytes() + job.getGuardedOutBytes();
		double total_input = job.getGuardInBytes() + job.getGuardedInBytes();
		
		// convert to MegaBytes
		total_interm /= (1024*1024);
		total_input /= (1024*1024);

		// transfer cost
		double transfer_cost = total_interm * settings.getTransferCost();

		// transfer startup penalty
		double red_tasks = Math.ceil((float)total_interm / settings.getRedChunkSizeMB());
		double map_tasks = Math.ceil((float)total_input / settings.getMapChunkSizeMB());
		double penalty_cost = red_tasks * map_tasks * settings.getTransferPenaltyCost();

		// merge cost
		int red_inmem_correction = 1;
		double red_pieces = Math.max(1, settings.getRedChunkSizeMB() / (float)settings.getRedSortBufferMB());
		double red_merge_levels = Math.log(red_pieces)/ Math.log(settings.getRedMergeFactor()) + red_inmem_correction;
		double merge_cost = red_merge_levels * (total_interm) * (settings.getLocalReadCost() + settings.getLocalWriteCost());

		// reduce cost
		double reduce_cost = total_interm * settings.getReduceCost();
		//reduce_cost += guard_interm * mr_settings.cost_hdfs_w
		reduce_cost = 0;

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
		double one_map_sort_chunks = one_map_output_size / settings.getMapSortBufferMB();
		double sort_cost = one_map_sort_chunks * settings.getMapChunkSizeMB() * settings.getSortCost();
		sort_cost = 0;

		// merge cost
		double map_merge_levels = Math.ceil(Math.log10(one_map_sort_chunks)/Math.log10(settings.getMapMergeFactor()));
		double merge_cost = map_merge_levels * intermediate * (settings.getLocalReadCost() + settings.getLocalWriteCost());

		// store cost
		double store_cost = intermediate * settings.getLocalWriteCost();

		return read_cost + sort_cost + merge_cost + store_cost;
	}



}
