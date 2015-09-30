package gumbo.engine.general.grouper.costmodel;

public class GumboCostModel implements CostModel {

	@Override
	public double calculateCost(MRSettings s, GroupedJob job) {

		return getMapCost(s,job) + getReduceCost(s,job);
	}


	public double getMapCost(MRSettings s, GroupedJob job) {
		return getMapGuardCost(s, job) + getMapGuardedCost(s, job);
	}

	public double getReduceCost(MRSettings s, GroupedJob job) {
		long total_interm = job.getGuardOutBytes() + job.getGuardedOutBytes();
		long total_input = job.getGuardInBytes() + job.getGuardedInBytes();
		
		// convert to MegaBytes
		total_interm /= (1024*1024);
		total_input /= (1024*1024);

		// transfer cost
		double transfer_cost = total_interm * s.getTransferCost();

		// transfer startup penalty
		double red_tasks = Math.ceil((float)total_interm / s.getRedChunkSizeMB());
		double map_tasks = Math.ceil((float)total_input / s.getMapChunkSizeMB());
		double penalty_cost = red_tasks * map_tasks * s.getTransferCost();

		// merge cost
		int red_inmem_correction = 1;
		double red_pieces = Math.max(1, s.getRedChunkSizeMB() / (float)s.getRedSortBufferMB());
		double red_merge_levels = Math.log(red_pieces)/ Math.log(s.getRedMergeFactor()) + red_inmem_correction;
		double merge_cost = red_merge_levels * (total_interm) * (s.getLocalReadCost() + s.getLocalWriteCost());

		// reduce cost
		double reduce_cost = total_interm * s.getReduceCost();
		//reduce_cost += guard_interm * mr_settings.cost_hdfs_w

		return transfer_cost + penalty_cost + merge_cost + reduce_cost;
	}

	public double getMapGuardCost(MRSettings s, GroupedJob job) {
		return getMapCost(s,job.getGuardInBytes(), job.getGuardOutBytes());
	}

	public double getMapGuardedCost(MRSettings s, GroupedJob job) {
		return getMapCost(s,job.getGuardedInBytes(), job.getGuardedOutBytes());
	}

	protected double getMapCost(MRSettings s, long input, long intermedate){

		// convert to MegaBytes
		input /= (1024*1024);
		intermedate /= (1024*1024);

		// read cost
		double read_cost = s.getLocalReadCost() * input;

		// sort cost
		double mappers = Math.ceil((double)input / s.getMapChunkSizeMB());
		double one_map_output_size = (double)intermedate / mappers;
		double one_map_sort_chunks = one_map_output_size / s.getMapSortBufferMB();
		double sort_cost = one_map_sort_chunks * s.getMapChunkSizeMB() * s.getSortCost();

		// merge cost
		double map_merge_levels = Math.ceil(Math.log10(one_map_sort_chunks)/Math.log10(s.getMapMergeFactor()));
		double merge_cost = map_merge_levels * intermedate * (s.getLocalReadCost() + s.getLocalWriteCost());

		// store cost
		double store_cost = intermedate * s.getLocalWriteCost();

		return read_cost + sort_cost + merge_cost + store_cost;
	}



}
