package gumbo.engine.general.grouper.costmodel;

import gumbo.engine.general.grouper.sample.SimulatorReport;
import gumbo.engine.general.grouper.structures.CalculationGroup;

public class PaperCostModel implements CostModel {

	private MRSettings settings;

	public PaperCostModel(MRSettings settings) {
		this.settings = settings;
	}

	@Override
	public double calculateCost(CalculationGroup job) {
		return calculateMapCost(job) + calculateReduceCost(job);
	}
	
	public double calculateCost(SimulatorReport report) {
		
		long metadataBytes = report.getTotalMapOutRec() * 16;
		// System.out.println("ALERT: meta:" + metadataBytes);
		
		CalculationGroup job = new CalculationGroup(null);
		job.setGuardedInBytes(report.getGuardedInBytes() + metadataBytes);
		job.setGuardedOutBytes(report.getGuardedOutBytes());
		job.setGuardInBytes(report.getGuardInBytes() );
		job.setGuardOutBytes(report.getGuardOutBytes());
		return calculateCost(job);
	}

	private double calculateReduceCost(CalculationGroup job) {
		
		double interm_mb = (job.getGuardedOutBytes() + job.getGuardOutBytes());
		interm_mb /= (1024*1024);
		
		int reduceCorrection = 0; // FIXME get from setting

		double redPieces = settings.getRedChunkSizeMB() / settings.getRedSortBufferMB();
		double redMergeLevels = Math.ceil(Math.log10(redPieces) / Math.log10(settings.getRedMergeFactor())) + reduceCorrection;
		System.out.println("merge levels:" + redMergeLevels);

		double redTransferCost = interm_mb * settings.getTransferCost();
		double redMergeCost = redMergeLevels * interm_mb * (settings.getLocalReadCost() + settings.getLocalWriteCost());

		return redTransferCost + redMergeCost;

	}

	private double calculateMapCost(CalculationGroup job) {

		double input_mb = (job.getGuardedInBytes() + job.getGuardInBytes());
		input_mb /= (1024*1024);

		double interm_mb = (job.getGuardedOutBytes() + job.getGuardOutBytes());
		interm_mb /= (1024*1024);


		double mapTasks = Math.ceil(input_mb/ settings.getMapChunkSizeMB());
		double mapPieces = settings.getMapChunkSizeMB() / settings.getMapSplitBufferMB();
		double mapMergeLevels = Math.ceil(Math.log10(mapPieces) / Math.log10(settings.getMapMergeFactor()));

		double mapInReadCost = input_mb * settings.getLocalReadCost();
		double mapOutInitialWrite = interm_mb * settings.getLocalWriteCost();
		double mapMergeCost = mapMergeLevels * interm_mb * (settings.getLocalReadCost() + settings.getLocalWriteCost());

		return mapInReadCost + mapOutInitialWrite + mapMergeCost;
	}






}
