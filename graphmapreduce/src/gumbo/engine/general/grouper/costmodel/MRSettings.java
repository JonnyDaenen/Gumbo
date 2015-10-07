package gumbo.engine.general.grouper.costmodel;

import gumbo.engine.general.settings.AbstractExecutorSettings;

/**
 * Contains relative cost for processing 1 unit of data.
 * @author Jonny Daenen
 *
 */
public class MRSettings {
	
	// costs
	protected double cost_local_r = 1;
	protected double cost_local_w = 1;
	protected double cost_hdfs_w = 330;
	protected double cost_hdfs_r = 59;
	protected double cost_transfer = 5;
	protected double cost_sort = 52;
	protected double cost_red = 0.12;
	
//	protected double cost_local_r = 1;
//	protected double cost_local_w = 1;
//	protected double cost_hdfs_w = 1;
//	protected double cost_hdfs_r = 1;
//	protected double cost_transfer = 50;
//	protected double cost_sort = 0.1;
//	protected double cost_red = 1000;
	
	// FUTURE extract these in separate class, as they can change for different jobs
	// map settings
	private double mapChunkSizeMB = 128;
	private double mapSortBufferMB = 100;
	private double mapMergeFactor = 10;
	
	// reduce settings
	private double redChunkSizeMB = 128;
	private double redSortBufferMB = 1024 * 0.7;
	private double redMergeFactor = 10;
	
	
	public MRSettings(AbstractExecutorSettings systemSettings) {
		// FIXME #group extract settings
	}
	public double getLocalReadCost() {
		return cost_local_r;
	}
	public double getLocalWriteCost() {
		return cost_local_w;
	}
	public double getHdfsReadCost() {
		return cost_hdfs_r;
	}
	public double getHdfsWriteCost() {
		return cost_hdfs_w;
	}
	
	public double getTransferCost() {
		return cost_transfer;
	}
	
	public double getSortCost() {
		return cost_sort;
	}
	public double getReduceCost() {
		return cost_red;
	}
	
	
	public double getMapChunkSizeMB() {
		return mapChunkSizeMB;
	}
	public double getMapSortBufferMB() {
		return mapSortBufferMB;
	}
	public double getMapMergeFactor() {
		return mapMergeFactor;
	}
	public double getRedChunkSizeMB() {
		return redChunkSizeMB;
	}
	public double getRedSortBufferMB() {
		return redSortBufferMB;
	}
	public double getRedMergeFactor() {
		return redMergeFactor;
	}
	
	
	
	

}