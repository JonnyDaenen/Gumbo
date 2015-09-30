package gumbo.engine.general.grouper.costmodel;


/**
 * Contains relative cost for processing 1 unit of data.
 * @author Jonny Daenen
 *
 */
public class MRSettings {
	
	protected double cost_local_r = 1;
	protected double cost_local_w = 1;
	protected double cost_hdfs_w = 330;
	protected double cost_hdfs_r = 59;
	protected double cost_transfer = 5;
	protected double cost_sort = 52;
	protected double cost_red = 0.12;
	
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
		// TODO Auto-generated method stub
		return 0;
	}
	public double getMapSortBufferMB() {
		// TODO Auto-generated method stub
		return 0;
	}
	public double getMapMergeFactor() {
		// TODO Auto-generated method stub
		return 0;
	}
	public double getRedChunkSizeMB() {
		// TODO Auto-generated method stub
		return 0;
	}
	public double getRedSortBufferMB() {
		// TODO Auto-generated method stub
		return 0;
	}
	public double getRedMergeFactor() {
		// TODO Auto-generated method stub
		return 0;
	}
	
	
	
	

}
