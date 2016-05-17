/**
 * Created: 12 Feb 2015
 */
package gumbo.engine.hadoop;

import java.io.IOException;

import org.apache.commons.collections4.BidiMap;
import org.apache.commons.collections4.bidimap.DualHashBidiMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;

import gumbo.compiler.filemapper.FileManager;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.compiler.partitioner.PartitionedCUGroup;
import gumbo.engine.general.utils.PartitionQueue;
import gumbo.structures.data.RelationSchema;

/**
 * Represents a queue of partitions, coupled to real hadoop jobs.
 * When the job status is updated, the queue is adjusted accordingly and
 * new (depending) jobs are made available to execute.
 * 
 * @author Jonny Daenen
 *
 */
public class HadoopPartitionQueue extends PartitionQueue {
	

	private static final Log LOG = LogFactory.getLog(HadoopPartitionQueue.class);
	
	
	// mapping
	BidiMap<CalculationUnitGroup,ControlledJob> calc2firstjob;
	BidiMap<CalculationUnitGroup,ControlledJob> calc2secondjob;


	private FileManager fileManager;


	private FileSystem dfs;


	public HadoopPartitionQueue(PartitionedCUGroup partitions, FileManager fileManager, Configuration conf) {
		super(partitions);
		this.fileManager = fileManager;
		calc2firstjob = new DualHashBidiMap<CalculationUnitGroup, ControlledJob>();
		calc2secondjob = new DualHashBidiMap<CalculationUnitGroup, ControlledJob>();
		
		try {
			this.dfs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}



	/**
	 * Checks whether all dependencies are finished
	 * 
	 * @param job a job
	 * @param jobcontrol the jobcontrol
	 * 
	 * @return <code>true</code> if all dependencies are done, <code>false</code> otherwise
	 */
	protected boolean isReady(CalculationUnitGroup group) {
		ControlledJob job1 = calc2firstjob.get(group);
		ControlledJob job2 = calc2secondjob.get(group);
		
		if (job1.isReady()) {
			LOG.info("Calculation group " + group + " round1 is ready.");
		}
		
		if (job2.isReady()) {
			LOG.info("Calculation group " + group + " round2 is ready.");
		}
		 
		return job1.isCompleted() && job2.isCompleted();
	}

	/**
	 * Connects jobs to a created partition.
	 * 
	 * @param partition a partition
	 * @param jobs hadoop jobs for the partitions
	 * 
	 */
	public void addJobs(CalculationUnitGroup partition, ControlledJob job1, ControlledJob job2) {
		calc2firstjob.put(partition, job1);
		calc2secondjob.put(partition, job2);
	}



	@Override
	protected void cleanup(CalculationUnitGroup group) {
		LOG.info("Group is done, moving its output files" + group);
		
		for (RelationSchema rs : group.getOutputRelations()) {
			LOG.info("Moving " + rs);
			
			RelationFileMapping outMapping = fileManager.getOutFileMapping();
			
			// create output dirs
			Path to = outMapping.getPaths(rs).iterator().next();

			ControlledJob job2 = calc2secondjob.get(group);
			
			
			Path from = Path.mergePaths(fileManager.getOutputRoot(), new Path("/" + job2.getJobName()+"/"+rs.getName()+ "*"));
			System.out.println("To: " + to );
			LOG.info("Moving to:  " + to);

			try {
				dfs.mkdirs(to);
				FileStatus[] files = dfs.globStatus(from);
//				System.out.println(files.length);
				for(FileStatus file: files) {
					System.out.println("From: " + file.getPath());
					if (!file.isDirectory()) {
						dfs.rename(file.getPath(), to);
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
			

		}
//			
//				System.out.println(dfs.getWorkingDirectory() +" this is from /n/n");
			
		
	}

}
