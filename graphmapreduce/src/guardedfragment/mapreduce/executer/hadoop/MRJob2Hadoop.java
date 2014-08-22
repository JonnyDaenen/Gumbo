/**
 * Created: 22 Aug 2014
 */
package guardedfragment.mapreduce.executer.hadoop;

import guardedfragment.mapreduce.planner.structures.MRJob;

import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;

/**
 * A converter for transforming internal MR-jobs into hadoop jobs.
 *  
 * @author Jonny Daenen
 *
 */
public class MRJob2Hadoop {

	/**
	 * Converts a MRJob to a hadoop ControlledJob.
	 * @param job the job to convert
	 * @return the Controlledjob
	 */
	ControlledJob convert(MRJob job){
		// TODO implement
		
		return null;
	}
	
}
