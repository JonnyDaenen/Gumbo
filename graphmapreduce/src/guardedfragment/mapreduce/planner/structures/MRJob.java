/**
 * Created: 22 Aug 2014
 */
package guardedfragment.mapreduce.planner.structures;

import guardedfragment.mapreduce.planner.structures.operations.GFMapper;
import guardedfragment.mapreduce.planner.structures.operations.GFReducer;

import java.util.Set;

/**
 * A 1-round MR-job containing:
 * - id
 * - name
 * - input files
 * - output files
 * - dependencies
 * - map function
 * - reduce function
 * 
 * @author Jonny Daenen
 *
 */
public class MRJob {
	
	Set<MRJob> dependencies;
	Set<String> inFiles;
	Set<String> outFiles;
	
	GFMapper mapFunction;
	GFReducer reduceFunction;
	

}
