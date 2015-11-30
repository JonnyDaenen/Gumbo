package gumbo.engine.hadoop2.mapreduce.tools;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;

/**
 * Wrapper class for mapper and reducer context.
 * The class enabled Gumbo-specific parameter extraction on contexts.
 * 
 * @author Jonny Daenen
 *
 */
public class ContextInspector {

	private Context contextMap;
	private org.apache.hadoop.mapreduce.Reducer.Context contextRed;
	
	
	private Configuration conf;

	private Set<GFExistentialExpression> queries;
	private Map<String, String> outmap;
	private Map<GFAtomicExpression, Integer> atomidmap;
	private int maxatomid;
	

	public ContextInspector(Context c) {
		this.contextMap = c;
		conf = c.getConfiguration();
	}

	public ContextInspector(Reducer.Context c) {
		this.contextRed = c;
		conf = c.getConfiguration();
	}
	
	public void fetchParameters() {

		// queries
		conf.get("gumbo.queries");
		
		// output mapping
		conf.get("gumbo.outmap");
		
		// atom-id mapping
		conf.get("gumbo.atomids");
		
		// atom-id mapping
		maxatomid = conf.getInt("gumbo.maxatomid", 32);
		
	}

	/**
	 * Looks up the current file id.
	 * The input split is exctracted from the context and the file id is extracted.
	 * Next, the file path - file id mapping in the context is used to determine the file id.
	 * 
	 * @return the id of the file that contains the current input split
	 */
	public long getFileId()  {
		
		// FIXME check the following, if it failes, use underscores
		String path = System.getenv("mapreduce.map.input.file");
		System.out.println("path: " + path);

		InputSplit is = contextMap.getInputSplit();

		Method method;
		try {
			method = is.getClass().getMethod("getInputSplit");
			
			method.setAccessible(true);
			FileSplit fileSplit = (FileSplit) method.invoke(is);
			Path filePath = fileSplit.getPath();

			return getPathID(filePath);

			// OPTIMIZE try this:
			// String filename= ((FileSplit)context.getInputSplit()).getPath().getName();
			
		} catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return 0;

		

	}

	private long getPathID(Path filePath) {
		// TODO implement
		return 0;
	}
	
	
	/**
	 * Looks up the relation name this mapper is processing.
	 * @return the current relation name
	 */
	public String getRelationName() {
		// get file id
		long fileid = getFileId();
		// convert
		return getRelationName(fileid);
	}
	
	/**
	 * Looks up the relation name for a given id.
	 * @return the current relation name
	 */
	public String getRelationName(long fileid) {
		// TODO implement
		// lookup relation for this file id
		return null;
	}

	public Set<GFExistentialExpression> getQueries() {
		return queries;
	}

	public Set<GFAtomicExpression> getGuardedAtoms() {
		HashSet<GFAtomicExpression> atoms = new HashSet<>();
		for (GFExistentialExpression query : queries){
			atoms.addAll(query.getGuardedAtoms());
		}
		return atoms;
	}

	/**
	 * Returns the mapping from out relation names to filenames.
	 * @return mapping from out relation names to filenames
	 */
	public Map<String, String> getOutMapping() {
		return outmap;
	}

	/**
	 * Returns the mapping from atoms to their ids.
	 * @return mapping from atoms to their ids
	 */
	public Map<GFAtomicExpression, Integer> getAtomIdMap() {
		return atomidmap;
	}

	/**
	 * @return highest atom number
	 */
	public int getMaxAtomID() {
		return maxatomid;
	}
	
	
	
	

}
