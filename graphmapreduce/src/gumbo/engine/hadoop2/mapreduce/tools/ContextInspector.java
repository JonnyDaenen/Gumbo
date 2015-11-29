package gumbo.engine.hadoop2.mapreduce.tools;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

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

	public ContextInspector(Context c) {
		this.contextMap = c;
	}

	public ContextInspector(Reducer.Context c) {
		this.contextRed = c;
	}

	/**
	 * Looks up the current file id.
	 * The input split is exctracted from the context and the file id is extracted.
	 * Next, the file path - file id mapping in the context is used to determine the file id.
	 * 
	 * @return the id of the file that contains the current input split
	 */
	public long getFileId()  {

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
		// TODO implement
		return null;
	}

	public Set<GFAtomicExpression> getGuardedAtoms() {
		// TODO implement
		return null;
	}

	public Map<String, String> getOutMapping() {
		// TODO Auto-generated method stub
		return null;
	}
	
	
	
	

}
