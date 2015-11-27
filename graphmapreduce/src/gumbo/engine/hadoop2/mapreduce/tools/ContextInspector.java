package gumbo.engine.hadoop2.mapreduce.tools;

import java.lang.reflect.Method;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.engine.hadoop.mrcomponents.tools.TupleIDCreator.TupleIDError;
import gumbo.structures.gfexpressions.GFExistentialExpression;

public class ContextInspector {

	Context context;

	public ContextInspector(Context c) {
		this.context = c;
	}

	/**
	 * Looks up the current file id.
	 * The input split is exctracted from the context and the file id is extracted.
	 * Next, the file path - file id mapping in the context is used to determine the file id.
	 * 
	 * @return the id of the file that contains the current input split
	 */
	public long getFileId()  {

		InputSplit is = context.getInputSplit();

		Method method = is.getClass().getMethod("getInputSplit");

		method.setAccessible(true);
		FileSplit fileSplit = (FileSplit) method.invoke(is);
		Path filePath = fileSplit.getPath();

		return getPathID(filePath);

		// OPTIMIZE try this:
		// String filename= ((FileSplit)context.getInputSplit()).getPath().getName();

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
		
	}

	public Set<GFExistentialExpression> getQueries() {
		// TODO implement
		return null;
	}
	
	
	

}
