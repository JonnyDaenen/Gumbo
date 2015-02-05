/**
 * Created: 23 Jan 2015
 */
package mapreduce.guardedfragment.executor.hadoop.mappers;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mapreduce.guardedfragment.planner.structures.RelationFileMapping;
import mapreduce.utils.LongBase64Converter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Responsible for keeping an id for each path used.
 * @author Jonny Daenen
 *
 */
public class TupleIDCreator {
	

	public class TupleIDError extends Exception {
		public TupleIDError(String msg, Exception cause) {
			super(msg, cause);
		}
	}
	

	LongBase64Converter longConverter;

	private RelationFileMapping rm;
	Map<Path, Integer> mapping;

	public TupleIDCreator(RelationFileMapping rm) {
		this.rm = rm;
		longConverter = new LongBase64Converter();
		createIds();
	}

	/**
	 * 
	 */
	private void createIds() {
		mapping = new HashMap<Path, Integer>();
		Set<Path> paths = rm.getAllPaths();
		List<Path> list = new ArrayList<Path>(paths);
		Collections.sort(list);
		for (int i = 0; i < list.size(); i++) {
			mapping.put(list.get(i), i);
		}
		
	}

	/**
	 * @param filePath
	 * @return
	 */
	public long getPathID(Path filePath) {
//		System.out.println(mapping + " " + filePath); 
		return mapping.get(filePath);
	}

	/**
	 * Uses a file id and file offset to create a unique tuple id.
	 * @param context
	 * @param l
	 * @return the id for the given filename
	 * 
	 * @throws TupleIDError 
	 */
	public String getTupleID(org.apache.hadoop.mapreduce.Mapper.Context context, long offset) throws TupleIDError  {
		
		try {
			
			// OPTIMIZE I think this takes some time...
			InputSplit is = context.getInputSplit();
			Method method;
			method = is.getClass().getMethod("getInputSplit");

			method.setAccessible(true);
			FileSplit fileSplit = (FileSplit) method.invoke(is);
			Path filePath = fileSplit.getPath();
			
			Path match = rm.findBestPathMatch(filePath);

			long pathID = getPathID(match);
			
//			OPTIMIZE try this:
//			String filename= ((FileSplit)context.getInputSplit()).getPath().getName();
			
			byte [] offsetEnc = longConverter.long2byte(offset);
//			byte [] pathIdEnc = longConverter.long2byte(pathID);
			System.out.println(" file: " +match + " fileid:" + pathID +  "Offset: " + offset + " id: " + new String(offsetEnc) + "-" + pathID);

			return "" + new String(offsetEnc) + "-" + pathID;
		} catch (Exception e) {
			throw new TupleIDError("Unable to determine tuple id. ", e);
		}

	}
	

}
