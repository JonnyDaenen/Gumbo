/**
 * Created: 23 Jan 2015
 */
package gumbo.engine.hadoop.mrcomponents.tools;

import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.utils.LongBase64Converter;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Responsible for keeping an id for each path used.
 * @author Jonny Daenen
 *
 */
public class TupleIDCreator {
	
	InputSplit prevSplit = null;
	long prevPathID = 0;
	
	StringBuilder sb;

	public class TupleIDError extends Exception {
		private static final long serialVersionUID = 1L;

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
		sb = new StringBuilder(20);
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
	 * 
	 * <b>Precondition:</b> we assume that the paths in the {@link RelationFileMapping} 
	 * are absolute (this can be done by the {@link PathExpander}).
	 * 
	 * @param context the MR context
	 * @param offset the offset in the file
	 * 
	 * @return a unique identifier for this position in the file
	 * 
	 * @throws TupleIDError when no ID can be determined
	 * 
	 */
	public String getTupleID(Context context, long offset) throws TupleIDError  {
		
		try {
			
			
			InputSplit is = context.getInputSplit();
			
			long pathID;
			
			// TODO extract this and re-use in other mappers
			// simple caching
			if (prevSplit == is) {
//				System.out.println("IS caching match");
				pathID = prevPathID;
			} else {
//				System.out.println("IS caching mismatch");
				Method method;
				method = is.getClass().getMethod("getInputSplit");

				method.setAccessible(true);
				FileSplit fileSplit = (FileSplit) method.invoke(is);
				Path filePath = fileSplit.getPath();

//				FileStatus fs = filePath.getFileSystem(null).getFileStatus(filePath);


				// filePath must be absolute
				pathID = getPathID(filePath);
				
//				OPTIMIZE try this:
//				String filename= ((FileSplit)context.getInputSplit()).getPath().getName();

				prevSplit = is;
				prevPathID = pathID;
			}
			
			
			
			byte [] offsetEnc = longConverter.newconvertNoPrefix(offset);
//			byte [] pathIdEnc = longConverter.long2byte(pathID);
//			System.out.println(" filename: " + filePath + " match:" +match + " fileid:" + pathID +  "Offset: " + offset + " id: " + new String(offsetEnc) + "-" + pathID);

			sb.setLength(0);
			sb.append(new String(offsetEnc)); // OPTIMIZE ?
			sb.append("-");
			sb.append(pathID);
			return sb.toString();
			
		} catch (Exception e) {
			throw new TupleIDError("Unable to determine tuple id. ", e);
		}

	}
	

}
