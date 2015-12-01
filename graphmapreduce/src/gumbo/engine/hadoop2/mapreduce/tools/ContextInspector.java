package gumbo.engine.hadoop2.mapreduce.tools;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
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
import gumbo.structures.gfexpressions.GFExpression;
import gumbo.structures.gfexpressions.io.DeserializeException;
import gumbo.structures.gfexpressions.io.GFPrefixSerializer;

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
	private HashMap<String, String> outmap;
	private HashMap<String, Long> fileidmap;
	private HashMap<Long, String> filerelationmap;
	private HashMap<String, Integer> atomidmap;
	private int maxatomid;



	public ContextInspector(Context c) throws InterruptedException {
		this.contextMap = c;
		conf = c.getConfiguration();
		fetchParameters();
	}

	public ContextInspector(Reducer.Context c) throws InterruptedException {
		this.contextRed = c;
		conf = c.getConfiguration();
		fetchParameters();
	}

	public void fetchParameters() throws InterruptedException {


		try {
			// queries
			GFPrefixSerializer serializer = new GFPrefixSerializer();
			String queryString = conf.get("gumbo.queries");
			queries = serializer.deserializeExSet(queryString);

			// output mapping
			String outmapString = conf.get("gumbo.outmap");
			outmap = PropertySerializer.stringToObject(outmapString, HashMap.class);

			// file to id mapping
			String filemapString = conf.get("gumbo.fileidmap");
			fileidmap = PropertySerializer.stringToObject(filemapString, HashMap.class);

			// file id to relation name mapping
			String filerelmap = conf.get("gumbo.filerelationmap");
			filerelationmap = PropertySerializer.stringToObject(filerelmap, HashMap.class);

			// atom-id mapping
			String atomidMapString = conf.get("gumbo.atomidmap");
			atomidmap = PropertySerializer.stringToObject(atomidMapString, HashMap.class);

			// highest id
			maxatomid = conf.getInt("gumbo.maxatomid", 32);

		} catch (DeserializeException e) {
			throw new InterruptedException("Error during parameter fetching: " + e.getMessage());
		}
	}

	/**
	 * Looks up the current file id.
	 * The input split is exctracted from the context and the file id is extracted.
	 * Next, the file path - file id mapping in the context is used to determine the file id.
	 * 
	 * @return the id of the file that contains the current input split
	 */
	public long getFileId()  {

		// FUTURE check the following, if it fails, use underscores
//		String path = System.getenv("mapreduce_map_input_file");
//		System.out.println(conf.get("mapreduce_map_input_file"));
//		System.out.println("path: " + path);
		

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
		return fileidmap.get(filePath.toString());
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
		return filerelationmap.get(fileid);
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
	
	public Set<GFAtomicExpression> getGuardAtoms() {
		HashSet<GFAtomicExpression> atoms = new HashSet<>();
		for (GFExistentialExpression query : queries){
			atoms.add(query.getGuard());
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
	public Map<String, Integer> getAtomIdMap() {
		return atomidmap;
	}

	/**
	 * @return highest atom number
	 */
	public int getMaxAtomID() {
		return maxatomid;
	}

	




}
