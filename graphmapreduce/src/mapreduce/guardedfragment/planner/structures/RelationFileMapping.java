/**
 * Created: 08 Oct 2014
 */
package mapreduce.guardedfragment.planner.structures;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import mapreduce.guardedfragment.executor.hadoop.mappers.GFMapper1GuardRel;
import mapreduce.guardedfragment.planner.structures.data.RelationSchema;
import mapreduce.guardedfragment.planner.structures.data.RelationSchemaException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Maps a set of relations to a specific file/folder
 * @author Jonny Daenen
 *
 */
public class RelationFileMapping {


	private static final Log LOG = LogFactory.getLog(RelationFileMapping.class);



	public static void main(String[] args) throws IOException, RelationSchemaException, RelationFileMappingException {

		RelationFileMapping rm = new RelationFileMapping();
		rm.addPath(new RelationSchema("R",2), new Path("input/dummyrelations1/part1.txt"));
		rm.addPath(new RelationSchema("R",2), new Path("input/dummyrelations2"));
		rm.addPath(new RelationSchema("R",3), new Path("data/jonny2"));
		rm.addPath(new RelationSchema("S",3), new Path("data/jonny3"));
		rm.setFormat(new RelationSchema("S",3), InputFormat.CSV);

		Path cp = new Path("data/jonny1");


		System.out.println(rm);


		RelationFileMapping rm2 = new RelationFileMapping(rm.toString(),null);

		System.out.println(rm2);
	}



	HashMap<RelationSchema, Set<Path>> mapping;
	HashMap<RelationSchema, InputFormat> format;
	private Path defaultPath;

	public RelationFileMapping() {
		mapping = new HashMap<>();
		format = new HashMap<>();
	}

	/**
	 * Loads a serial representaion of a mapping.
	 * The input is of the following form:
	 * RelSchema1 - InputPath1, InputPath2, ... - format1; RelSchema2 - Input File2 - format2; ....
	 *  
	 * @param sIn a string-representation of the mapping
	 * @param fs 
	 */
	public RelationFileMapping(String sIn) throws RelationSchemaException, RelationFileMappingException {
		this(sIn,null);

	}

	/**
	 * Loads a serial representaion of a mapping. Paths are made qualified if a filesystem is provided.
	 * The input is of the following form:
	 * RelSchema1;InputPath1,InputPath2, ...;format1|RelSchema2 - Input File2 - format2; ....
	 *  
	 * @param sIn a string-representation of the mapping
	 * @param fs a filesystem
	 */
	public RelationFileMapping(String sIn, FileSystem fs) throws RelationSchemaException, RelationFileMappingException {
		this();

		// split schema's
		String[] listIn = sIn.split("'");
		//		LOG.error(Arrays.deepToString(listIn));

		for (int i = 0; i< listIn.length; i++) {
			String[] dummy = listIn[i].split(";");
			//			LOG.error(Arrays.deepToString(dummy));

			if (dummy.length <= 2) {
				throw new RelationFileMappingException("Expecting exactly one input file and optionally a format");
			}
			// schema
			RelationSchema rs = new RelationSchema(dummy[0].trim());

			// paths
			String [] paths = dummy[1].split(",");
			for (String path : paths) {
				Path newPath = new Path(path.trim());
				// make qualified and absolute
				if (fs != null)
					newPath = fs.makeQualified(newPath);
				addPath(rs, newPath);
			}

			// format if present
			InputFormat format = InputFormat.REL;
			if (dummy.length >= 3 && dummy[2].trim().toLowerCase().equals("csv")) {
				format = InputFormat.CSV;
			}
			setFormat(rs, format);

		}

	}

	/**
	 * Sets the format of input files for the given relation schema.
	 * @param s the relation schema
	 * @param f the format
	 */
	public void setFormat(RelationSchema s, InputFormat f) {
		format.put(s, f);
	}

	/**
	 * Fetches the format of the input files for the given relationschema.
	 * @param s the relationschema
	 * @return the file format of the input files
	 */
	public InputFormat getFormat(RelationSchema s) {
		if (format.containsKey(s))
			return format.get(s);
		return InputFormat.REL;
	}


	public Set<Path> getPathsWithFormat(InputFormat f) {
		Set<Path> result = new HashSet<>();
		for (RelationSchema rs : mapping.keySet()) {
			if (getFormat(rs) == f)
				result.addAll(mapping.get(rs));
		}
		return result;
	}


	/**
	 * Adds a path to the set of input paths of the relation schema.
	 * This can be a file or directory.
	 * @param s the relationschema
	 * @param p an input path for the relation schema
	 */
	public void addPath(RelationSchema s, Path p) {
		Set<Path> paths;
		if(mapping.containsKey(s)) {
			paths = mapping.get(s);
		} else {
			paths = new HashSet<Path>();
			mapping.put(s, paths);
		}
		paths.add(p);

	}

	/**
	 * @param infiles the mapping to incorporate into this one
	 * @param copyDefault true iff the default path should be copied too
	 */
	public void putAll(RelationFileMapping infiles, boolean copyDefault) {
		for (RelationSchema rs : infiles.mapping.keySet()) {
			Set<Path> set = infiles.mapping.get(rs);
			for (Path path : set) {
				addPath(rs,path);
			}
			format.putAll(infiles.format);
		}

		if (copyDefault) {
			defaultPath = infiles.defaultPath;
		}

	}




	/**
	 * Checks if there is a file mapping present for a schema.
	 * @param rs a relation schema
	 * @return true iff there is a set of paths present
	 */
	public boolean containsSchema(RelationSchema rs) {
		return mapping.containsKey(rs);
	}

	/**
	 * Gets the set of paths associated with a relationschema.
	 * @param rs a relation schema
	 * @return the pats of the relationschema, null if
	 */
	public Set<Path> getPaths(RelationSchema rs) {
		if (!containsSchema(rs))
			return new HashSet<>();
			return mapping.get(rs);
	}


	/**
	 * Fetches all the relation schemas in the mapping.
	 * @return all the relation schemas in the mapping
	 */
	public Iterable<RelationSchema> getSchemas() {
		return mapping.keySet();
	}

	/**
	 * Checks if a given path is coupled to a relation schema
	 * @param p a path
	 * @return true iff there is a relation schema with this path
	 */
	public boolean containsPath(Path p) {
		for (Set<Path> paths : mapping.values()) {
			if(paths.contains(p))
				return true;
		}
		return false;
	}

	/**
	 * Sets the path to be used when no other input is available.
	 * @param defaultPath a path
	 */
	public void setDefaultPath(Path defaultPath) {
		this.defaultPath = defaultPath;

	}

	/**
	 * Fetches the default path to be used when no other input is available.
	 * @return the default path
	 */
	public Path getDefaultPath() {
		return defaultPath;
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		String s = "";

		for (RelationSchema rs : mapping.keySet()) {
			s += "'" + rs.toString() + ";";

			Set<Path> paths = mapping.get(rs);
			for (Path path : paths) {
				s += path.toString() + ",";
			}
			s = s.substring(0, s.length()-1);
			s +=  ";";

			switch (getFormat(rs)) {
			case CSV:
				s += "csv";
				break;
			default:
				s += "rel";
				break;
			}
			s += ";";
		}

		return s.length() > 0 ? s.substring(1) : "";
	}

	/**
	 * Finds a schema that has a given path as input source.
	 * TODO return set of schemas? fuzzy matching with wildcard support
	 * OPTIMIZE make reverse mapping
	 * @param filePath a path
	 * @return a relationschema of the path
	 */
	public RelationSchema findSchema(Path filePath) {
		// find exact match
		for (RelationSchema rs : mapping.keySet()){
			Set<Path> paths = mapping.get(rs);
			if(paths.contains(filePath))
				return rs;
		}
		// find partial match
		for (RelationSchema rs : mapping.keySet()){
			Set<Path> paths = mapping.get(rs);
			for ( Path path : paths) {
				//				LOG.debug("Looking for relation name:" + filePath + " " + path); 
				if(filePath.toString().contains(path.toString()))
					return rs;
			}	
		}

		//		LOG.debug("Looking for relation name: nothing found"); 
		// TODO throw error
		return null;
	}


	/**
	 * Calculates the number of bytes that are input to this 
	 * @return
	 */
	public static long getInputSize(Path path) {

		long length = 0;

		try {
			
			Configuration config = new Configuration();
			FileSystem hdfs = path.getFileSystem(config);
			ContentSummary cSummary = hdfs.getContentSummary(path);
			length = cSummary.getLength();
			
		} catch (IOException e) {
			LOG.error("Cannot determine input size of " + path, e);
		}
		return length;
	}

	
	public long getRelationSize(RelationSchema s) {
		Set<Path> paths;
		long l = 0;
		if(mapping.containsKey(s)) {
			paths = mapping.get(s);
		} else {
			return 0;
		}
		
		// update path estimate
		for (Path p : paths) {
			l += getInputSize(p);
		}
		return l;
		
	}



}
