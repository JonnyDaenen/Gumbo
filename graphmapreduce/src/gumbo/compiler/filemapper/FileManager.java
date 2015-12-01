/**
 * Created: 12 May 2014
 */
package gumbo.compiler.filemapper;

import gumbo.structures.data.RelationSchema;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.Path;

/**
 * Bookkeeping for input/output locations, coupled to relations. 
 * TODO check
 * 
 * @author Jonny Daenen
 * 
 *         TODO add unit tests
 */
public class FileManager {

	protected Path outputroot;
	protected Path scratchroot;

	protected Path tmproot;

	protected int counter;
	protected final static String TMP_PREFIX = "TMP_";
	protected final static String OUT_PREFIX = "OUT_";

	protected Set<Path> tempdirs;
	protected Set<Path> outdirs;

	RelationFileMapping inMapping;
	RelationFileMapping outMapping; 
	

	protected HashMap<String, Path> references;


	public FileManager(RelationFileMapping infiles, Path output, Path scratch) {

		// output and scratch location
		this.outputroot = output;
		this.scratchroot = scratch;
		// create tmp root, other paths are relative
		this.tmproot = scratch.suffix(Path.SEPARATOR + "tmp");

		// copy input mapping
		this.inMapping = new RelationFileMapping();
		this.inMapping.putAll(infiles);

		// set output mapping
		this.outMapping = new RelationFileMapping();

		// keep track of directories
		// these are relative to outputroot and tmproot paths
		this.tempdirs = new HashSet<Path>();
		this.outdirs = new HashSet<Path>();

		// app specific path refrences
		references = new HashMap<>();
		
		// used to make unique directories
		this.counter = 0;

	}

	public Path getNewTmpPath(String suffix) {

		Path tmp = tmproot.suffix(Path.SEPARATOR + TMP_PREFIX + (counter++) + "_" + suffix);
		tempdirs.add(tmp);

		return tmp;

	}
	
	/**
	 * Create a new temp path, but add references to it for easy lookup later.
	 * 
	 * @param suffix
	 * @param labels set of references to link to the path
	 * 
	 * @return a new tmp path in the tmp dir
	 */
	public Path getNewTmpPath(String suffix, HashSet<String> labels) {

		Path tmp = getNewTmpPath(suffix);
		
		for (String label : labels) {
			addReference(label, tmp);
		}

		return tmp;

	}
	
	public void addReference(String label, Path tmp) {
		references.put(label, tmp);
	}
	
	public Path getReference(String label) {
		return references.get(label);
	}

	public Path getNewOutPath(String suffix) {

		Path out = outputroot.suffix(Path.SEPARATOR + OUT_PREFIX + (counter++) + "_" + suffix);
		outdirs.add(out);

		return out;

	}
	
	public Path getOutputRoot() {
		return outputroot;
	}



	/**
	 * @see java.lang.Object#toString()
	 * @return String representation of the mapping from relationschemas to
	 *         paths
	 */
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();


		sb.append("Out root: " + outputroot + System.lineSeparator());
		sb.append("Scratch root: " + scratchroot + System.lineSeparator());
		sb.append("Temp root: " + tmproot + System.lineSeparator());

		for (RelationSchema rs : inMapping.getSchemas()) {
			sb.append(rs + " <- " + inMapping.getPaths(rs));
			sb.append(System.lineSeparator());
		}

		for (RelationSchema rs : outMapping.getSchemas()) {
			sb.append(rs + " -> " + outMapping.getPaths(rs));
			sb.append(System.lineSeparator());
		}

		sb.append("Temp dirs: " + System.lineSeparator());
		for (Path tmpDir : tempdirs) {
			sb.append(tmpDir);
			sb.append(System.lineSeparator());
		}
		return sb.toString();
	}

	public void updatePath(RelationSchema rs, Path p) {
		inMapping.addPath(rs, p);
	}


	/**
	 * Returns a mapping between the used relations and paths, both input and output.
	 * @return a mapping between relations and paths
	 */
	public RelationFileMapping getFileMapping(){
		return inMapping.combine(outMapping);
	}
	
	/**
	 * Creates a copy of the input mapping.
	 * @return a copy of the input mapping
	 */
	public RelationFileMapping getInFileMapping() {
		RelationFileMapping newmap = new RelationFileMapping();
		newmap.putAll(inMapping);
		return newmap;
	}
	
	/**
	 * Creates a copy of the output mapping.
	 * @return a copy of the output mapping
	 */
	public RelationFileMapping getOutFileMapping() {
		RelationFileMapping newmap = new RelationFileMapping();
		newmap.putAll(outMapping);
		return newmap;
	}


	/**
	 * Creates a new path for the output relation in the out directory
	 * and adds it to the internal file mapping.
	 * @param rs an output relation
	 */
	public void addOutRelation(RelationSchema rs) {

		Path p = getNewOutPath(rs.getName());
		outMapping.addPath(rs, p);

	}


	// Path getters
	/**
	 * @return an unmodifiable collection containing all the temp locations
	 */
	public Collection<Path> getTempPaths() {
		return Collections.unmodifiableSet(tempdirs);
	}

	/**
	 * @return an unmodifiable collection containing all the output locations
	 */
	public Collection<Path> getOutPaths() {
		return Collections.unmodifiableSet(outdirs);
	}

	/**
	 * @return an unmodifiable collection containing all the input paths
	 */
	public Collection<Path> getInDirs() {
		// input = all - output - temp
		Set<Path> set = inMapping.getAllPaths();
		set.removeAll(getTempPaths());
		set.removeAll(getOutPaths());
		return Collections.unmodifiableSet(set);
	}

	/**
	 * @return an new collection containing all the paths that appear in the mapping
	 */
	public Collection<Path> getAllPaths() {
		Set<Path> all = inMapping.getAllPaths();
		all.addAll(getOutPaths());
		all.addAll(getTempPaths());
		return all;
	}



}
