/**
 * Created: 12 May 2014
 */
package gumbo.compiler.filemapper;

import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.compiler.structures.data.RelationSchema;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.Path;

/**
 * Bookkeeping for input/output locations,
 * coupled to relations. 
 * The relative paths are stored, TODO #core no, they are stored absolute!
 *  making it possible to change the output and scratch roots afterwards.
 * TODO check
 * TODO #core add estimates?
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

	RelationFileMapping filemapping;


	public FileManager(RelationFileMapping infiles, Path output, Path scratch) {

		// output and scratch location
		this.outputroot = output;
		changeScratch(scratch);

		// copy input mapping
		this.filemapping = new RelationFileMapping();
		this.filemapping.putAll(infiles, true);

		// keep track of directories
		// these are relative to outputroot and tmproot paths
		this.tempdirs = new HashSet<Path>();
		this.outdirs = new HashSet<Path>();

		// used to make unique directories
		this.counter = 0;

	}
	
	/**
	 * Should only be called once as paths are stored absolute.
	 * @param scratch
	 */
	private void changeScratch(Path scratch) {
		this.scratchroot = scratch;
		// also change tmp root, other paths are relative
		this.tmproot = scratch.suffix(Path.SEPARATOR + "tmp");
	}

	public Path getNewTmpPath(String suffix) {

		Path tmp = tmproot.suffix(Path.SEPARATOR + TMP_PREFIX + (counter++) + "_" + suffix);
		tempdirs.add(tmp);

		return tmp;

	}

	public Path getNewOutPath(String suffix) {

		Path out = outputroot.suffix(Path.SEPARATOR + OUT_PREFIX + (counter++) + "_" + suffix);
		outdirs.add(out);

		return out;

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

		for (RelationSchema rs : filemapping.getSchemas()) {
			sb.append(rs + " <-> " + filemapping.getPaths(rs));
			sb.append(System.lineSeparator());
		}
		
		return sb.toString();
	}

	public void updatePath(RelationSchema rs, Path p) {
		filemapping.addPath(rs, p);
	}

	/**
	 * @return the default input path
	 */
	@Deprecated
	public Object getDefaultInputPath() {
		return filemapping.getDefaultPath();
	}
	
	
	/**
	 * Returns a mapping between the used relations and paths.
	 * @return a mapping between relations and paths
	 */
	@Deprecated
	public RelationFileMapping getFileMapping(){
		return filemapping;
	}

	/**
	 * Creates a new path for the intermediate relation in the
	 * temp directory (which resides in the scratch directory)
	 * and adds it to the internal file mapping.
	 * @param rs an intermediate relation
	 */
	public void addTempRelation(RelationSchema rs) {
		
		Path p = getNewTmpPath(rs.getName());
		filemapping.addPath(rs, p);
		
	}

	/**
	 * Creates a new path for the output relation in the out directory
	 * and adds it to the internal file mapping.
	 * @param rs an output relation
	 */
	public void addOutRelation(RelationSchema rs) {
		
		Path p = getNewOutPath(rs.getName());
		filemapping.addPath(rs, p);
		
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
		Set<Path> set = filemapping.getAllPaths();
		set.removeAll(getTempPaths());
		set.removeAll(getOutPaths());
		return Collections.unmodifiableSet(set);
	}

	/**
	 * @return an unmodifiable collection containing all the paths that appear in the mapping
	 */
	public Collection<Path> getAllPaths() {
		return Collections.unmodifiableSet(filemapping.getAllPaths());
	}
	
	
	
	

}
