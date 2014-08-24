/**
 * Created: 12 May 2014
 */
package mapreduce.guardedfragment.planner.compiler;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import mapreduce.guardedfragment.planner.calculations.CalculationUnitDAG;
import mapreduce.guardedfragment.planner.structures.data.RelationSchema;

import org.apache.hadoop.fs.Path;

/**
 * Creates unique path names inside a given empty parent folder.
 * 
 * @author Jonny Daenen
 * 
 *         TODO add unit tests
 */
public class DirManager {

	protected Path input;
	protected Path output;
	protected Path scratch;
	protected CalculationUnitDAG dag;
	protected Path intdir;
	protected Path tmpdir;

	protected int counter;
	protected final static String TMP_PREFIX = "TMP_";
	protected final static String OUT_PREFIX = "OUT_";

	protected Set<Path> tempdirs;
	protected Set<Path> outdirs;

	Map<RelationSchema, Path> filemapping;

	/**
	 * @param root
	 *            folder location that serves as parent directory to empty
	 *            folders
	 */
	public DirManager(CalculationUnitDAG dag, Path input, Path output, Path scratch) {

		this.input = input;
		this.output = output;
		this.scratch = scratch;
		this.dag = dag;

		this.intdir = scratch.suffix(Path.SEPARATOR + "intermediate");
		this.tmpdir = scratch.suffix(Path.SEPARATOR + "tmp");

		this.filemapping = new HashMap<RelationSchema, Path>();
		this.tempdirs = new HashSet<Path>();
		this.outdirs = new HashSet<Path>();

		this.counter = 0;

		fillFileMap();
	}

	public Path getNewTmpPath(String suffix) {

		Path tmp = tmpdir.suffix(Path.SEPARATOR + TMP_PREFIX + (counter++) +"_" + suffix);
		tempdirs.add(tmp);

		return tmp;

	}

	public Path getNewOutPath(String suffix) {

		Path out = tmpdir.suffix(Path.SEPARATOR + OUT_PREFIX + (counter++)+"_" + suffix);
		outdirs.add(out);

		return out;

	}

	/**
	 * Constructs the set of paths where the relations can be found.
	 * 
	 * 
	 * @param relations
	 *            the set of relations to look up
	 * @return the set of Paths where the relations are located
	 * 
	 * @pre all relationschemas used are loaded
	 */
	public Set<Path> lookup(Set<RelationSchema> relations) {
		Set<Path> result = new HashSet<Path>();

		for (RelationSchema rs : relations) {
			if (filemapping.containsKey(rs))
				result.add(filemapping.get(rs));
		}

		return result;
	}

	/**
	 * Creates a mapping from relations to locations (folders/files) on disk.
	 * All input relations are mapped to the input path, output relations are
	 * mapped to the output path, intermediate relations are mapped into a
	 * separate folder in the scratch dir.
	 * 
	 * @see CalculationCompiler.getFolder for the folder naming.
	 * 
	 * 
	 * @pre partitionedDAG contains no overlap in input,intermediate and output
	 *      (correct implementation of getter functions :))
	 */
	private void fillFileMap() {

		// input relations
		for (RelationSchema rs : dag.getInputRelations()) {
			filemapping.put(rs, input);
		}

		// intermediate relations
		for (RelationSchema rs : dag.getIntermediateRelations()) {
			filemapping.put(rs, getIntermediateFolder(scratch, rs));
		}

		// output relations
		for (RelationSchema rs : dag.getOutputRelations()) {
			filemapping.put(rs, output);
		}

	}

	/**
	 * Constructs a path representing a folder inside the working directory. The
	 * folder name is a concatenation of the schema name and its arity.
	 * 
	 * @param outputSchema
	 * @return unique path for the given schema inside the working directory
	 */
	private Path getIntermediateFolder(Path scratchdir, RelationSchema rs) {
		return intdir.suffix(Path.SEPARATOR + rs.getName() + rs.getNumFields());
	}

	/**
	 * 
	 * @return a view on the set of generated temporary dirs
	 */
	public Set<Path> getTempDirs() {
		return Collections.unmodifiableSet(tempdirs);
	}
	
	/**
	 * 
	 * @return a view on the set of generated output dirs
	 */
	public Set<Path> getOutDirs() {
		return Collections.unmodifiableSet(outdirs);
	}

	/**
	 * @see java.lang.Object#toString()
	 * @return String representation of the mapping from relationschemas to
	 *         paths
	 */
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (RelationSchema rs : filemapping.keySet()) {
			sb.append(rs + " -> " + filemapping.get(rs));
			sb.append(System.lineSeparator());
		}
		return sb.toString();
	}

	public void updatePath(RelationSchema rs, Path p) {
		filemapping.put(rs, p);
	}

}
