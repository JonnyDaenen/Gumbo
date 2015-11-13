/**
 * Created: 10 Feb 2015
 */
package gumbo.engine.general.utils;

import gumbo.compiler.filemapper.FileManager;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.structures.data.RelationSchema;

import java.util.Collection;

import org.apache.hadoop.fs.Path;

/**
 * Extracts a file mapping from a {@link FileManager}, possibly based on a set of
 * given {@link RelationSchema}s (FUTURE). For input relations, the paths are 
 * expanded when they are directories/globs and retained when they are normal files.
 * This means that the extractor has to run when the input files are all available.
 * 
 * @author Jonny Daenen
 *
 */
public class FileMappingExtractor {


	InputPathExpander expander;
	private boolean includeOut;

	public FileMappingExtractor() {
		this(true);
	}

	public FileMappingExtractor(boolean includeOutputDirs) {
		expander = new InputPathExpander();
		setIncludeOutputDirs(includeOutputDirs);
	}

	public void setIncludeOutputDirs(boolean includeOutputDirs){
		includeOut = includeOutputDirs;
	}

	/**
	 * Extracts a mapping from a {@link FileManager} and expands the input paths
	 * into file paths.
	 * 
	 * @param fm a file manager
	 * 
	 * @return expanded mapping, extracted from the file manager 
	 */
	public RelationFileMapping extractFileMapping(FileManager fm) {
		RelationFileMapping ins = fm.getInFileMapping();
		RelationFileMapping outs = fm.getOutFileMapping();

		RelationFileMapping expandedIns = expand(ins);

		if (includeOut) {
			System.out.println("Adding output paths");
			expandedIns.putAll(expand(outs));
		}
		return expandedIns;
	}

	/**
	 * Expands the {@link Path}s inside a given {@link RelationFileMapping}.
	 * 
	 * @param ins the mapping to expand
	 * 
	 * @return a new expanded mapping
	 */
	private RelationFileMapping expand(RelationFileMapping ins) {

		RelationFileMapping newMapping = new RelationFileMapping();

		for (RelationSchema rs : ins.getSchemas()){
			Collection<Path> expandedPaths = expander.expand(ins.getPaths(rs));
			for (Path p : expandedPaths) {
				newMapping.addPath(rs, p);
			}
			newMapping.setFormat(rs, ins.getFormat(rs));
//			System.out.println(rs + " " + ins.getFormat(rs));
		}
		return newMapping;
	}

	
	public RelationFileMapping extractFileMapping(FileManager fm, CalculationUnitGroup cug) {
		RelationFileMapping ins = fm.getInFileMapping();
		RelationFileMapping outs = fm.getOutFileMapping();

		RelationFileMapping all = expand(ins);

		if (includeOut)
			all.putAll(outs);

		RelationFileMapping result = new RelationFileMapping();
		for (RelationSchema rs : all.getSchemas()){
			if (cug.getInputRelations().contains(rs)) {
				for (Path p : all.getPaths(rs)) {
					result.addPath(rs, p, all.getInputFormat(rs));
				}
			}
		}

		return result;
	}

}
