/**
 * Created: 05 Feb 2015
 */
package gumbo.engine.hadoop.converter;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

/**
 * Converts directories and globs to real files.
 * 
 * @author Jonny Daenen
 *
 */
public class InputPathExpander {

	protected FileSystem fs;

	public InputPathExpander() {
		Configuration conf = new Configuration();
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO what to do?

			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {
		InputPathExpander ipe = new InputPathExpander();
		LinkedList<Path> paths = new LinkedList<>();
		paths.add(new Path("input/experiments/EXP_005/R/R.txt")); // file
		paths.add(new Path("input/experiments/EXP_005/R/")); // dir
		paths.add(new Path("input/experiments/EXP_005/R")); //dir
		paths.add(new Path("input/experiments/EXP_005/*/*.txt")); // none
		paths.add(new Path("input/experiments/EXP_005/*")); // none
		paths.add(new Path("input/q4/1e04")); // dir
		paths.add(new Path("input/notexists")); // none



		for (Path p: paths) {
			ipe.report(p);
		}
	}


	/**
	 * @param p
	 * @throws IOException 
	 */
	private void report(Path p) throws IOException {
		System.out.println(p);
		System.out.println("exists: " + fs.exists(p));
		System.out.println("directory: " + fs.isDirectory(p));
		System.out.println("file: " + fs.isFile(p));
		//System.out.println("content: " + fs.getContentSummary(p));
		System.out.println("files: " + expand(p));
		System.out.println();


	}


	/**
	 * 
	 * @param set
	 * @return
	 */
	Collection<Path> expand(Collection<Path> set) {
		HashSet<Path> result = new HashSet<>();

		for (Path p : set) {
			expand(p,result);
		}

		return result;

	}

	/**
	 * @param p
	 * @param result
	 */
	private void expand(Path p, HashSet<Path> result) {

		try {
			if (fs.isFile(p) || fs.isDirectory(p) ) {
				FileStatus [] files = fs.listStatus(p);
				// add "good" files to result
				addFiles(files, result);
			} else {
				
				// if it is a glob, we expand and process one more time as files and directories
				FileStatus [] files = fs.globStatus(p);
				Path[] paths = FileUtil.stat2Paths(files);
				
				if (paths == null)
					return;
				
				for ( Path p2 : paths) {
					expand(p2,result);
				}
			}
		} catch ( IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * Adds files to a set, filtering out non-files and 0-size files.
	 * @param files
	 * @param result
	 * @throws IOException 
	 */
	private void addFiles(FileStatus[] files, HashSet<Path> result) throws IOException {
		for ( FileStatus f : files) {
			Path p = f.getPath();
			if (f.isFile()  && f.getBlockSize() > 0 && fs.exists(p))
				result.add(p);
		}
		
		
	}

	/**
	 * Expands an input path into a set of absolute paths.
	 * When the paths is either a directory or a glob,
	 * they are expanded, when it is a file, it is unchanged
	 * (apart from being converted to a absolute file).
	 * 
	 * Expansion of <b>directories</b> happens by listing all the files in the directory.
	 * Expansion of <b>globs</b> evaluates the glob to a set of paths, and then applies
	 * the above algorithm to the result.
	 * 
	 * 
	 * @param p a path
	 * 
	 * @return a set of absolute expanded paths
	 */
	public Collection<Path> expand(Path p) {
		HashSet<Path> result = new HashSet<>();
		expand(p,result);
		return result;
	}

}
