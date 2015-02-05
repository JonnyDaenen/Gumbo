/**
 * Created: 05 Feb 2015
 */
package mapreduce.guardedfragment.executor.hadoop;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

/**
 * @author Jonny Daenen
 *
 */
public class InputPathExpander {

	private FileSystem fs;

	/**
	 * 
	 */
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
		paths.add(new Path("input/experiments/EXP_005/R/R.txt"));
		paths.add(new Path("input/experiments/EXP_005/R/"));
		paths.add(new Path("input/experiments/EXP_005/R"));
		paths.add(new Path("input/experiments/EXP_005/*/*.txt"));
		paths.add(new Path("input/experiments/EXP_005/*"));
		paths.add(new Path("input/q4/1e04"));
		paths.add(new Path("input/notexists"));



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
	 * @param files
	 * @param result
	 * @throws IOException 
	 */
	private void addFiles(FileStatus[] files, HashSet<Path> result) throws IOException {
		Path[] paths = FileUtil.stat2Paths(files);
		for ( Path p : paths) {
			if (fs.isFile(p))
				result.add(p);
		}
		
	}

	/**
	 * @param p
	 * @return
	 */
	public Collection<Path> expand(Path p) {
		HashSet<Path> result = new HashSet<>();
		expand(p,result);
		return result;
	}

}
