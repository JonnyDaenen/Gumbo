package mapreduce.hadoop;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

public class HDFSHelloWorld {

	public static final String theFilename = "hello.txt";
	public static final String message = "Hello, world!\n";

	public static void main(String[] args) throws IOException {

		
	
		

		Path localPath = new Path("file:/");
		Path hdfsPath = new Path("hdfs:/");
		
		test(localPath);
		test(hdfsPath);
		
		localPath = new Path("file:"+theFilename);
		hdfsPath = new Path("hdfs:"+theFilename);
		test2(localPath);
		test2(hdfsPath);
	}

	/**
	 * @param localPath
	 */
	private static void test(Path path) {

		Configuration conf = new Configuration();

//		String dir = conf.get("hadoop.tmp.dir");
//		System.out.println(dir);
		
		
		
		try {
			FileSystem fs = FileSystem.get(path.toUri(),conf);
			
			
			System.out.println(path);
			
			System.out.println(fs.getHomeDirectory());
			System.out.println(fs.getWorkingDirectory());
			path = new Path(fs.getWorkingDirectory().toString() +"/" + theFilename);
			System.out.println(path);
			
			if (fs.exists(path)) {
				// remove the file first
				fs.delete(path,true);
			}
			
			// local file system temp dir
//			dir = conf.get("hadoop.tmp.dir");
//			System.out.println(dir);

			FSDataOutputStream out = fs.create(path);
			out.writeUTF(message);
			out.close();

			FSDataInputStream in = fs.open(path);
			String messageIn = in.readUTF();
			System.out.print(messageIn);
			in.close();
		} catch (IOException ioe) {
			System.err.println("IOException during operation: " + ioe.toString());
		}
	}
	
	private static void test2(Path path) {

		Configuration conf = new Configuration();

//		String dir = conf.get("hadoop.tmp.dir");
//		System.out.println(dir);
		
		
		
		try {
			FileSystem fs = FileSystem.get(path.toUri(),conf);
			
			
			System.out.println(path);
			
			System.out.println(fs.getHomeDirectory());
			System.out.println(fs.getWorkingDirectory());
			
			if (fs.exists(path)) {
				// remove the file first
				fs.delete(path,true);
			}
			
			// local file system temp dir
//			dir = conf.get("hadoop.tmp.dir");
//			System.out.println(dir);

			FSDataOutputStream out = fs.create(path);
			out.writeUTF(message);
			out.close();

			FSDataInputStream in = fs.open(path);
			String messageIn = in.readUTF();
			System.out.print(messageIn);
			in.close();
		} catch (IOException ioe) {
			System.err.println("IOException during operation: " + ioe.toString());
		}
	}
}