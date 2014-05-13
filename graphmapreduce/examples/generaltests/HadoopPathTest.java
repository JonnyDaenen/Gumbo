/**
 * Created: 12 May 2014
 */
package generaltests;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

/**
 * @author Jonny Daenen
 *
 */
public class HadoopPathTest {
	
	public static void main(String[] args) throws IOException {
		Path p = new Path("file:/Users/jonny/");
		
	

		System.out.println(p);
		System.out.println(p.suffix(Path.SEPARATOR + "UHasselt"));
		System.out.println(p);
	}

}
