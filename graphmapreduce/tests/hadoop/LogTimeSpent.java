/**
 * Created: 27 Jan 2015
 */
package hadoop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author jonny
 *
 */
public class LogTimeSpent {
	

	private static final Log LOG = LogFactory.getLog(LogTimeSpent.class);
	
	public static void main(String[] args) {
		
		long start = System.nanoTime();
		for (int i = 0 ; i < 100000; i ++) {
			for (int j = 0 ; j < 10; j ++) {
				LOG.info("test" + new String(""+new Long(i)));
			}
		}

		long end = System.nanoTime();
		System.out.println((end - start)/1000000.0);
		
		
	}

}
