/**
 * Created on: 27 Apr 2015
 */
package hadoop;

import gumbo.utils.LongBase64Converter;

import java.io.IOException;

/**
 * @author jonny
 *
 */
public class LongEncoding2 {
	
	public static void main(String[] args) throws IOException {
		
		LongBase64Converter converter = new LongBase64Converter();
		
		long start = System.nanoTime();
		long k = 1;
		int i = 0;
		for (i = 0; i < 50000000; i++) {
			k += 1;
//			converter.long2byte(k);
			newconvert(k);
//			System.out.println(new String());
		}
		long stop = System.nanoTime();
		
		System.out.println((stop - start)/1000000 + "ms");
	}
	
	static byte [] helper = new byte[11];
	static byte [] newconvert(long l){
		byte[] result = new byte[11];
		for (int i = 0; i < 11; i++) {
			if (l == 0)
				result[i] = '0';
			else {
				result[i] = convert6bit((l % 64));
				l = l >> 6;
			}
		}
		return result;
	}

	/**
	 * @param l
	 * @return
	 */
	private static byte convert6bit(long l) {
		if ( l < 26 )
			l = 'A' + l;
		else if ( l < 52)
			l = 'a' + l - 26;
		else if ( l < 62)
			l = '0' + l - 52;
		else if (l == 62)
			l = '+';
		else
			l = '/';
			
		return (byte)l;
	}

}
