/**
 * Created: 23 Jan 2015
 */
package hadoop;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.codec.binary.Base64;

import gumbo.utils.LongBase64Converter;

/**
 * @author jonny
 *
 */
public class LongEncoding {

	public static void main(String[] args) throws IOException {
		byte[] encodedBytes = Base64.encodeBase64("Test".getBytes());
		System.out.println("encodedBytes " + new String(encodedBytes));
		byte[] decodedBytes = Base64.decodeBase64(encodedBytes);
		System.out.println("decodedBytes " + new String(decodedBytes));	

		byte[] encodedBytes2 = Base64.encodeBase64(long2byte(10000000L));
		System.out.println("encodedBytes " + new String(encodedBytes2));
		
		byte[] decodedBytes2 = Base64.decodeBase64(new String(encodedBytes2).getBytes());
		System.out.println("decodedBytes " + byte2long(decodedBytes2));	

		
		LongBase64Converter converter = new LongBase64Converter();
		for (long i =1; i < Long.MAX_VALUE/(7*7*7*7); i*=7) {
			convert(converter, i);
		}
	}

	/**
	 * @param converter
	 * @param i
	 * @throws IOException 
	 */
	private static void convert(LongBase64Converter converter, long l) throws IOException {
		System.out.println("Original: "+ l);
		
		byte [] buf = converter.long2byte(l);
		System.out.println("Encoded: " + new String(buf));
		
		long recovered = converter.byte2long(buf);
		System.out.println("Decoded: " + recovered);
		
		System.out.println("Test ok: " + (recovered == l));
		
		System.out.println();
	}

	public static byte[] long2byte(long l) throws IOException
	{
		ByteArrayOutputStream baos=new ByteArrayOutputStream(Long.SIZE/8);
		DataOutputStream dos=new DataOutputStream(baos);
		dos.writeLong(l);
		dos.flush();
		byte[] result=baos.toByteArray();
		dos.close();    
		return result;
	}


	public static long byte2long(byte[] b) throws IOException
	{
		ByteArrayInputStream baos=new ByteArrayInputStream(b);
		DataInputStream dos=new DataInputStream(baos);
		long result=dos.readLong();
		dos.close();
		return result;
	}


}
