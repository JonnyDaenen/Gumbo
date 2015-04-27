/**
 * Created: 23 Jan 2015
 */
package gumbo.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.codec.binary.Base64;

/**
 * @author jonny
 *
 */
public class LongBase64Converter {
	
	private ByteArrayOutputStream baos;
	private DataOutputStream dos;
	private ByteBuffer b;
	
	/**
	 * 
	 */
	public LongBase64Converter() {
		baos = new ByteArrayOutputStream(Long.SIZE/8);
		dos = new DataOutputStream(baos);
		b = ByteBuffer.allocate(8);
	}
	
	
	
	public byte[] long2byte(long l) throws IOException
	{
		
//		b.clear();
//		b.putLong(l);
		
//		dos.writeLong(l);
//		dos.flush();
//		byte[] result=baos.toByteArray();  
//		baos.reset();
		return Base64.encodeBase64(longToByteArray(l));
	}
	
	public byte[] longToByteArray(long value) {
	    return new byte[] {
	        (byte) (value >> 56),
	        (byte) (value >> 48),
	        (byte) (value >> 40),
	        (byte) (value >> 32),
	        (byte) (value >> 24),
	        (byte) (value >> 16),
	        (byte) (value >> 8),
	        (byte) value
	    };
	}
	
	


	public long byte2long(byte[] b) throws IOException
	{
		byte [] b2 =Base64.decodeBase64(b);
		ByteArrayInputStream bais = new ByteArrayInputStream(b2);
		DataInputStream dis = new DataInputStream(bais);
		long result = dis.readLong();
		dis.close();
		return result;
	}

}
