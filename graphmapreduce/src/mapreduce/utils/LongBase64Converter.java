/**
 * Created: 23 Jan 2015
 */
package mapreduce.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.codec.binary.Base64;

/**
 * @author jonny
 *
 */
public class LongBase64Converter {
	
	private ByteArrayOutputStream baos;
	private DataOutputStream dos;
	
	/**
	 * 
	 */
	public LongBase64Converter() {
		baos = new ByteArrayOutputStream(Long.SIZE/8);
		dos = new DataOutputStream(baos);
	}
	
	
	
	public byte[] long2byte(long l) throws IOException
	{
		
		dos.writeLong(l);
		dos.flush();
		byte[] result=baos.toByteArray();  
		baos.reset();
		return Base64.encodeBase64(result);
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
