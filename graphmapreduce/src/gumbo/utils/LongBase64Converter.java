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
import java.util.Arrays;

import javax.xml.bind.DatatypeConverter;

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
		byte [] bytes = longToByteArray(l);
//		DatatypeConverterImpl._printBase64Binary(bytes, 0, 8, charbytes, 0);;
//		
//		for (int i = 0; i < 12; i++)
//			long64bytes[i] = (byte) charbytes[i];
//		
//		return long64bytes;
		
		return DatatypeConverter.printBase64Binary(bytes).getBytes();
//		return Base64.encodeBase64(longToByteArray(l));
	}
	
	public byte[] longToByteArray(long value) {
//		int x = 65;
//		for (int i = 0; i < 8; i++) {
//			longbytes[i] = (byte)(value >> x);
//			x -= 8;
//		}
//		
//		return longbytes;
				
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
	
	/**
	 * Converts a long to base 64 encoding.
	 * @param l the long to convert
	 * @return a new byte array containing the encoded long
	 */
	public byte [] newconvert(long l){
		byte[] result = new byte[11];
		for (int i = 0; i < 11; i++) {
			if (l == 0)
				result[i] = 'A'; // OPTIMIZE break here, but set at least one byte
			else {
				result[i] = convert6bit((l % 64));
				l = l >> 6;
			}
		}
		return result;
	}
	

	private byte[] buffer = new byte[11];
	/**
	 * Converts a long to base 64 encoding, with no trailing
	 * zero ('A') symbols, except when it is equal to zero.
	 * @param l the long to convert
	 * @return a new byte array containing the encoded long
	 */
	public byte [] newconvertNoPrefix(long l){
		int i;
		for (i = 0; i < 11; i++) {
			if (l == 0 && i > 0) {
				i++;
				break;
			}
			else {
				buffer[i] = convert6bit((l % 64));
				l = l >> 6;
			}
		}
		byte [] result = Arrays.copyOf(buffer, i);
		
		
		return result;
	}

	/**
	 * @param l
	 * @return
	 */
	private byte convert6bit(long l) {
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
