import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import gumbo.engine.hadoop2.datatypes.GumboMessageWritable;

public class GumboMessageTest {

	public static void main(String[] args) throws IOException {
		
		byte type = GumboMessageWritable.GumboMessageType.REQUEST;
		long fileid = 120;
		long offset = 33000;
		byte [] atomids = {1,2,3,4,5};
		
		
		GumboMessageWritable gw = new GumboMessageWritable(type,fileid,offset,atomids);
		System.out.println(gw);
		byte[] bytes = serialize(gw);
		System.out.println(bytes.length);
		deserialize(gw, bytes);
		System.out.println(gw);

	}


	public static byte[] serialize(Writable writable) throws IOException { 
		ByteArrayOutputStream out = new ByteArrayOutputStream(); 
		DataOutputStream dataOut = new DataOutputStream(out); 
		writable.write(dataOut);
		dataOut.close();
		return out.toByteArray(); 
	}

	public static byte[] deserialize(Writable writable, byte[] bytes) throws IOException {
		ByteArrayInputStream in = new ByteArrayInputStream(bytes); 
		DataInputStream dataIn = new DataInputStream(in); 
		writable.readFields(dataIn);
		dataIn.close();
		return bytes; 
	}

}
