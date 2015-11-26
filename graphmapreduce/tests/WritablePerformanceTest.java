import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;

import gumbo.engine.hadoop2.datatypes.GumboMessageWritable;
import scala.tools.scalap.scalax.rules.ArrayInput;

public class WritablePerformanceTest {

	public static void main(String[] args) throws IOException, InterruptedException {

		Thread.sleep(10000);
		System.out.println("started");
		testIntW();
		testIntWBytes();
		testTextW();
		testTextWBytes();
		testBytesW();
		testGumboMessageW();
		System.out.println("done");

	}
	
	
	public static void testTextWBytes() throws IOException {
		RawComparator<Text> comparator =
				WritableComparator.get(Text.class);

		Text w1 = new Text("1234,12314,234");
		Text w2 = new Text("1234,12314,235");
		byte[] b1 = GumboMessageTest.serialize(w1);
		byte[] b2 = GumboMessageTest.serialize(w2);
		for (int i = 0; i < 100000; i++) {
			
			comparator.compare(b1, 0, b1.length, b2, 0, b2.length);
		}
	}
	
	public static void testTextW() {
		RawComparator<Text> comparator =
				WritableComparator.get(Text.class);

		Text w1 = new Text("1234,12314,234");
		Text w2 = new Text("1234,12314,235");
		for (int i = 0; i < 100000; i++) {
			comparator.compare(w1, w2);
		}
	}
	
	public static void testGumboMessageW() throws IOException {
		

		RawComparator<BytesWritable> comparator =
				WritableComparator.get(GumboMessageWritable.class);
		
		byte type = GumboMessageWritable.GumboMessageType.REQUEST;
		long fileid = 120;
		long offset = 33000;
		byte [] atomids1 = {1,2,3,4,5};
		byte [] atomids2 = {1,2,3,4,6};
		

		GumboMessageWritable w1 = new GumboMessageWritable(type,fileid,offset,atomids1);
		GumboMessageWritable w2 = new GumboMessageWritable(type,fileid,offset,atomids2);
		
		byte[] b1 = GumboMessageTest.serialize(w1);
		byte[] b2 = GumboMessageTest.serialize(w2);
		
		for (int i = 0; i < 100000; i++) {
			comparator.compare(b1, 0, b1.length, b2, 0, b2.length);
		}
	}
	
	public static void testBytesW() throws IOException {
		RawComparator<BytesWritable> comparator =
				WritableComparator.get(BytesWritable.class);

		
		Text tw1 = new Text("1234,12314,234");
		Text tw2 = new Text("1234,12314,235"); 
		
		BytesWritable w1 = new BytesWritable();
		w1.set(tw1.getBytes(), 0, tw1.getLength());
		
		BytesWritable w2 = new BytesWritable();
		w1.set(tw2.getBytes(), 0, tw2.getLength());
		
		
		byte[] b1 = serialize(w1);
		byte[] b2 = serialize(w2);
		for (int i = 0; i < 100000; i++) {
			comparator.compare(b1, 0, b1.length, b2, 0, b2.length);
		}
	}

	public static void testIntWBytes() throws IOException {
		RawComparator<IntWritable> comparator =
				WritableComparator.get(IntWritable.class);

		IntWritable w1 = new IntWritable(163);
		IntWritable w2 = new IntWritable(67); 
		byte[] b1 = serialize(w1);
		byte[] b2 = serialize(w2);
		for (int i = 0; i < 100000; i++) {
			comparator.compare(b1, 0, b1.length, b2, 0, b2.length);
		}
	}

	public static void testIntW() {
		RawComparator<IntWritable> comparator =
				WritableComparator.get(IntWritable.class);

		IntWritable w1 = new IntWritable(163);
		IntWritable w2 = new IntWritable(67); 
		for (int i = 0; i < 100000; i++) {
			comparator.compare(w1, w2);
		}
	}

	public static byte[] serialize(Writable writable) throws IOException { 
		return GumboMessageTest.serialize(writable);
	}

}
