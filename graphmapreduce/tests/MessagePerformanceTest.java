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

public class MessagePerformanceTest {

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
		byte[] ones = ":1".getBytes();
		byte[] twos = ":2".getBytes();
		for (int i = 0; i < 100000; i++) {
			w1.set("A123-341");
			w1.append(ones, 0, 1);
			w1.append(twos, 0, 1);
			w2.set("A123-342");
			w2.append(twos, 0, 1);
			w2.append(ones, 0, 1);
			byte[] b1 = GumboMessageTest.serialize(w1);
			byte[] b2 = GumboMessageTest.serialize(w2);
			comparator.compare(b1, 0, b1.length, b2, 0, b2.length);
			GumboMessageTest.deserialize(w1, b1);
			GumboMessageTest.deserialize(w2, b2);
			String s1 = w1.toString();
			String s2 = w2.toString();
		}
	}
	
	public static void testTextW() {
		RawComparator<Text> comparator =
				WritableComparator.get(Text.class);

		Text w1 = new Text("A123-341:1;2");
		Text w2 = new Text("A123-342:1;2"); 
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
		
		for (int i = 0; i < 100000; i++) {
			w1.set(type,fileid,offset,atomids1);
			w2.set(type,fileid,offset,atomids2);
			byte[] b1 = GumboMessageTest.serialize(w1);
			byte[] b2 = GumboMessageTest.serialize(w2);
			comparator.compare(b1, 0, b1.length, b2, 0, b2.length);
			GumboMessageTest.deserialize(w1, b1);
			GumboMessageTest.deserialize(w2, b2);
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
