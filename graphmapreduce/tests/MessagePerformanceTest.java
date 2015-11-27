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
import gumbo.engine.hadoop2.datatypes.GumboMessageWritable2;
import gumbo.engine.hadoop2.datatypes.VLongPair;
import scala.tools.scalap.scalax.rules.ArrayInput;

public class MessagePerformanceTest {

	static int numtests = 1000000;
	public static void main(String[] args) throws IOException, InterruptedException {

		Thread.sleep(10000);
		System.out.println("started");
		testVLongPairW();
		System.out.println("longpair done");
		testGumboMessageW2();
		System.out.println("gumbo done");
		testGumboMessageW();
		System.out.println("gumbo done");
		testTextWBytes();
		System.out.println("text done");
		System.out.println("done");

	}



	private static void testGumboMessageW2() throws IOException {

		RawComparator<BytesWritable> comparator =
				WritableComparator.get(GumboMessageWritable2.class);

		byte type = GumboMessageWritable2.GumboMessageType.REQUEST;
		long fileid = 120;
		long offset1 = 33000;
		long offset2 = 3000;
		byte [] atomids1 = {1,2,3,4,5};
		byte [] atomids2 = {1,2,3,4,6};


		GumboMessageWritable2 w1 = new GumboMessageWritable2(type,fileid,offset1,atomids1);
		GumboMessageWritable2 w2 = new GumboMessageWritable2(type,fileid,offset2,atomids2);
		byte[] b1 = GumboMessageTest.serialize(w1);
		byte[] b2 = GumboMessageTest.serialize(w2);

		for (int i = 0; i < numtests; i++) {
			comparator.compare(b1, 0, b1.length, b2, 0, b2.length);
			GumboMessageTest.deserialize(w1, b1);
			GumboMessageTest.deserialize(w2, b2);
			calc(w1);
			calc(w2);
			b1 = GumboMessageTest.serialize(w1);
			b2 = GumboMessageTest.serialize(w2);
		}
		
	}



	private static void calc(GumboMessageWritable2 w) {
		long result = w.getFileId() + w.getOffset();
		if (w.containsAtomId((byte) 1)) {
			result += 1;
		}
	}



	private static void testVLongPairW() throws IOException {
		RawComparator<Text> comparator =
				WritableComparator.get(VLongPair.class);

		VLongPair w1 = new VLongPair(1000,2000);
		VLongPair w2 = new VLongPair(1000,2000);

		byte[] b1 = GumboMessageTest.serialize(w1);
		byte[] b2 = GumboMessageTest.serialize(w2);

		for (int i = 0; i < numtests; i++) {
			comparator.compare(b1, 0, b1.length, b2, 0, b2.length);
			GumboMessageTest.deserialize(w1, b1);
			GumboMessageTest.deserialize(w2, b2);
//			calc(w1);
//			calc(w2);
			b1 = GumboMessageTest.serialize(w1);
			b2 = GumboMessageTest.serialize(w2);
		}
	}



	private static void calc(VLongPair w) {
		long result = w.getFirst() + w.getSecond();
		
	}



	public static void testTextWBytes() throws IOException {
		RawComparator<Text> comparator =
				WritableComparator.get(Text.class);

		
		Text w1 = new Text("FecFec -3;1,2");
		Text w2 = new Text("FecFec -2;1,2");
//		Text w1 = new Text("FecFec -3");
//		Text w2 = new Text("FecFec -2");

		byte[] b1 = GumboMessageTest.serialize(w1);
		byte[] b2 = GumboMessageTest.serialize(w2);

		for (int i = 0; i < numtests; i++) {
			comparator.compare(b1, 0, b1.length, b2, 0, b2.length);
			GumboMessageTest.deserialize(w1, b1);
			GumboMessageTest.deserialize(w2, b2);
			calc(w1);
			calc(w2);
			b1 = GumboMessageTest.serialize(w1);
			b2 = GumboMessageTest.serialize(w2);
		}
	}



	public static void testGumboMessageW() throws IOException {


		RawComparator<BytesWritable> comparator =
				WritableComparator.get(GumboMessageWritable.class);

		byte type = GumboMessageWritable.GumboMessageType.REQUEST;
		long fileid = 120;
		long offset1 = 33000;
		long offset2 = 3000;
		byte [] atomids1 = {1,2,3,4,5};
		byte [] atomids2 = {1,2,3,4,6};


		GumboMessageWritable w1 = new GumboMessageWritable(type,fileid,offset1,atomids1);
		GumboMessageWritable w2 = new GumboMessageWritable(type,fileid,offset2,atomids2);
		byte[] b1 = GumboMessageTest.serialize(w1);
		byte[] b2 = GumboMessageTest.serialize(w2);

		for (int i = 0; i < numtests; i++) {
			comparator.compare(b1, 0, b1.length, b2, 0, b2.length);
			GumboMessageTest.deserialize(w1, b1);
			GumboMessageTest.deserialize(w2, b2);
			calc(w1);
			calc(w2);
			b1 = GumboMessageTest.serialize(w1);
			b2 = GumboMessageTest.serialize(w2);
		}
	}



	private static void calc(GumboMessageWritable w) {
		long result = w.getFileId() + w.getOffset();
		if (w.containsAtomId((byte) 1)) {
			result += 1;
		}
	}



	private static void calc(Text w) {
		String atoms = w.toString().split(";")[1];
		String [] atomids = atoms.split(",");
		for (int i = 0 ; i < atomids.length; i++) {
			if (Integer.parseInt(atomids[i]) == 2) {
				break;
			}
		}
	}





}
