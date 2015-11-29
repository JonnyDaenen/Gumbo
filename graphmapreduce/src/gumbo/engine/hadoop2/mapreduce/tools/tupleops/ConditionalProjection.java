package gumbo.engine.hadoop2.mapreduce.tools.tupleops;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;

import gumbo.engine.hadoop2.datatypes.GumboMessageWritable;
import gumbo.engine.hadoop2.mapreduce.tools.QuickWrappedTuple;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.io.Pair;


/**
 * Tuple filter that checks equality between fields.
 * 
 * @author Jonny Daenen
 *
 */
public class ConditionalProjection implements TupleProjection {


	long fileid;
	String name;
	byte [] keyEt;
	byte [] fields;
	byte [] atomIds;
	
	EqualityFilter ef;
	

	public ConditionalProjection() {
		ef = new EqualityFilter(0);
	}
	

	public void initialize(String representation) {
		String[] parts = representation.split(":");

		name = parts[0];

		keyEt = convert(parts[1]);
		fields = convert(parts[1]);
		atomIds = convert(parts[1]);
		
		ef.set(fields);
	}


	private byte[] convert(String s) {
		String[] parts = s.split(",");
		byte [] result = new byte[parts.length];
		int i = 0;
		for (String part : parts) {
			result[i++] = (byte) Integer.parseInt(part);
		}
		return result;
	}
	
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(name);
		sb.append(":");
		addArray(sb, keyEt);
		sb.append(":");
		addArray(sb, fields);
		sb.append(":");
		addArray(sb, atomIds);
		
		return sb.toString();
		
	};
	
	private void addArray(StringBuffer sb, byte[] a) {
		int i;
		for (i = 0; i < a.length; i++) {
			sb.append((int)a[i]);
			sb.append(",");
		}
		if (i > 0)
			sb.deleteCharAt(i-1);
		
	}


	@Override
	public int hashCode() {
		return name.hashCode() ^ Arrays.hashCode(fields) ^ Arrays.hashCode(keyEt) ^ Arrays.hashCode(atomIds); 
	}
	
	
	public boolean equals(Object obj) {
		if (obj instanceof ConditionalProjection) {
			ConditionalProjection et = (ConditionalProjection) obj;
			return Arrays.equals(fields,et.fields) &&
					Arrays.equals(keyEt,et.keyEt) &&
					Arrays.equals(atomIds,et.atomIds) &&
					name.equals(et.name);
		}
		return false;
		
	}


	/**
	 * Checks whether the current tuple conforms to the guard expression
	 * and, if so, prepares the writables.
	 * The BytesWritable will contain a projection to the correct attributes,
	 * the GumboMessage will be a request message containing the tuple address
	 * and the requested atom ids.
	 * 
	 *  @return true iff writables should be written to output
	 */
	@Override
	public boolean load(QuickWrappedTuple qt, long offset, BytesWritable bw, GumboMessageWritable gw) {
		
		// if tuple conforms to fields
		if (!ef.check(qt))
			return false;
		
		// output key
		qt.project(fields, bw);
		
		// output atom ids
		gw.setRequest(fileid, offset, atomIds, atomIds.length);
		
		return false;
	};
	


	

}
