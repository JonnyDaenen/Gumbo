package gumbo.engine.hadoop2.mapreduce.tools.tupleops;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import gumbo.engine.hadoop2.mapreduce.tools.QuickWrappedTuple;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.io.Pair;


/**
 * Tuple filter that checks equality between fields.
 * 
 * @author Jonny Daenen
 *
 */
public class EqualityType implements TupleFilter {


	String name;
	byte [] keyEt;
	byte [] fields;
	byte [] atomIds;
	
	List<Pair<Integer,Integer>> comparisons;

	public EqualityType(int sizeRequirement) {
		comparisons = new ArrayList<>(10);
	}
	

	public void initialize(String representation) {
		String[] parts = representation.split(":");

		name = parts[0];

		keyEt = convert(parts[1]);
		fields = convert(parts[1]);
		atomIds = convert(parts[1]);

		// TODO add comparisons?
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
		if (obj instanceof EqualityType) {
			EqualityType et = (EqualityType) obj;
			return Arrays.equals(fields,et.fields) &&
					Arrays.equals(keyEt,et.keyEt) &&
					Arrays.equals(atomIds,et.atomIds) &&
					name.equals(et.name);
		}
		return false;
		
	};
	
	public boolean fieldsConform(EqualityType et) {
		return Arrays.equals(fields,et.fields);
	}


	@Override
	public boolean check(QuickWrappedTuple qt) {
		// TODO Auto-generated method stub
		return false;
	}


	

}
