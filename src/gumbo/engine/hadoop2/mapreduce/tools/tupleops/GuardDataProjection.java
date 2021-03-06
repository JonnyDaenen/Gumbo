package gumbo.engine.hadoop2.mapreduce.tools.tupleops;

import java.util.Arrays;

import gumbo.engine.hadoop2.datatypes.GumboMessageWritable;
import gumbo.engine.hadoop2.datatypes.VBytesWritable;
import gumbo.engine.hadoop2.mapreduce.tools.QuickWrappedTuple;
import gumbo.structures.gfexpressions.GFAtomicExpression;


/**
 * 
 * @author Jonny Daenen
 *
 */
public class GuardDataProjection implements TupleProjection {


	String name;
	byte [] keyEt;
	EqualityType fields;
	
	EqualityFilter ef;
	
	byte [] queryids;
	

	public GuardDataProjection(String relation, GFAtomicExpression guard, GFAtomicExpression guarded, byte queryid) {
		this.name = relation;
		fields = new EqualityType(guard);
		this.queryids = new byte[1];
		queryids[0] = queryid;
		
		// extract key
		loadKey(guard, guarded);
		
		ef = new EqualityFilter(fields);
	}
	
	
	private GuardDataProjection(String relation, byte [] keyEt, EqualityType fields, byte [] queryids) {
		this.name = relation;
		this.fields = fields;
		this.queryids = queryids;
		this.keyEt = keyEt;
		
		ef = new EqualityFilter(fields);
	}
	

	private void loadKey(GFAtomicExpression guard, GFAtomicExpression guarded) {
		String [] gvars = guard.getVars();
		String [] vars = guarded.getVars();
		keyEt = new byte[vars.length];
		int i = 0;
		for (String var : vars) {
			for (int j = 0; j < gvars.length; j++)
				if (gvars[j].equals(var)){
					keyEt[i++] = (byte) fields.equality[j];
					break;
				}
		}
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(name);
		sb.append(":");
		addArray(sb, keyEt);
		sb.append(":");
		sb.append(fields);
		sb.append(":");
		addArray(sb, queryids);
		
		return sb.toString();
		
	}
	
	private void addArray(StringBuffer sb, byte[] a) {
		int i;
		for (i = 0; i < a.length; i++) {
			sb.append((int)a[i]);
			sb.append(",");
		}
		if (i > 0)
			sb.deleteCharAt(sb.length() - 1);
		
	}


	@Override
	public int hashCode() {
		return name.hashCode() ^ fields.hashCode() ^ Arrays.hashCode(keyEt); 
	}
	
	
	public boolean equals(Object obj) {
		if (obj instanceof GuardDataProjection) {
			GuardDataProjection et = (GuardDataProjection) obj;
			return fields.equals(et.fields) &&
					Arrays.equals(keyEt,et.keyEt) &&
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
	public boolean load(QuickWrappedTuple qt, long offset, VBytesWritable bw, GumboMessageWritable gw) {
		
		// if tuple conforms to fields
		if (!ef.check(qt))
			return false;
		
		// output key
		qt.project(keyEt, bw);
		
		// output atom ids
		gw.setDataRequest(queryids, qt.getData(), qt.getLength());
		
		return true;
	}


	@Override
	public boolean canMerge(TupleProjection pi) {
		if (pi instanceof GuardDataProjection) {
			GuardDataProjection pi2 = (GuardDataProjection) pi;
			return name.equals(pi2.name) 
					&& Arrays.equals(keyEt, pi2.keyEt) 
					&& fields.equals(pi2.fields);
		}
		return false;
	}


	@Override
	public TupleProjection merge(TupleProjection pi) {
		if (pi instanceof GuardDataProjection) {
			GuardDataProjection pi2 = (GuardDataProjection) pi;
			
			// concatenate atom ids
			byte [] newatomids = new byte [queryids.length + pi2.queryids.length];
			int pos = 0;
			for (int i = 0; i < queryids.length; i++, pos++) {
				newatomids[pos] = queryids[i];
			}
			for (int i = 0; i < pi2.queryids.length; i++, pos++) {
				newatomids[pos] = pi2.queryids[i];
			}
			
			return new GuardDataProjection(name, Arrays.copyOf(keyEt, keyEt.length), new EqualityType(fields.equality), newatomids);
		}
		return null; // TODO exception
	};
	

}
