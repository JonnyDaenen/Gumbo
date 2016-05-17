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
public class GuardProjection implements TupleProjection {


	long fileid;
	String name;
	byte [] keyEt;
	EqualityType fields;
	byte [] atomIds;

	EqualityFilter ef;




	public GuardProjection(String relationname, long fileid, GFAtomicExpression guard, GFAtomicExpression guarded, byte guardedAtomid) {

		this.name = relationname;
		this.fileid = fileid;
		fields = new EqualityType(guard);

		// extract key
		loadKey(guard, guarded);

		// extract atom id
		atomIds = new byte[1];
		atomIds[0] = guardedAtomid;

		ef = new EqualityFilter(fields);

	}

	protected GuardProjection(String relationname, long fileid, byte [] keyEt, EqualityType fields, byte [] atomids) {

		this.name = relationname;
		this.fileid = fileid;
		this.keyEt = keyEt;
		this.fields = fields;
		this.atomIds = atomids;


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


	public void initialize(String representation) {
		String[] parts = representation.split(":");

		name = parts[0];

		keyEt = convert(parts[1]);
		fields = new EqualityType(convertInt(parts[2]));
		atomIds = convert(parts[3]);

		ef.set(fields.equality);
	}


	private int[] convertInt(String s) {
		String[] parts = s.split(",");
		int [] result = new int[parts.length];
		int i = 0;
		for (String part : parts) {
			result[i++] = Integer.parseInt(part);
		}
		return result;
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
		sb.append(fields);
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
			sb.deleteCharAt(sb.length() - 1);

	}


	@Override
	public int hashCode() {
		return name.hashCode() ^ fields.hashCode() ^ Arrays.hashCode(keyEt) ^ Arrays.hashCode(atomIds); 
	}


	public boolean equals(Object obj) {
		if (obj instanceof GuardProjection) {
			GuardProjection et = (GuardProjection) obj;
			return fields.equals(et.fields) &&
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
	public boolean load(QuickWrappedTuple qt, long offset, VBytesWritable bw, GumboMessageWritable gw) {

		// if tuple conforms to fields
		if (!ef.check(qt))
			return false;

		// output key
		qt.project(keyEt, bw);

		// output atom ids
		gw.setRequest(fileid, offset, atomIds, atomIds.length);

		return true;
	}


	@Override
	public boolean canMerge(TupleProjection pi) {
		// FIXME guarded and guarded cannot merge!
		if (pi instanceof GuardProjection) {
			GuardProjection pi2 = (GuardProjection) pi;
			return name.equals(pi2.name) 
					&& fileid == pi2.fileid 
					&& Arrays.equals(keyEt, pi2.keyEt) 
					&& fields.equals(pi2.fields);
		}
		return false;
	}


	@Override
	public TupleProjection merge(TupleProjection pi) {
		if (pi instanceof GuardProjection) {
			GuardProjection pi2 = (GuardProjection) pi;
			
			// concatenate atom ids
			byte [] newatomids = new byte [atomIds.length + pi2.atomIds.length];
			int pos = 0;
			for (int i = 0; i < atomIds.length; i++, pos++) {
				newatomids[pos] = atomIds[i];
			}
			for (int i = 0; i < pi2.atomIds.length; i++, pos++) {
				newatomids[pos] = pi2.atomIds[i];
			}
			
			return new GuardProjection(name, fileid, Arrays.copyOf(keyEt, keyEt.length), new EqualityType(fields.equality), newatomids);
		}
		return null; // TODO exception
	};






}
