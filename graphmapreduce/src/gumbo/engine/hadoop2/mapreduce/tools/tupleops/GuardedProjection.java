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
public class GuardedProjection extends GuardProjection {


	public GuardedProjection(String relationname, GFAtomicExpression guarded, byte guardedAtomId) {
		super(relationname, 0, guarded, guarded, guardedAtomId);
	}
	
	public GuardedProjection(String relationname, byte [] keyEt, EqualityType fields, byte [] atomids) {
		super(relationname, 0, keyEt, fields, atomids);
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
		gw.setAssert(atomIds, atomIds.length);
		
		return true;
	};
	
	
	@Override
	public boolean canMerge(TupleProjection pi) {
		if (pi instanceof GuardedProjection) {
			GuardedProjection pi2 = (GuardedProjection) pi;
			return name.equals(pi2.name) 
					&& fileid == pi2.fileid 
					&& Arrays.equals(keyEt, pi2.keyEt) 
					&& fields.equals(pi2.fields);
		}
		return false;
	}


	@Override
	public TupleProjection merge(TupleProjection pi) {
		if (pi instanceof GuardedProjection) {
			GuardedProjection pi2 = (GuardedProjection) pi;
			
			// concatenate atom ids
			byte [] newatomids = new byte [atomIds.length + pi2.atomIds.length];
			int pos = 0;
			for (int i = 0; i < atomIds.length; i++, pos++) {
				newatomids[pos] = atomIds[i];
			}
			for (int i = 0; i < pi2.atomIds.length; i++, pos++) {
				newatomids[pos] = pi2.atomIds[i];
			}
			
			return new GuardedProjection(name, Arrays.copyOf(keyEt, keyEt.length), new EqualityType(fields.equality), newatomids);
		}
		return null; // TODO exception
	};


	

}
