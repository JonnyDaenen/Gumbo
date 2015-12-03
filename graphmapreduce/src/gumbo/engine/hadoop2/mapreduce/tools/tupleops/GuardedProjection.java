package gumbo.engine.hadoop2.mapreduce.tools.tupleops;

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
	


	

}
