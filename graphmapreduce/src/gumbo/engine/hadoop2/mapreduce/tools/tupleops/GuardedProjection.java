package gumbo.engine.hadoop2.mapreduce.tools.tupleops;

import org.apache.hadoop.io.BytesWritable;

import gumbo.engine.hadoop2.datatypes.GumboMessageWritable;
import gumbo.engine.hadoop2.mapreduce.tools.QuickWrappedTuple;


/**
 * Guarded projection operation for Validation operator mappers.
 * 
 * @author Jonny Daenen
 *
 */
public class GuardedProjection implements TupleProjection {
	EqualityType eq;
	long fileid;
	byte [] atomids;

	
	public GuardedProjection(long fileid, EqualityType eq, byte [] atomids) {
		this.fileid = fileid;
		this.eq = eq;
		this.atomids = atomids;
	}
	
	/**
	 * Checks whether the current tuple conforms to the guarded expression
	 * and, if so, prepares the writables.
	 * The BytesWritable will contain a projection to the correct attributes,
	 * the GumboMessage will be an assert message containing the requested atom ids.
	 * 
	 *  @return true iff writables should be written to output
	 */
	@Override
	public boolean load(QuickWrappedTuple qt, long offset, BytesWritable bw, GumboMessageWritable gw) {
		
		// check if it matches
		if (!eq.check(qt))
			return false;
		
		// fill up key
		// FUTURE this should become a projection
		bw.set(qt.getData(), 0, qt.getLength());
		
		// fill up value
		gw.setAssert(atomids, atomids.length);
		
		return true;
	}
	

}
