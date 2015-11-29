package gumbo.engine.hadoop2.mapreduce.tools.tupleops;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;

import gumbo.engine.hadoop2.datatypes.GumboMessageWritable;
import gumbo.engine.hadoop2.mapreduce.tools.QuickWrappedTuple;

/**
 * Guard projection operation for Validation operator mappers.
 * 
 * @author Jonny Daenen
 *
 */
public class GuardProjection implements TupleProjection {
	EqualityType eq;
	long fileid;
	byte [] atomids;
	List<Integer> fields;

	public GuardProjection(long fileid, EqualityType eq, byte [] atomids) {
		this.fileid = fileid;
		this.eq = eq;
		this.atomids = atomids;
		
		fields = new ArrayList<>(5);
	}
	
	public void addField(int i) {
		fields.add(i);
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
		
		// check if it matches
		if (!eq.check(qt))
			return false;
		
		// fill up key
		qt.project(fields, bw);
		
		// fill up value
		gw.setRequest(fileid, offset, atomids, atomids.length);
		
		return true;
		

	}

	

}
