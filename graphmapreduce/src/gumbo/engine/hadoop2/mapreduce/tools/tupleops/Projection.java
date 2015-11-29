package gumbo.engine.hadoop2.mapreduce.tools.tupleops;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;

import gumbo.engine.hadoop2.datatypes.GumboMessageWritable;
import gumbo.engine.hadoop2.mapreduce.tools.QuickWrappedTuple;

/**
 * @author Jonny Daenen
 *
 */
public class Projection implements TupleProjection {

	long fileid;
	Set<ConditionalProjection> eqts;
	byte [] atombuffer;
	int maxatoms;

	public Projection(long fileid) {
		this.fileid = fileid;
		maxatoms = 0;
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

		// fill up value
		if (atombuffer == null || atombuffer.length < maxatoms)
			atombuffer = new byte[maxatoms];
				
		boolean outputPresent = false;
		
		// assemble atoms
		int i = 0;
		byte [] keyFields = null;
		for (ConditionalProjection et : eqts) {

			// if equality type is satisfied, add its atoms
			if (et.check(qt)) {
				keyFields = et.keyEt;
				System.arraycopy(et.atomIds, 0, atombuffer, i, et.atomIds.length);
				i += et.atomIds.length;
				outputPresent = true;
			}
		}

		if (!outputPresent)
			return false;

		// prepare writables

		// fill up key
		qt.project(keyFields, bw);
		
		// fill up request message
		gw.setRequest(fileid, offset, atombuffer, i);

		return true;

	}



}
