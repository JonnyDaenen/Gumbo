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
 * Guard projection operation for Validation operator mappers.
 * This operation provides its own packing.
 * 
 * One Projection object should be made per output key equality type.
 * Multiple internal equality types are used to provide packing.
 * 
 * 
 * @author Jonny Daenen
 *
 */
public class GuardProjection implements TupleProjection {

	long fileid;
	byte [] atomids;
	List<Integer> fields;
	
	Map<EqualityType, Set<Byte>> atomMapping;
	Set<Byte> atoms;

	public GuardProjection(long fileid) {
		this.fileid = fileid;

		fields = new ArrayList<>(5);
		atomMapping = new HashMap<>();
		atoms = new HashSet<>();
		atomids = new byte[16];
	}

	/**
	 * Adds a field that should be in the output key. Note that the order 
	 * the keys are added is retained in the final output.
	 * 
	 * @param i attribute index
	 */
	public void addField(int i) {
		fields.add(i);
	}
	
	/**
	 * Add an atom id to an equality type.
	 * @param et equality type
	 * @param atomid atom id
	 */
	public void add(EqualityType et, byte atomid) {
		
		if (!atomMapping.containsKey(et)) {
			atomMapping.put(et, new HashSet<Byte>());
		}
		atomMapping.get(et).add(atomid);
		
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

		boolean outputPresent = false;
		atoms.clear();
		// assemble atoms
		for (EqualityType et : atomMapping.keySet()) {

			// if equality type is satisfied, add its atoms
			if (et.check(qt)) {
				atoms.addAll(atomMapping.get(et));
				outputPresent = true;
			}
		}

		if (!outputPresent)
			return false;

		// prepare writables

		// fill up key
		qt.project(fields, bw);

		// fill up value
		if (atomids.length < atoms.size())
			atomids = new byte[atoms.size()];
		
		int i = 0;
		for (byte atom : atoms) {
			atomids[i++] = atom;
		}
		gw.setRequest(fileid, offset, atomids, i);

		return true;

	}



}
