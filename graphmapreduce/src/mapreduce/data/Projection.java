package mapreduce.data;

import java.util.Map;

/**
 * A mapping between relations. Source and target relations are indicated, together with a mapping between them.
 * The mapping indicates for each position in the target relation where to extract it from the source relation.
 * 
 * @author Jonny Daenen
 *
 */
public class Projection {
	
	RelationSchema source;
	RelationSchema target;
	Map<Integer, Integer> mapping;
	
	public Projection(RelationSchema source, RelationSchema target) {
		super();
		this.source = source;
		this.target = target;
	}
	
	/**
	 * Empties the current mapping and loads the given one (by copying all entries).
	 * @param newmap the new mapping
	 */
	public void loadMapping(Map<Integer, Integer> newmap) {
		// TODO add bound control
		this.mapping.clear();
		for( int key : newmap.keySet())
			this.mapping.put(key, newmap.get(key));
	}
	
	/**
	 * Connect a position in the target relation to a position in the source relation.
	 * @param pos1 position in the target relation
	 * @param pos2 position in the source relation
	 */
	public void addMapping(int pos1, int pos2) {
		// TODO add bound control
		this.mapping.put(pos1, pos2);
		
	}
	
	/** 
	 * Converts a tuple from the source relation to a tuple in the target relation, 
	 * according to the mapping.
	 *
	 * @return a projection of a given source-tuple to the target relation
	 */
	public Tuple project(Tuple t) {
		
		String [] s = new String[target.getNumFields()];
		
		// copy fields one by one
		for(int i = 0; i < target.getNumFields(); i++)
			s[i] = t.get(mapping.get(i));
		
		// create a new tuple from the generated String
		return new Tuple(target.getName(),s);
		
	}
	

}
