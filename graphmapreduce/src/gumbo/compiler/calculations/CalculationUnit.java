/**
 * Created: 28 Apr 2014
 */
package gumbo.compiler.calculations;

import gumbo.compiler.structures.data.RelationSchema;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A CalculationUnit represents an operation that cannot be split up into smaller parts.
 * A list of dependendencies is kept to easily construct a dependency graph.
 * It can be translated directly into a (set of) (MR-)jobs in a framework. 
 * An example is a {@link BasicGFCalculationUnit}.
 *  
 * 
 * @author Jonny Daenen
 *
 */
public abstract class CalculationUnit {
	
	static int COUNTER = 0; // CLEAN dirty code
	int id;
	Map<RelationSchema,CalculationUnit> directDependencies;
	
	
	public CalculationUnit() {
		this(COUNTER);
	}

	public CalculationUnit(int id) {
		this.id = id;
		COUNTER = Math.max(COUNTER, id) + 1;
		directDependencies = new HashMap<RelationSchema,CalculationUnit>();
	}

	
	/**
	 * @return set of direct dependent {@link CalculationUnit}s
	 */
	public Collection<CalculationUnit> getDependencies() {
		return directDependencies.values();
	}
	
	/**
	 * @param cu a CU on which this CU depends
	 * @param rs the relation of the CU
	 */
	public void setDependency(RelationSchema rs,CalculationUnit cu) {
		directDependencies.put(rs, cu);
	}
	
	
	
	abstract public Set<RelationSchema> getInputRelations();
	

	
	/**
	 * TODO #core change to set
	 * @return the output schema
	 */
	abstract public RelationSchema getOutputSchema();
	


	/**
	 * @return true if this calculation has no dependencies
	 */
	public boolean isLeaf() {
		return directDependencies.isEmpty();
	}

	/**
	 * Calculates the height of the DAG rooted here, leafs have height 1.
	 * @return the height of the DAG rooted at this node
	 */
	public int getHeight() {
		
		int max = 0;
		for (CalculationUnit dep : directDependencies.values()) {
			max = Math.max(max, dep.getHeight());
		}
			
		return max + 1;
		
	}
	
	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		String s = "";
		s += "id : " + id + " ";
		s += "Depends on: ";
		if(directDependencies.size() == 0)
			s += "None.";
		for (CalculationUnit c : directDependencies.values()) {
			s += c.id +",";
			
		}
		return s;
	}
	
	/**
	 * @return the id
	 */
	public int getId() {
		return id;
	}

}
