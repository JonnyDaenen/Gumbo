/**
 * Created: 31 Mar 2014
 */
package guardedfragment.structure;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Jonny Daenen
 *
 */
public class GFSerializer {
	
	
	public String serializeGuard(GFAtomicExpression e) {
		return e.generateString();
	}
	
	public GFAtomicExpression deserializeGuard(String s) throws DeserializeException {
		// form R(x,y,z)
		
		int split1 = s.indexOf("(");
		int split2 = s.indexOf(")");
		
		if( split1 == -1)
			throw new DeserializeException("No '(' found in serial String");
		
		if( split2 == -1)
			throw new DeserializeException("No ')' found in serial String");
		
		if( split2 <= split1)
			throw new DeserializeException("Bracket mismatch in serial String");
		
		String name = s.substring(0,split1);
		String rest = s.substring(split1+1,split2);
		
		String [] vars = rest.split(",");
		
		return new GFAtomicExpression(name,vars);
		
		
	}
	
	public String serializeGuarded(Set<GFAtomicExpression> gfeset) {
		String s = "";
		
		for (GFAtomicExpression e : gfeset) {
			s += ";" + serializeGuard(e);
		}
		return s.substring(1);
		
	}
	
	public Set<GFAtomicExpression> deserializeGuarded(String s) throws DeserializeException {
		String [] eset = s.split(";");
		HashSet<GFAtomicExpression> set = new HashSet<GFAtomicExpression>();
		
		for (int i = 0; i < eset.length; i++) {
			set.add(deserializeGuard(eset[i]));
		}
		
		return set;
	}
	

}
