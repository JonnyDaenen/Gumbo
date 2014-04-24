/**
 * Created: 31 Mar 2014
 */
package guardedfragment.structure.gfexpressions.io;

import guardedfragment.structure.gfexpressions.GFAndExpression;
import guardedfragment.structure.gfexpressions.GFAtomicExpression;
import guardedfragment.structure.gfexpressions.GFExistentialExpression;
import guardedfragment.structure.gfexpressions.GFExpression;
import guardedfragment.structure.gfexpressions.GFNotExpression;
import guardedfragment.structure.gfexpressions.GFOrExpression;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Jonny Daenen
 * 
 */
public class GFInfixSerializer {

	public String serializeGuard(GFAtomicExpression e) {
		return e.generateString();
	}

	public GFAtomicExpression deserializeGuard(String s) throws DeserializeException {
		// form R(x,y,z)

		int split1 = s.indexOf("(");
		int split2 = s.indexOf(")");

		if (split1 == -1)
			throw new DeserializeException("No '(' found in serial String");

		if (split2 == -1)
			throw new DeserializeException("No ')' found in serial String");

		if (split2 <= split1)
			throw new DeserializeException("Bracket mismatch in serial String");

		String name = s.substring(0, split1);
		String rest = s.substring(split1 + 1, split2);

		String[] vars = rest.split(",");

		return new GFAtomicExpression(name, vars);

	}
	
	
	public GFAtomicExpression deserializeGFAtom(String s) throws DeserializeException {
		// form R(x,y,z)

		int split1 = s.indexOf("(");
		int split2 = s.indexOf(")");

		if (split1 == -1)
			throw new DeserializeException("No '(' found in serial String");

		if (split2 == -1)
			throw new DeserializeException("No ')' found in serial String");

		if (split2 <= split1)
			throw new DeserializeException("Bracket mismatch in serial String");

		String name = s.substring(0, split1);
		String rest = s.substring(split1 + 1, split2);

		String[] vars = rest.split(",");

		return new GFAtomicExpression(name, vars);

	}
	

	public String serializeGuarded(Set<GFAtomicExpression> gfeset) {
		String s = "";

		for (GFAtomicExpression e : gfeset) {
			s += ";" + serializeGuard(e);
		}
		return s.substring(1);

	}

	public Set<GFAtomicExpression> deserializeGuarded(String s) throws DeserializeException {
		String[] eset = s.split(";");
		HashSet<GFAtomicExpression> set = new HashSet<GFAtomicExpression>();

		for (int i = 0; i < eset.length; i++) {
			set.add(deserializeGuard(eset[i]));
		}

		return set;
	}

	public String serializeGFBoolean(GFAtomicExpression e) {
		return e.generateString();
	}
	
	public String serializeGFBoolean(GFAndExpression e) {
		return e.generateString();
	}

	public String serializeGFBoolean(GFNotExpression e) {
		return e.generateString();
	}

	public String serializeGFBoolean(GFOrExpression e) {
		return e.generateString();
	}

	public String serializeGFBoolean(GFExistentialExpression e) {
		return e.generateString();
	}
	
	/**
	 * @param string
	 * @return
	 * @throws DeserializeException
	 */
	public GFExpression deserializeGFBoolean(String s) throws DeserializeException {
		
		if (s.length() < 2)
			throw new DeserializeException("Too short string to be a boolean GFExpression");

		// no brackets, hence atomic? TODO ok?
		if (s.charAt(0) != '(' && s.charAt(s.length() - 1) != ')')
			throw new DeserializeException("Wrong syntax");

		if (s.charAt(0) != '(')
			return deserializeGuard(s);

		// remove brackets and spacing
		s = s.substring(1, s.length() - 1).trim();

		// extract part 1
		String part1 = "";
		GFExpression gfe1 = null;

		if (s.charAt(0) != '!') {
			part1 = extractPart1(s);
			gfe1 = deserializeGFBoolean(part1);
		}
		// extract part 2
		String part2 = extractPart2(s);
		GFExpression gfe2 = deserializeGFBoolean(part2);

		
		// extract operator
		GFExpression gfe3 = extractOperator(s, part1, part2, gfe1, gfe2);

		return gfe3;
	}

	/**
	 * @param s
	 * @param part1
	 * @param part2
	 * @param gfe2
	 * @param gfe1
	 * @return
	 * @throws DeserializeException
	 */
	private GFExpression extractOperator(String s, String part1, String part2, GFExpression gfe1, GFExpression gfe2)
			throws DeserializeException {
		s = s.substring(0, s.length() - part2.length());
		s = s.substring(part1.length());
		s = s.trim();

		switch (s) {
		case "&":
			return new GFAndExpression(gfe1, gfe2);
		case "|":
			return new GFOrExpression(gfe1, gfe2);
		case "!":  // TODO: There maybe something wrong here
			return new GFNotExpression(gfe2);

		}

		throw new DeserializeException("Unknown boolean operator " + s);
	}

	/**
	 * @param s
	 * @return
	 * @throws DeserializeException
	 */
	private String extractPart2(String s) throws DeserializeException {
		if (s.charAt(s.length() - 1) == ')' && s.charAt(s.length() - 2) == ')') { // CLEAN blergh :(
			int index = findPrevOpen(s, s.length() - 2);
			return s.substring(index, s.length());
		} else if (s.charAt(0) == '!') {
			return s.substring(1);
		} else {
			// atom
			int index = s.lastIndexOf("(");
			index = s.lastIndexOf(" ", index);
			return s.substring(index+1);
		}
	}

	/**
	 * @param s
	 * @param i
	 * @return
	 * @throws DeserializeException
	 */
	private int findPrevOpen(String s, int i) throws DeserializeException {
		int closed = 0;
		for (int j = i; j >= 0; j--) {
			if (s.charAt(j) == ')')
				closed++;
			else if (s.charAt(j) == '(') {
				closed--;
				if (closed < 0)
					return j;
			}

		}
		throw new DeserializeException("No '(' found in serial String");

	}

	/**
	 * @param s
	 * @return
	 * @throws DeserializeException
	 */
	private String extractPart1(String s) throws DeserializeException {
		if (s.charAt(0) == '(') {
			int index = findNextClose(s, 1);
			return s.substring(0, index + 1);
		} else {
			// atom
			int index = s.indexOf(")");
			return s.substring(0, index + 1);
		}
	}

	/**
	 * @param s
	 * @param i
	 * @throws DeserializeException
	 */
	private int findNextClose(String s, int i) throws DeserializeException {
		int open = 0;
		for (int j = i; j < s.length(); j++) {
			if (s.charAt(j) == '(')
				open++;
			else if (s.charAt(j) == ')') {
				open--;
				if (open < 0)
					return j;
			}

		}
		throw new DeserializeException("No ')' found in serial String");

	}
	
	
	public String serializeVars(String[] vars) {
		if (vars.length == 0) {
			return new String();
		}
		
		String s = "";

		for (int i=0; i< vars.length;i++) {
			s += ";" + vars[i];
		}
		return s.substring(1);

	}

	public String[] deserializeVars(String s) throws DeserializeException {
		//if (s.length() == 0) {
		//	return null;
		//}
		
		String[] eset = s.split(";");
		return eset;
	}
	
	
	

	/**
	 * @param booleanformula
	 * @return
	 * @throws SerializeException 
	 */
	public String serializeGFBoolean(GFExpression booleanformula) throws SerializeException {
		// CLEAN dirty code
//		if(booleanformula instanceof GFAtomicExpression || booleanformula instanceof GFAndExpression || booleanformula instanceof GFOrExpression || booleanformula instanceof GFNotExpression)
//			return serializeGFBoolean(booleanformula);
		if(booleanformula instanceof GFAtomicExpression)
			return serializeGFBoolean((GFAtomicExpression)booleanformula);
		if(booleanformula instanceof GFAndExpression)
			return serializeGFBoolean((GFAndExpression)booleanformula);
		if(booleanformula instanceof GFOrExpression)
			return serializeGFBoolean((GFOrExpression)booleanformula);
		if(booleanformula instanceof GFNotExpression)
			return serializeGFBoolean((GFNotExpression)booleanformula);
		
		throw new SerializeException("Unsupported type");
	}

}
