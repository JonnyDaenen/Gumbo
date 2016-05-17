/**
 * Created: 31 Mar 2014
 */
package gumbo.structures.gfexpressions.io;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import gumbo.structures.gfexpressions.GFAndExpression;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.GFExpression;
import gumbo.structures.gfexpressions.GFNotExpression;
import gumbo.structures.gfexpressions.GFOrExpression;
import gumbo.structures.gfexpressions.GFVisitor;
import gumbo.structures.gfexpressions.GFVisitorException;
import gumbo.structures.gfexpressions.GFXorExpression;


/**
 * (De)serialisation of GFE's using prefix/Polish notation.
 * 
 * @author Tony Tan
 * @author Jonny Daenen
 * 
 * NOTE: An object of this class is a "translator" 
 * between a GF query in String in prefix notation (or a number of GF queries) and 
 * an object of GFExpression (or a set of GFExpression objects).
 * 
 * The method serializer is to convert GFExpression objects into its string form;
 * while deserializer is the other way round. 
 * 
 * A GF query is written in the form:
 * OutputName(x1,...,xk) : & GuardRelation(x1,...xk,y1,...,ym) Boolean combination of guarded relations
 * 
 */
public class GFPrefixSerializer implements GFVisitor<String>, Serializer<GFExpression> {


	private static final Log LOG = LogFactory.getLog(GFPrefixSerializer.class);
		
	StringSetSerializer setSerializer;

	public GFPrefixSerializer() {
		setSerializer = new StringSetSerializer();
	}

	
	/**
	 * Serializes a GFExpression into Polisch/Prefix notation.
	 * 
	 * @param e
	 * @return
	 * @throws GFVisitorException 
	 */
	public String serialize(GFExpression e) {
		try {
			return e.accept(this);
		} catch (GFVisitorException e1) {
			// should not happen
			LOG.info("Unexpected exception during serialization of "+e+": " + e1.getMessage());
			e1.printStackTrace();
		}
		return "";
	}

	public String serializeSet(Collection<? extends GFExpression> set) {

		HashSet<String> stringSet = new HashSet<String>(set.size());

		// serialize each expression
		for (GFExpression e : set) {
			stringSet.add(serialize(e));
		}

		// flatten
		return setSerializer.serialize(stringSet);
	}

	/**
	 * Deserializes a GFExpression in Polish/prefix notation.
	 * 
	 * @param s
	 * @return
	 * @throws DeserializeException
	 */
	public GFExpression deserialize(String s) throws DeserializeException {
		Pair<GFExpression, Integer> result = deserialize(s, 0);
		return result.fst;
	}
	
	
	
	public Collection<GFExpression> deserialize(Collection<String> set) throws DeserializeException {
		Set<GFExpression> resultSet = new HashSet<GFExpression>();
		for (String s : set) {
			resultSet.add(deserialize(s));
		}
		return resultSet;
	}

	public Set<GFExpression> deserializeSet(String set) throws DeserializeException {

		Set<String> strings = setSerializer.deserialize(set);
		HashSet<GFExpression> eSet = new HashSet<>(strings.size());

		// serialize each expression
		for (String s : strings) {
			eSet.add(deserialize(s));
		}

		// flatten
		return eSet;
	}
	
	public Set<GFExistentialExpression> deserializeExSet(String set) throws DeserializeException {

		Set<String> strings = setSerializer.deserialize(set);
		HashSet<GFExpression> eSet = new HashSet<>(strings.size());

		// serialize each expression
		for (String s : strings) {
			eSet.add(deserialize(s));
		}
		
		// check whether the type is existential
		HashSet<GFExistentialExpression> formulaSet = new HashSet<GFExistentialExpression>();
		for (GFExpression exp : eSet) {
			if (exp instanceof GFExistentialExpression) {
				formulaSet.add((GFExistentialExpression) exp);
			}
		}

		// flatten
		return formulaSet;
	}

	/**
	 * Deserializes a GFExpression in Polish/prefix notation.
	 * 
	 * @param s
	 * @param startpos
	 *            starting position
	 * @return the GFExpression and the position on which to continue
	 * @throws DeserializeException
	 */
	public Pair<GFExpression, Integer> deserialize(String s, int startpos) throws DeserializeException {

		Pair<GFExpression, Integer> result = null;

		if (startpos >= s.length())
			throw new DeserializeException("Incomplete expression (" + s + ")");

		// remove whitespace
		s = s.trim();

		// check if there is something to deserialize
		if (s.length() == 0)
			throw new DeserializeException("Empty expression (" + s + ")");

		// get operator
		String operator = s.substring(startpos, startpos + 1);
		Pair<GFExpression, Integer> pass1, pass2;

		switch (operator) {
		case "&": // The And operator
			pass1 = deserialize(s, startpos + 1);
			pass2 = deserialize(s, pass1.snd);

			GFExpression and = new GFAndExpression(pass1.fst, pass2.fst);
			result = new Pair<GFExpression, Integer>(and, pass2.snd);

			break;
			
		case "+": // The Xor operator
			pass1 = deserialize(s, startpos + 1);
			pass2 = deserialize(s, pass1.snd);

			GFExpression xor = new GFXorExpression(pass1.fst, pass2.fst);
			result = new Pair<GFExpression, Integer>(xor, pass2.snd);

			break;


		case "|": // The Or operator
			pass1 = deserialize(s, startpos + 1);
			pass2 = deserialize(s, pass1.snd);

			GFExpression or = new GFOrExpression(pass1.fst, pass2.fst);
			result = new Pair<GFExpression, Integer>(or, pass2.snd);

			break;

		case "!": // The Negation operator
			pass1 = deserialize(s, startpos + 1);

			GFExpression not = new GFNotExpression(pass1.fst);
			result = new Pair<GFExpression, Integer>(not, pass1.snd);

			break;

		case "#": // The symbol that separates the OutputName and the GFExpression itself.
			result = processExistential(s, startpos);
			break;
			
		case "*":
			// TODO add support for universal
			break;

		default: // found relationname
			result = processAtom(s, startpos);
			break;
		}

		return result;
	}

	/**
	 * @param s
	 * @param startpos
	 * @return
	 * @throws DeserializeException
	 */
	private Pair<GFExpression, Integer> processAtom(String s, int startpos) throws DeserializeException {
		// find next '(' and ')'
		int bracket1Pos = s.indexOf('(', startpos);
		int bracket2Pos = s.indexOf(')', startpos);

		if (bracket1Pos == -1 || bracket2Pos == -1 || bracket1Pos >= bracket2Pos || bracket1Pos <= startpos)
			throw new DeserializeException("Wrong atom notation after position " + startpos + " (" + s + ")");

		// get name and var list
		String relationName = s.substring(startpos, bracket1Pos);
		String varString = s.substring(bracket1Pos + 1, bracket2Pos);
		String[] vars = varString.split(",");

		// check if name uses allowed alphabet
		if (!relationName.matches("[a-zA-Z0-9]+")) {
			throw new DeserializeException("Expecting only alphanumeric symbols for relation names at position "
					+ startpos + " (" + s + ")");
		}

		return new Pair<GFExpression, Integer>(new GFAtomicExpression(relationName, vars), bracket2Pos + 1);
	}

	// --- VISIT METHODS

	/**
	 * @param s
	 * @param startpos
	 * @return
	 * @throws DeserializeException
	 */
	private Pair<GFExpression, Integer> processExistential(String s, int startpos) throws DeserializeException {

		Pair<GFExpression, Integer> pass1 = deserialize(s, startpos + 1);
		Pair<GFExpression, Integer> pass2 = deserialize(s, pass1.snd);

		GFExpression arg;

		// check atomicy of output schema
		
		// NOTE forbid duplicate var names in output relation? -> no, this causes no harm
		
		arg = pass1.fst;
		if (!(arg instanceof GFAtomicExpression))
			throw new DeserializeException("Output relation is non-atomic at postion " + startpos + 1 + " (" + s + ")");
		GFAtomicExpression out = (GFAtomicExpression) arg;

		// check whether guard operation is AND
		arg = pass2.fst;
		if (!(arg instanceof GFAndExpression))
			throw new DeserializeException("Guard is not by conjunction (AND-operation) at position " + pass1.snd
					+ " (" + s + ")");
		GFAndExpression and = (GFAndExpression) arg;

		// check atomicy of guard
		arg = and.getChild1();
		if (!(arg instanceof GFAtomicExpression))
			throw new DeserializeException("Guard is non-atomic at position " + pass1.snd + " (" + s + ")");
		GFAtomicExpression guard = (GFAtomicExpression) arg;

		// get actual child
		GFExpression child = and.getChild2();

		// assemble expression
		GFExistentialExpression e = new GFExistentialExpression(guard, child, out);
		int endpos = pass2.snd;

		return new Pair<GFExpression, Integer>(e, endpos);
	}

	/**
	 * @see gumbo.structures.gfexpressions.GFVisitor#visit(gumbo.structures.gfexpressions.GFExpression)
	 */
	@Override
	public String visit(GFExpression e) {
		return "";
	}

	/**
	 * @see gumbo.structures.gfexpressions.GFVisitor#visit(gumbo.structures.gfexpressions.GFAtomicExpression)
	 */
	@Override
	public String visit(GFAtomicExpression e) {
		String s = "";
		int i = 0;
		String[] constants = e.getConstants();
		for (String var : e.getVars()) {
			s += "," + var;
			// CONSTANTCODE begin
			if (constants[i] != null)
				s += "=" + constants[i];
			// CONSTANTCODE end
			i++;
		}

		s = s.substring(1);
		s = e.getName() + "(" + s + ")";
		return s;
	}

	/**
	 * @see gumbo.structures.gfexpressions.GFVisitor#visit(gumbo.structures.gfexpressions.GFAndExpression)
	 */
	@Override
	public String visit(GFAndExpression e) {

		String a1 = serialize(e.getChild1());
		String a2 = serialize(e.getChild2());

		return "&" + a1 + a2;
	}

	/**
	 * @see gumbo.structures.gfexpressions.GFVisitor#visit(gumbo.structures.gfexpressions.GFOrExpression)
	 */
	@Override
	public String visit(GFOrExpression e) {
		String a1 = serialize(e.getChild1());
		String a2 = serialize(e.getChild2());

		return "|" + a1 + a2;
	}

	/**
	 * @see gumbo.structures.gfexpressions.GFVisitor#visit(gumbo.structures.gfexpressions.GFNotExpression)
	 */
	@Override
	public String visit(GFNotExpression e) {
		String a1 = serialize(e.getChild());
		return "!" + a1;
	}

	/**
	 * @see gumbo.structures.gfexpressions.GFVisitor#visit(gumbo.structures.gfexpressions.GFExistentialExpression)
	 */
	@Override
	public String visit(GFExistentialExpression e) {
		String o = serialize(e.getOutputRelation());
		String g = serialize(e.getGuard());
		String c = serialize(e.getChild());
		return "#" + o + "&" + g + c;
	}


}
