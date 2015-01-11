/**
 * Created: 31 Mar 2014
 */
package mapreduce.guardedfragment.structure.gfexpressions.io;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.Stack;

import mapreduce.guardedfragment.structure.gfexpressions.GFAndExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFAtomicExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFExistentialExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFNotExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFOrExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFXorExpression;

/**
 * @author Jonny Daenen
 * 
 *  NOTE: An object of this class is a "translator" 
 * between a GF query in String in infix notation (or a number of GF queries) and 
 * an object of GFExpression (or a set of GFExpression objects).
 * 
 * The method serializer is to convert GFExpression objects into its string form;
 * while deserializer is the other way round. 
 * 
 * A GF query is written in the form:
 * OutputName(x1,...,xk) : GuardRelation(x1,...xk,y1,...,ym) & Boolean combination of guarded relations
 * 
 */
public class GFInfixSerializer {

	public String serializeGuard(GFAtomicExpression e) {
		return e.generateString();
	}
	
	public Set<GFExpression> GetGFExpression(String s) throws DeserializeException {
		
		GFPrefixSerializer prefixParser = new GFPrefixSerializer();
		
		String [] sArray = s.split(";");
		HashSet<String> sSet = new HashSet<String>(Arrays.asList(sArray));
		
		String [] dummyArray;
		
		String dummyString=new String();
		
		for (String ss : sSet) {
		    dummyArray = ss.split(":");
		    System.out.println("length of dummyArray" + dummyArray.length);
		    
		    for(int i =0 ; i < dummyArray.length; i++) {
		    	System.out.println(dummyArray[i]);
		    }
		    		    
		    if (dummyArray.length != 2) {
		    	throw new DeserializeException("Expect exactly one : on defining the query "+ss);		    	
		    }
		       
		    dummyString = dummyString+"#"+dummyArray[0].trim()+InfixToPrefix(dummyArray[1])+";";	
		}
		
		
		GFPrefixSerializer dp = new GFPrefixSerializer();
		Set<GFExpression> sp = dp.deserializeSet("{"+dummyString+"}");
		return sp;
	}
	
    private String InfixToPrefix(String s) throws DeserializeException {
    	Stack<String> mystack = new Stack<String>();
    	int index = 0;
    	int maxIndex = s.length()-1;
    	
    	String x;
    	String y;
    	
    	while (index <= maxIndex && Character.isWhitespace(s.charAt(index))) {
    		index++;
    	}
    	
    	if (index > maxIndex) {
    		throw new DeserializeException("Expecting some formula");
    	}

    	   	
    	while (index <= maxIndex) {
    		
    		if (! (s.charAt(index) == '(' || 
    				s.charAt(index) == ')' ||
    				s.charAt(index) == '&' ||
    				s.charAt(index) == '|' ||
    				s.charAt(index) == '!' ||
    				s.charAt(index) == '+' ||
    				Character.isLetter(s.charAt(index)) )) {
    			throw new DeserializeException("Error in index-"+index);
    		}
    		
    		if (s.charAt(index) == '(') {
    			
    			if(mystack.empty()) {
    				mystack.push(new String("("));
    			} else {
        			if (isTerm(mystack.peek()) || isCloseBracket(mystack.peek())) {
        				throw new DeserializeException("Error in index-"+index);
        			}       			
        			mystack.push(new String("("));		
    			}
    		}
    		
      		if (s.charAt(index) == ')') {
      			if (mystack.size() <= 1) {
      				throw new DeserializeException("Error in index-"+index);
      			}
      			x = mystack.pop();
      			y = mystack.pop();
      			
      			if (! isOpenBracket(y) || ! isTerm(x)) {
      				throw new DeserializeException("Error in index-"+index);
      			}
      			
      			mystack.push(x);
      			
      			mystack = cleanup(mystack);	
       		}
    		
    		if (s.charAt(index) == '&') {
    			if (mystack.empty()) {
    				throw new DeserializeException("Error in index-"+index); 				
    			}
    			if (! isTerm(mystack.peek())) {
    				throw new DeserializeException("Error in index-"+index); 				
    			}
    			mystack.push(new String("&"));
 
    		}
    		
    		if (s.charAt(index) == '+') {
    			if (mystack.empty()) {
    				throw new DeserializeException("Error in index-"+index); 				
    			}
    			if (! isTerm(mystack.peek())) {
    				throw new DeserializeException("Error in index-"+index); 				
    			}
    			mystack.push(new String("+"));
 
    		}
    		
       		if (s.charAt(index) == '|') {
    			if (mystack.empty()) {
    				throw new DeserializeException("Error in index-"+index);  				
    			}
    			if (! isTerm(mystack.peek())) {
    				throw new DeserializeException("Error in index-"+index); 				
    			}
    			mystack.push(new String("|"));
    		}
       		
       		
       		if (s.charAt(index) == '!') {
    			if (mystack.empty()) {
    				mystack.push(new String("!"));  				
    			} else {
        			if (isTerm(mystack.peek())) {
        				throw new DeserializeException("Error in index-"+index); 				
        			}
        			mystack.push(new String("!"));
      			}      			
    		}
       		
       		if (Character.isLetter(s.charAt(index))) {
       			int k = index;
       			index++;
       			while (index <= maxIndex &&  Character.isLetterOrDigit(s.charAt(index))) {
       				index++;
       			}
       			
       			if (index > maxIndex) {
       				throw new DeserializeException("Error in reading atomic formula in index-"
       									+index+": Expecting (");
       			}
       			
       			if (s.charAt(index) != '(') {
       				throw new DeserializeException("Error in reading atomic formula in index-"
								+index+": Expecting (");
       				
       			}
       			index++;
       			
       			//String nameRel = s.substring(k,index);
       			
       			while (index <= maxIndex &&  s.charAt(index) != ')') {
       				index++;
       			}
       			if (index > maxIndex) {
       				throw new DeserializeException("Error in reading atomic formula in index-"
								+index+": Expecting )");
       			}      			
       			mystack.push(s.substring(k,index+1));
       			mystack = cleanup(mystack);
       		} // End of if for the case Character.isLetter(s.charAt(index))
       		
       		index++;
       	   	while (index <= maxIndex && Character.isWhitespace(s.charAt(index))) {
        		index++;
        	} 		
    	}
    	if (mystack.size() == 1) {
    		return mystack.pop();
    	}
    	throw new DeserializeException("Something wrong"); 	
    }

    

    
    private boolean isTerm(String s) {
    	return s.length() > 1;
    	//return ! (isOperator(s) || isOpenBracket(s) || isCloseBracket(s) || isNegation(s));
    }
    
    private boolean isOperator(String s) {
    	return (s.length() == 1 && ( s.charAt(0) == '+' || s.charAt(0) == '&' || s.charAt(0) == '|'));
    }
    
    private boolean isOpenBracket(String s) {
    	return (s.length() == 1 && s.charAt(0) == '(' );
    }
    
    private boolean isCloseBracket(String s) {
    	return (s.length() == 1 && s.charAt(0) == ')' );
    }
    
    private boolean isNegation(String s) {
    	return (s.length() == 1 && s.charAt(0) == '!' );
    }
    
    private Stack<String> cleanup(Stack<String> st) {
    	
    	if (st.size() <= 1) {
    		return st;
    	}
    	String x,y,z;
    	
        x = st.pop();
        y = st.pop();
        	
        if (isTerm(x) && isNegation(y)) {
        	st.push(y+x);  
        	//return st;
        	return cleanup(st);
        }
        
        if (isTerm(x) && isOperator(y)) {
        	z = st.pop();
        	st.push(y+z+x);
        	//return st;
        	return cleanup(st);
        }
        
        if (isCloseBracket(x) && isTerm(y)) {
        	z = st.pop();
        	st.push(y);
        	//return st;
        	return cleanup(st);
        }
    	
        st.push(y);
        st.push(x);
    	
    	return st;
    	
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
		case "+":
			return new GFXorExpression(gfe1, gfe2);
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
		if(booleanformula instanceof GFXorExpression)
			return serializeGFBoolean((GFXorExpression)booleanformula);
		
		throw new SerializeException("Unsupported type");
	}

}
