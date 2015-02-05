/**
 * Created: 23 Apr 2014
 */
package gumbo.guardedfragment.gfexpressions.io;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Jonny Daenen
 *
 */
public class StringSetSerializer implements Serializer<Set<String>>{

	String separator = ";";
	

	public StringSetSerializer() {
		separator = ";";
	}
	
	public StringSetSerializer(String separator) {
		this.separator = separator;
	}
	
	

	@Override
	public String serialize(Set<String> set) {
		StringBuilder sb = new StringBuilder(set.size() * 5);
		for (String string : set) {
			sb.append(separator+string);
		}
		return "{"+sb.toString().substring(separator.length())+"}";
	}


	@Override
	public Set<String> deserialize(String s) throws DeserializeException {
		
		s = s.trim();
		
		// check syntax
		if(s.length() < 2 || s.charAt(0) != '{' || s.charAt(s.length()-1) != '}')
			throw new DeserializeException("Wrong set syntax");
		
		// cut off {}
		s = s.substring(1,s.length()-1);
		
		String [] array = s.split(separator);
		HashSet<String> set = new HashSet<String>(Arrays.asList(array));
		
		return set;
	}


	
	
}
