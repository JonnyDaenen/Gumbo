/**
 * Created on: 07 Apr 2015
 */
package gumbo.engine.hadoop.mrcomponents.comparators;

/**
 * @author Jonny Daenen
 *
 */
public class KeyPairWrapper {
	
	public String first;
	public String second;


	public KeyPairWrapper(String message) {
		int length = message.length();
		if (length > 0 && message.charAt(length-1) == '#') {
			first = message.substring(0,message.length()-1);
			second = "1";
		} else {
			first = message;
			second = "2";
		}
	}
	
	
	@Override
	public String toString() {
		return first + second;
	}
	
}
