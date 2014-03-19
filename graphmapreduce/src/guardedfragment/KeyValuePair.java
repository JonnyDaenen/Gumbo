package guardedfragment;

public class KeyValuePair {
	
	String mykey;
	String myvalue;
	
	public KeyValuePair(String s1, String s2){
		mykey  = s1;
		myvalue = s2;
	}
	
	public String getKey(){
		return mykey;
	}
	
	public String getValue(){
		return myvalue;
	}

}
