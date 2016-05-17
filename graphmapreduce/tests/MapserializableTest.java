import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;

import org.apache.commons.net.util.Base64;


public class MapserializableTest {

	public static void main(String[] args) {

		HashMap<String, Integer> map = new HashMap<>();
		map.put("Jonny", 1);
		map.put("Test", 2);

		String s = objectToString(map);

		HashMap<String, Integer> map2 = stringToObject(s, map.getClass());
		
		System.out.println(map.equals(map2));
		System.out.println(map);
		System.out.println(map2);

	}

	public static String objectToString(Serializable object) {
		String encoded = null;

		try {
			ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
			ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
			objectOutputStream.writeObject(object);
			objectOutputStream.close();
			encoded = Base64.encodeBase64String(byteArrayOutputStream.toByteArray());
		} catch (IOException e) {
			e.printStackTrace();
		}
		return encoded;
	}

	@SuppressWarnings("unchecked")
	public static <T extends Serializable> T stringToObject(String string, Class<T> clazz) {
		byte[] bytes = Base64.decodeBase64(string.getBytes());
		T object = null;
		try {
			ObjectInputStream objectInputStream = new ObjectInputStream( new ByteArrayInputStream(bytes) );
			object = (T)objectInputStream.readObject();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (ClassCastException e) {
			e.printStackTrace();
		}
		return object;
	}


}
