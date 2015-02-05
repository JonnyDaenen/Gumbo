/**
 * Created: 23 Apr 2014
 */
package gumbo.guardedfragment.gfexpressions.io;

/**
 * @author Jonny Daenen
 *
 */
public interface Serializer<T> {

	public String serialize(T object);
	
	public T deserialize(String s) throws DeserializeException;
}
