/**
 * Created: 23 Jul 2014
 */
package gumbo.structures.gfexpressions.io;

/**
 * @author Jonny Daenen
 * 
 * reverse engineered version of com.sun.tools.javac.util.Pair
 *
 */
public class Pair<type1,type2> {
	public type1 fst;
	public type2 snd;


	public Pair(type1 fst, type2 snd) {
		this.fst = fst;
		this.snd = snd;
	}

	// not necessary
	/**
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Pair<?,?>) {
			Pair<?, ?> otherPair = (Pair<?,?>) obj;
			return snd.equals(otherPair.snd) && fst.equals(otherPair.fst);
		} else
			return false;

	}

	/**
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return fst.hashCode() ^ snd.hashCode();
	}

}
