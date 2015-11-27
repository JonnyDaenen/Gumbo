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
public class Triple<type1,type2,type3> {
	public type1 fst;
	public type2 snd;
	public type3 trd;


	public Triple(type1 fst, type2 snd, type3 trd) {
		this.fst = fst;
		this.snd = snd;
		this.trd = trd;
	}

	// not necessary
	/**
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Triple<?,?,?>) {
			Triple<?,?,?> otherPair = (Triple<?,?,?>) obj;
			return trd.equals(otherPair.trd) && snd.equals(otherPair.snd) && fst.equals(otherPair.fst);
		} else
			return false;

	}

	/**
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return fst.hashCode() ^ snd.hashCode() ^ trd.hashCode();
	}
}
