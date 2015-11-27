package gumbo.generator;

import gumbo.compiler.filemapper.InputFormat;

import java.util.Iterator;
import java.util.Vector;

public class GFGeneratorInput implements Iterable<GFGeneratorInput.Relation> {
	
	private Vector<Relation> _relations;

	public class Relation {
		public String name;
		public int arity;
		public String path;
		public InputFormat format;
		public Boolean guard;
		public Boolean guarded;

		public Relation(String name, int arity, String path, InputFormat format) {
			this.name = name;
			this.arity = arity;
			this.path = path;
			this.format = format;
			this.guard = new Boolean(false);
			this.guarded = new Boolean(false);
		}
	}
	
	public GFGeneratorInput() {
		_relations = new Vector<>();
	}
	
	public void addInput(String name, int arity, String path, InputFormat format) {
		_relations.add(new Relation(name, arity, path, format));
	}
	
	public Vector<Relation> getRelations() {
		return _relations;
	}

	@Override
	public Iterator<Relation> iterator() {
		return _relations.iterator();
	}
}
