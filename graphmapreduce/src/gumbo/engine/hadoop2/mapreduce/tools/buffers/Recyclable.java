package gumbo.engine.hadoop2.mapreduce.tools.buffers;

public interface Recyclable<T extends Recyclable<T>> {
	void set(T obj);
	T duplicate();
}