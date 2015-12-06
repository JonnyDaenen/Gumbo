package gumbo.engine.hadoop2.converter;

import java.util.Comparator;

import org.apache.hadoop.fs.Path;

import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.io.Pair;

public class HelpComparator implements Comparator<Pair<RelationSchema, Path>> {

	@Override
	public int compare(Pair<RelationSchema, Path> o1, Pair<RelationSchema, Path> o2) {
		return o1.toString().compareTo(o2.toString());
	}
	

}
