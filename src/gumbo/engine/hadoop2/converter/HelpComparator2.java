package gumbo.engine.hadoop2.converter;

import java.util.Comparator;

import gumbo.structures.gfexpressions.GFExistentialExpression;

public class HelpComparator2 implements Comparator<GFExistentialExpression> {

	@Override
	public int compare(GFExistentialExpression o1, GFExistentialExpression o2) {
		return o1.toString().compareTo(o2.toString());
	}
	

}
