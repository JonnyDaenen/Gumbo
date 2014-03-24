package guardedfragment.mapreduce;

import com.sun.tools.javac.util.Convert;

import guardedfragment.mapreduce.jobs.BooleanMRJob;
import guardedfragment.mapreduce.jobs.ExistentialMRJob;
import guardedfragment.mapreduce.jobs.MRJob;
import guardedfragment.structure.GFAndExpression;
import guardedfragment.structure.GFExistentialExpression;
import guardedfragment.structure.GFExpression;
import guardedfragment.structure.GFNotExpression;
import guardedfragment.structure.GFOrExpression;

/**
 * Converter from a GF expression to a Map-reduce plan.
 * For now, only basic GR expressions are allowed.
 * 
 * @author Jonny Daenen
 * 
 *
 */
public class GF2MRConverter {

	MRJob convert(GFExpression e) throws ConversionNotImplementedException {
		throw new ConversionNotImplementedException();
	}
	
	MRJob convert(GFExistentialExpression e) throws ConversionNotImplementedException {
		BooleanMRJob boolpart = convertBoolean(e.getChild());
		// TODO
		
		return null;
	}

	MRJob convert(GFAndExpression e) throws ConversionNotImplementedException {
		return convertBoolean(e);
	}

	MRJob convert(GFOrExpression e) throws ConversionNotImplementedException {
		return convertBoolean(e);
	}
	
	MRJob convert(GFNotExpression e) throws ConversionNotImplementedException {
		return convertBoolean(e);
	}

	BooleanMRJob convertBoolean(GFExpression e) throws ConversionNotImplementedException {
		if (e.isAtomicBooleanCombination())
			return null; // TODO new BooleanMRJob();
		else
			throw new ConversionNotImplementedException();
	}

}
