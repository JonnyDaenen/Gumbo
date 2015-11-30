package gumbo.engine.hadoop2.mapreduce.tools.tupleops;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;

import gumbo.engine.hadoop2.mapreduce.tools.QuickWrappedTuple;
import gumbo.structures.booleanexpressions.BEvaluationContext;
import gumbo.structures.booleanexpressions.BExpression;
import gumbo.structures.booleanexpressions.BVariable;
import gumbo.structures.booleanexpressions.VariableNotFoundException;
import gumbo.structures.conversion.GFBooleanMapping;
import gumbo.structures.conversion.GFtoBooleanConversionException;
import gumbo.structures.conversion.GFtoBooleanConvertor;
import gumbo.structures.data.QuickTuple;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;


/**
 * Guard tuple checker for reducer of evaluation operation.
 * It can evaluate the boolean expression and project the guard onto
 * an output schema. It also keeps track of a target file location.
 * 
 * @author Jonny Daenen
 *
 */
public class TupleEvaluator {

	private BExpression be;
	private BEvaluationContext context;
	private Map<Integer,Integer> mapping;
	
	private List<Integer> fields;
	private String filename;
	
	EqualityFilter ef;
	
	public TupleEvaluator(GFExistentialExpression e, String filename, Map<GFAtomicExpression, Integer> atomids) {
		
		this.filename = filename;
		
		GFAtomicExpression guard = e.getGuard();
		ef = new EqualityFilter(guard);
		
		//  get output fields
		fields = new ArrayList<>();
		GFAtomicExpression outrel = e.getOutputRelation();
		String [] guardVars = e.getGuard().getVars();
		for (String var : outrel.getVars()) {
			for (int i = 0; i < guardVars.length; i++) {
				if (guardVars[i].equals(var)) {
					fields.add(i);
					break;
				}
			}
		}
		
		// construct mapping between atom ids and var ids.
		try {
			GFtoBooleanConvertor convertor = new GFtoBooleanConvertor();
			be = convertor.convert(e.getChild());
			GFBooleanMapping varmapping = convertor.getMapping();
			
			context = new BEvaluationContext();
			mapping = new HashMap<Integer, Integer>();
			
			for (GFAtomicExpression atom : e.getGuardedAtoms()) {
				BVariable var = varmapping.getVariable(atom);
				mapping.put(atomids.get(atom), var.getID());
			}
			
			
		} catch (GFtoBooleanConversionException e1) {
			// should not happen
			e1.printStackTrace();
		}
		
	}

	/**
	 * Check if the guarded part is satisfied.
	 * 
	 * @param atomids truth values for all atoms
	 * @return true iff expression evaluates to true
	 */
	public boolean eval(boolean[] atomids) {

		context.clear();
		
		// set correct atoms ids to true
		for (int i : mapping.keySet()) {
			// only set true atoms, as missing once are false
			if (i < atomids.length && atomids[i])
				context.setValue(mapping.get(i), atomids[i]);
		}
		try {
			return be.evaluate(context);
		} catch (VariableNotFoundException e) {
			// should not happen
			e.printStackTrace();
		}
		
		return false;
	}

	
	/**
	 * Projects the guard tuple to the correct fields 
	 * and puts them in the output writable.
	 * 
	 * @param data guarded data
	 * @param output
	 * @param atomids 
	 * @return 
	 */
	public boolean project(QuickWrappedTuple qt, Text output, boolean[] atomids) {
		
		// check guard and formula satisfaction
		if (!ef.check(qt) || !eval(atomids))
			return false;
		
		qt.project(fields, output);
		
		return true;
	}
	

	/**
	 * Returns the location the output of this projection should go to.
	 * @return output location
	 */
	public String getFilename() {
		return filename;
	}

}
