/**
 * Created: 25 Aug 2014
 */
package gumbo.convertors.pig;

import gumbo.convertors.GFConversionException;
import gumbo.structures.conversion.DNFConverter;
import gumbo.structures.conversion.DNFConverter.DNFConversionException;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.GFExpression;
import gumbo.structures.gfexpressions.GFNotExpression;

/**
 * Contains methods to convert a Basic GF Formula to a Pig Latin script.
 * FIXME this is not working yet!
 * 
 * @author Jonny Daenen
 * 
 */
public class GFToPig {
	
	int counter = 0;

	/**
	 * Converts a basic GF expression to a Pig Latin script.
	 * 
	 * @param gfe
	 *            a basic GFE
	 * @return an equivalent Pig Latin script
	 * 
	 * @throws GFConversionException
	 */
	public String convert(GFExistentialExpression gfe) throws GFConversionException {

		if (!gfe.isBasicGF() ) {
			throw new GFConversionException("The supplied expression is not a Basic GF: " + gfe);
		}
		
		// convert to DNF
		DNFConverter converter = new DNFConverter();
		GFExpression gfe2;
		try {
			gfe2 = converter.convert(gfe);
		} catch (DNFConversionException e) {
			throw new GFConversionException("Something went wrong during conversion to DNF.",e);
		}

		if(gfe2.containsOr())
			throw new GFConversionException("DNF-version of "+gfe+" contains an OR-operation: " + gfe2);
			
		// work with new formula
		gfe = (GFExistentialExpression) gfe2;
		
		String output = "";
		
		GFAtomicExpression outSchema = gfe.getOutputRelation();
		GFAtomicExpression gSchema = gfe.getGuard();

		
		int i = 1;
		String gName = gSchema.getName();
		String oName = gSchema.getName(); // in case the loop is not executed, this is the fallback
		
		for( GFAtomicExpression gd : gfe.getGuardedRelations()){
			
			// find out if it is negated (formula is in DNF with non-nested negation!)
			GFExpression parent = gfe.getParent(gd);
			boolean negated = false;
			if (parent instanceof GFNotExpression)
				negated = true;
			
			oName = outSchema.getName() + i;

			output += convert1Atom(oName,gName, gSchema, gd, negated);
			
			gName = oName;
			i++;
		}
		
		output += outSchema.getName() + " = " +  oName + ";";

		return output;

	}

	public String convert1Atom(String currentOutName, String currentGuardName, GFAtomicExpression guardSchema, GFAtomicExpression guardedSchema, boolean negated) {
		String output = "";
		
		String match = generateMatch(guardSchema, guardedSchema);
		String tmp = generateNewName(guardedSchema.getName());
		
		// if not negated, we need not (!confusing)
		String negPart = "";
		if(!negated)
			negPart = "NOT";
		
		output += tmp + "_A = " + "COGROUP " + currentGuardName + " BY " + match + ", " + guardedSchema.getName() + " BY " + match + ";\n";
		output += tmp + "_B = " + "FILTER " + tmp+"_A" + " BY "+negPart+"(IsEmpty("+guardedSchema.getName()+"));\n";
		output += currentOutName + " = FOREACH " + tmp+"_B" + " GENERATE flatten(" +currentGuardName+ ");\n";
		output += "\n";
		
		
		return output;

	}

	/**
	 * @param name
	 * @return
	 */
	private String generateNewName(String name) {
		return name + "_" + counter;
	}

	/**
	 * @param guardedSchema
	 * @param guardedSchema2
	 * @return
	 */
	private String generateMatch(GFAtomicExpression s1, GFAtomicExpression s2) {
		// CLEAN better code + put it in RelationSchema
		String[] fields1 = s1.getVars();
		String[] fields2 = s2.getVars();
		
		String result = "";
		for (String x : fields1) {
			for (String y : fields2) {
				if(x.equals(y))
					result += ", " + x;
			}	
		}
		
		if(result.length() > 1)
			result = result.substring(2);

		

		return "(" + result + ")";
	}

}
