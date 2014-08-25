/**
 * Created: 25 Aug 2014
 */
package mapreduce.guardedfragment.convertors;

import java.util.LinkedList;

import mapreduce.guardedfragment.planner.structures.data.RelationSchema;
import mapreduce.guardedfragment.structure.gfexpressions.GFAtomicExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFExistentialExpression;

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

		if (!gfe.isBasicGF()) {
			throw new GFConversionException("The supplied expression is not a Basic GF: " + gfe);
		}

		String output = "";
		
		RelationSchema outSchema = gfe.getOutputRelation().getRelationSchema();
		RelationSchema gSchema = gfe.getGuard().getRelationSchema();
		
		int i = 1;
		String gName = gSchema.getName();
		String oName = gSchema.getName(); // in case the loop is not executed, this is the fallback
		
		for( GFAtomicExpression gd : gfe.getGuardedRelations()){
			RelationSchema gdSchema = gd.getRelationSchema();
			
			oName = outSchema.getName() + i;

			output += convert1Atom(oName,gName, gSchema, gdSchema);
			
			gName = oName;
			i++;
		}
		
		output += outSchema.getName() + " = " +  oName + ";";

		return output;

	}

	public String convert1Atom(String currentOutName, String currentGuardName, RelationSchema guardSchema, RelationSchema guardedSchema) {
		String output = "";
		
		String match = generateMatch(guardedSchema, guardedSchema);
		String tmp = generateNewName(guardedSchema.getName());
		
		output += tmp + "_A = " + "COGROUP " + currentGuardName + " BY " + match + "," + guardedSchema.getName() + " BY " + match + ";\n";
		output += tmp + "_B = " + "FILTER " + tmp+"_A" + " BY IsEmpty("+guardedSchema.getName()+");\n";
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
	private String generateMatch(RelationSchema s1, RelationSchema s2) {
		// CLEAN better code + put it in RelationSchema
		String[] fields1 = s1.getFields();
		String[] fields2 = s2.getFields();
		LinkedList<String> common = new LinkedList<>();
		String result = "";
		
		for (String x : fields1) {
			for (String y : fields2) {
				if(x==y)
					result += ", " + x;
			}	
		}
		
		if(result.length() > 1)
			result = result.substring(2);

		

		return "(" + result + ")";
	}

}
