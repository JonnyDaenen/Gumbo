/**
 * Created: 28 Apr 2014
 */
package gumbo.compiler.calculations;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFExistentialExpression;

/**
 * Converts a set of basic GF expressions into {@link CalculationUnit}s.
 * TODO #core change to GFBasicExpression
 * 
 * 
 * @author Jonny Daenen
 * 
 */
public class BGFE2CUConverter {


	private static final Log LOG = LogFactory.getLog(BGFE2CUConverter.class);



	public BasicGFCalculationUnit createCalculationUnit(GFExistentialExpression gfe) throws CalculationUnitException {
		return new BasicGFCalculationUnit(gfe);
	}
	
	public Map<RelationSchema, BasicGFCalculationUnit> createCalculationUnits(Collection<GFExistentialExpression> gfeset) throws CalculationUnitException {

		Map<RelationSchema, BasicGFCalculationUnit> basics = new HashMap<RelationSchema, BasicGFCalculationUnit>();

		for (GFExistentialExpression gfe : gfeset) {
			try {
				
					BasicGFCalculationUnit cu = createCalculationUnit(gfe);
					basics.put(gfe.getOutputSchema(), cu);
				
			} catch (Exception e) {
				LOG.error("Skipping formula because of error: " + gfe + ". Error message: "
						+ e.getMessage());
				e.printStackTrace();
				throw new CalculationUnitException("Skipping formula because of error: " + gfe + ".", e);
			}
		}

		return basics;

	
	}
}
