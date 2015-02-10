/**
 * Created: 22 Aug 2014
 */
package gumbo.engine;

import gumbo.compiler.GumboPlan;
import gumbo.compiler.structures.MRPlan;

/**
 * Interface for {@link GumboPlan} execution.
 * @author Jonny Daenen
 *
 */
public interface GFEngine {

	public void execute(MRPlan plan) throws ExecutionException;
	
}
