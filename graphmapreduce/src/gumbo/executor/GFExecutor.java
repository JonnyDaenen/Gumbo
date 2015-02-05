/**
 * Created: 22 Aug 2014
 */
package gumbo.executor;

import gumbo.compiler.structures.MRPlan;

/**
 * @author Jonny Daenen
 *
 */
public interface GFExecutor {

	public void execute(MRPlan plan) throws ExecutionException;
	
}
