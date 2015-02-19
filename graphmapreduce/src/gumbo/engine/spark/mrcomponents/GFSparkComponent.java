/**
 * Created on: 19 Feb 2015
 */
package gumbo.engine.spark.mrcomponents;

import gumbo.engine.settings.AbstractExecutorSettings;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;

/**
 * @author Jonny Daenen
 *
 */
public class GFSparkComponent {

	protected ExpressionSetOperations eso;
//	TODO add SparkExecutorSettings settings;
	protected AbstractExecutorSettings settings;
	

	public GFSparkComponent(ExpressionSetOperations eso, AbstractExecutorSettings settings) {
		this.eso = eso;
		this.settings = settings;
	}
	
}
