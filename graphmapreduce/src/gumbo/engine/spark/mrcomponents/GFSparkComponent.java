/**
 * Created on: 19 Feb 2015
 */
package gumbo.engine.spark.mrcomponents;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.compiler.filemapper.RelationFileMappingException;
import gumbo.engine.settings.AbstractExecutorSettings;
import gumbo.engine.settings.ExecutorSettings;
import gumbo.structures.data.RelationSchemaException;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.GFExpression;
import gumbo.structures.gfexpressions.io.DeserializeException;
import gumbo.structures.gfexpressions.io.GFPrefixSerializer;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations.GFOperationInitException;

/**
 * @author Jonny Daenen
 *
 */
public class GFSparkComponent implements Externalizable {



	private static final long serialVersionUID = 1L;

	protected ExpressionSetOperations eso;
//	TODO add SparkExecutorSettings settings;
	protected AbstractExecutorSettings settings;
	

	public GFSparkComponent(ExpressionSetOperations eso, AbstractExecutorSettings settings) {
		this.eso = eso;
		this.settings = settings;
	}

	/**
	 * For deserialization purposes.
	 */
	public GFSparkComponent() {
		eso = null;
		settings = new ExecutorSettings();
	}

	/* (non-Javadoc)
	 * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
	 */
	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		GFPrefixSerializer serializer = new GFPrefixSerializer();
		String esoSer = serializer.serializeSet(eso.getExpressionSet());
		String fileMapSer = eso.getFileMapping().toString();
		out.writeObject(esoSer);
		out.writeObject(fileMapSer);
		out.writeObject(settings.save());
		
		
	}

	/* (non-Javadoc)
	 * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
	 */
	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		String esoSer = (String) in.readObject();
		String fileMapSer = (String) in.readObject();
		settings.load((String) in.readObject());

		GFPrefixSerializer serializer = new GFPrefixSerializer();
		try {
			
			Set<GFExpression> exps = serializer.deserializeSet(esoSer);
			Collection<GFExistentialExpression> formulaSet = new HashSet<>();
			for (GFExpression exp : exps) {
				if (exp instanceof GFExistentialExpression) {
					formulaSet.add((GFExistentialExpression) exp);
				}
			}
			
			RelationFileMapping rfm = new RelationFileMapping(fileMapSer,null); // FIXME filesystem??
			eso = new ExpressionSetOperations(formulaSet, rfm);
			
		} catch (DeserializeException | GFOperationInitException | RelationSchemaException | RelationFileMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	

	
}
