package gumbo.engine.hadoop.mrcomponents;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.compiler.filemapper.RelationFileMappingException;
import gumbo.compiler.structures.data.RelationSchemaException;
import gumbo.compiler.structures.operations.GFOperationInitException;
import gumbo.engine.hadoop.settings.ExecutorSettings;
import gumbo.guardedfragment.gfexpressions.GFExistentialExpression;
import gumbo.guardedfragment.gfexpressions.GFExpression;
import gumbo.guardedfragment.gfexpressions.io.DeserializeException;
import gumbo.guardedfragment.gfexpressions.io.GFPrefixSerializer;
import gumbo.guardedfragment.gfexpressions.operations.ExpressionSetOperations;

public class ParameterPasser {


	public class ParameterLoadException extends Exception {

		public ParameterLoadException(String msg, Exception e) {
			super(msg,e);
		}

		private static final long serialVersionUID = 1L;

	}

	private Configuration conf;
	GFPrefixSerializer serializer = new GFPrefixSerializer();

	public ParameterPasser(Configuration conf) {
		this.conf = conf;
	}


	public ExpressionSetOperations loadESO() throws ParameterLoadException {
		try {
			HashSet<GFExistentialExpression> formulaSet = new HashSet<GFExistentialExpression>();
			String formulaString = conf.get("formulaset");
			Set<GFExpression> deserSet = serializer.deserializeSet(formulaString);

			// check whether the type is existential
			// FUTURE allow other types?
			for (GFExpression exp : deserSet) {
				if (exp instanceof GFExistentialExpression) {
					formulaSet.add((GFExistentialExpression) exp);
				}
			}


			// get relation name
			String relmapping = conf.get("relationfilemapping");

			FileSystem fs = FileSystem.get(conf);
			RelationFileMapping rm = new RelationFileMapping(relmapping,fs);


			RelationFileMapping mapping;
			ExpressionSetOperations eso = new ExpressionSetOperations(formulaSet, rm);

			return eso;
		} catch (DeserializeException | GFOperationInitException | RelationSchemaException | RelationFileMappingException | IOException e) {
			throw new ParameterLoadException("Failed to load expression parameters.", e);
		}

	}

	public ExecutorSettings loadSettings() {
		return new ExecutorSettings(); // FIXME load
	}

}
