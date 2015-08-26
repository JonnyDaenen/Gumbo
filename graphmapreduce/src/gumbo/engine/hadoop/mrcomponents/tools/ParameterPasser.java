package gumbo.engine.hadoop.mrcomponents.tools;

import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.compiler.filemapper.RelationFileMappingException;
import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
import gumbo.structures.data.RelationSchemaException;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.GFExpression;
import gumbo.structures.gfexpressions.io.DeserializeException;
import gumbo.structures.gfexpressions.io.GFPrefixSerializer;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations.GFOperationInitException;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

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
			

			HashSet<GFExistentialExpression> allFormulaSet = new HashSet<GFExistentialExpression>();
			String allExpString = conf.get("allexpressions");
			Set<GFExpression> allExpressions = serializer.deserializeSet(allExpString);
			for (GFExpression exp : allExpressions) {
				if (exp instanceof GFExistentialExpression) {
					allFormulaSet.add((GFExistentialExpression) exp);
				}
			}


			// get relation name
			String relmapping = conf.get("relationfilemapping");

			FileSystem fs = FileSystem.get(conf);
			RelationFileMapping rm = new RelationFileMapping(relmapping,fs);
			ExpressionSetOperations eso = new ExpressionSetOperations(formulaSet,allFormulaSet, rm);

			return eso;
		} catch (DeserializeException | GFOperationInitException | RelationSchemaException | RelationFileMappingException | IOException e) {
			throw new ParameterLoadException("Failed to load expression parameters.", e);
		}

	}

	public HadoopExecutorSettings loadSettings() {
		return new HadoopExecutorSettings(conf);
	}

}
