package gumbo.engine.hadoop.mrcomponents.tools;

import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;

import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Used to extract the filename/relationname from a context.
 * 
 * @author Jonny
 *
 */
public class RelationResolver {
	

	public class InputResolveException extends Exception {

		private static final long serialVersionUID = 1L;
		
		public InputResolveException(Exception e) {
			super(e);
		}
	}


	private static final Log LOG = LogFactory.getLog(RelationResolver.class);

	protected InputSplit prevSplit;
	protected Path cachedPath;

	ExpressionSetOperations eso;
	private RelationSchema rsCache;

	public RelationResolver(ExpressionSetOperations eso) {
		prevSplit = null;
		cachedPath = null;
		this.eso = eso;
		
		// dummy
		LOG.getClass();
	}


	protected boolean isCached(Context context) {
		InputSplit is = context.getInputSplit();
//		LOG.info("cache hit?" + (prevSplit == is));
		return prevSplit == is;
	}

	public Path extractFileName(Context context) throws InputResolveException {
		try {

			// simple caching
			if (!isCached(context)) {
				
				InputSplit is = context.getInputSplit();
				Method method = is.getClass().getMethod("getInputSplit");
				method.setAccessible(true);

				FileSplit fileSplit = (FileSplit) method.invoke(is);
				Path filePath = fileSplit.getPath();

				//			OPTIMIZE try this:
				//			String filename= ((FileSplit)context.getInputSplit()).getPath().getName();

				prevSplit = is;
				cachedPath = filePath;

			}

			return cachedPath;

		} catch(Exception e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
			throw new InputResolveException(e);
		}

	}


	public RelationSchema extractRelationSchema(Context context) throws InputResolveException {
		if (!isCached(context)) {
			Path p = extractFileName(context);
			RelationSchema rs = eso.getFileMapping().findSchema(p);
			rsCache = rs;
		}
		return rsCache;
	}
}
