/**
 * Created: 09 Jan 2015
 */
package mapreduce.guardedfragment.structure;

import gumbo.compiler.structures.RelationFileMapping;
import gumbo.guardedfragment.gfexpressions.GFExistentialExpression;

import java.util.Collection;

import org.apache.hadoop.fs.Path;

/**
 * Representation of a Guarded Fragment Query.
 * 
 * @author Jonny Daenen
 *
 */
public class GFQuery {
	
	Collection<GFExistentialExpression> gfees;
	RelationFileMapping filemapping;
	Path outputPath;
	Path scratchPath;
	
	public Collection<GFExistentialExpression> getExpressions() {
		return gfees;
	}
	public void setExpressions(Collection<GFExistentialExpression> gfees) {
		this.gfees = gfees;
	}
	public RelationFileMapping getFilemapping() {
		return filemapping;
	}
	public void setFilemapping(RelationFileMapping filemapping) {
		this.filemapping = filemapping;
	}
	public Path getOutputPath() {
		return outputPath;
	}
	public void setOutputPath(Path outputPath) {
		this.outputPath = outputPath;
	}
	public Path getScratchPath() {
		return scratchPath;
	}
	public void setScratchPath(Path scratchPath) {
		this.scratchPath = scratchPath;
	}
	
	
	

}
