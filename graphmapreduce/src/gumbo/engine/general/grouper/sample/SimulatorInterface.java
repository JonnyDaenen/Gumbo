package gumbo.engine.general.grouper.sample;

import java.util.Collection;

import org.apache.hadoop.conf.Configuration;

import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.engine.general.algorithms.AlgorithmInterruptedException;
import gumbo.engine.general.grouper.structures.CalculationGroup;
import gumbo.engine.general.settings.AbstractExecutorSettings;
import gumbo.engine.hadoop.reporter.RelationTupleSampleContainer;
import gumbo.structures.data.RelationSchema;

public interface SimulatorInterface {
	
	public void setInfo(RelationTupleSampleContainer rtsc, RelationFileMapping mapping, AbstractExecutorSettings execSettings);
	
	public SimulatorReport execute(CalculationGroup calcJob) throws AlgorithmInterruptedException;


}
