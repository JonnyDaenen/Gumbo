package gumbo.experiments.grouping;

import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.engine.general.grouper.structures.CalculationGroup;
import gumbo.engine.general.settings.AbstractExecutorSettings;
import gumbo.engine.hadoop.converter.HadoopCostSheet;
import gumbo.engine.hadoop.reporter.RelationReport;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFExistentialExpression;

import java.util.Collection;
import java.util.HashMap;

public class GroupingTest1 extends HadoopCostSheet {


	
	public GroupingTest1(RelationFileMapping mapping,
			AbstractExecutorSettings settings) {
		super(null, null);
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public void initialize(CalculationGroup group,
			Collection<GFExistentialExpression> expressions) {

		
		reports = new HashMap<>();
		this.group = group;
		
		RelationSchema r = new RelationSchema("R",2);
		RelationSchema s = new RelationSchema("S",1);
		RelationSchema t = new RelationSchema("T",1);

		
		long mfactor = 1000000;

		RelationReport rr = new RelationReport(r);
		rr.setNumInputBytes(10*100*mfactor);
		rr.setEstInputTuples(rr.getNumInputBytes()/100);
		rr.setEstIntermBytes(rr.getNumInputBytes()*(group.size()-1));
		rr.setEstIntermKeyBytes((long) (rr.getEstIntermBytes()*0.1));
		rr.setEstIntermTuples(rr.getEstInputTuples()*(group.size()-1));
		rr.setEstIntermValueBytes((long) (rr.getEstIntermBytes()*0.9));
		rr.setNumFiles(1);
		
		
		RelationReport sr = new RelationReport(s);
		sr.setNumInputBytes(10*100*mfactor);
		sr.setEstInputTuples(sr.getNumInputBytes()/100);
		sr.setEstIntermBytes(sr.getNumInputBytes());
		sr.setEstIntermKeyBytes((long) (sr.getEstIntermBytes()*0.1));
		sr.setEstIntermTuples(sr.getEstInputTuples());
		sr.setEstIntermValueBytes((long) (sr.getEstIntermBytes()*0.9));
		sr.setNumFiles(1);
		
		RelationReport tr = new RelationReport(t);
		tr.setNumInputBytes(10*100*mfactor);
		tr.setEstInputTuples(tr.getNumInputBytes()/100);
		tr.setEstIntermBytes(tr.getNumInputBytes());
		tr.setEstIntermKeyBytes((long) (tr.getEstIntermBytes()*0.1));
		tr.setEstIntermTuples(tr.getEstInputTuples());
		tr.setEstIntermValueBytes((long) (tr.getEstIntermBytes()*0.9));
		tr.setNumFiles(1);
		
		
		if (group.getAllSchemas().contains(r)){
			reports.put(r, rr);
		}
		
		if (group.getAllSchemas().contains(s)){
			reports.put(s, sr);
		}
		
		if (group.getAllSchemas().contains(t)){
			reports.put(t, tr);
		}
		
	}


}
