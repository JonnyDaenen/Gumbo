/**
 * Created on: 12 Mar 2015
 */
package gumbo.compiler.plan;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;

import gumbo.compiler.GumboPlan;
import gumbo.compiler.calculations.BasicGFCalculationUnit;
import gumbo.compiler.calculations.CalculationUnit;
import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.compiler.partitioner.PartitionedCUGroup;
import gumbo.structures.data.RelationSchema;
import gumbo.utils.GraphViz;

/**
 * @author Jonny Daenen
 *
 */
public class GraphVizPlanVisualizer implements PlanVisualizer {

	private Map<Object, String> nodeids;
	private int counter;

	protected boolean detailsOn = true;


	private String inputNode = "";


	/** Converts the plan to a GraphViz dot format
	 * @see gumbo.compiler.plan.PlanVisualizer#visualize(gumbo.compiler.GumboPlan)
	 */
	@Override
	public String visualize(GumboPlan gp) {

		nodeids = new HashMap<Object, String>();
		counter = 0;

		GraphViz gv = new GraphViz();
		gv.start_graph();
		gv.addln("compound=true;"); // for cluster connections
		gv.addln("nodesep=1.5;");
		gv.addln("ranksep=1;");
		gv.addln("rankdir=TB;");
		gv.addln("node [shape=box,style=rounded];");
		gv.addln("splines=spline;");
//		gv.addln("struct1[shape=record,label=\"{test|test2}\"];");
		//		gv.addln("{rank=same n4 n5 n6 n7}");

		gv.setSubgraphOptions("style=\"dashed\";");

		// input relations
		dotInputRelations(gv,gp);
		dotPartitions(gv,gp);

		gv.end_graph();

		System.out.println(gv.getDotSource());

		return gv.getDotSource();
	}

	/**
	 * @param gv 
	 * @param gp
	 * @return
	 */
	private void dotPartitions(GraphViz gv, GumboPlan gp) {

		int i = 1;
		String oldID = this.inputNode;
		String currentID = "";
		for (CalculationUnitGroup p : gp.getPartitions().getBottomUpList()) {


			// render partition
			gv.startSubgraph("clusterPartition_" + i, "Partition " + i);
			

			// list nodes
			for (CalculationUnit cu :  p.getCalculations()) {
				// node name
				String nodeID = getID(cu.getOutputSchema().getName());
				gv.addNode(nodeID, getNodeLabel(cu));

				if(detailsOn) {
					// links to inputs
					for (RelationSchema rs : cu.getInputRelations()) {
						gv.addEdge(nodeID, getID(rs.getName()), "", "color=\"#000000F0\",constraint=false"); // constraint = false means that they are not used in layout determination
					}
				}
				currentID = nodeID;
			}

			addDummyNode(gv, i);
			gv.endSubgraph();

			// link this partition to its inputs
			for (RelationSchema rs : p.getInputRelations()) {
				gv.addEdge(currentID, getID(rs.getName()), "", "ltail=clusterPartition_"+(i)+",constraint=false");
			}

			// inter-cluster dependencies (invis)
			if (!oldID.isEmpty()){
				gv.addEdge(currentID, oldID, "", "ltail=clusterPartition_"+(i)+",lhead=clusterPartition_"+(i-1)+",weight=100,style=invis");
//				gv.addEdge("DUMMY_"+i, "DUMMY_"+(i-1), "", "ltail=clusterPartition_"+(i)+",lhead=clusterPartition_"+(i-1)+",weight=100,style=dashed");
			}
			oldID = currentID;





			i++;
		}

	}

	/**
	 * @param cu
	 * @return
	 */
	private String getNodeLabel(CalculationUnit cu) {
		String output = cu.getOutputSchema().getName();

		if (detailsOn) {
			output += "\\n";
			//		output += "<BR>";
			//		output += "<FONT COLOR=\"#999999\">test"; 

			output += ((BasicGFCalculationUnit)cu).getBasicExpression();
			//		output += "</FONT>";
		}
		return output;
	}

	/**
	 * @param gv 
	 * @param gp
	 * @return
	 */
	private void dotInputRelations(GraphViz gv, GumboPlan gp) {
		gv.startSubgraph("clusterPartition_0", "InputRelations");
		
		
		for (RelationSchema rs : gp.getFileManager().getInFileMapping().getSchemas()) {
			// input schema
			String nodeID = getID(rs.getName());
			this.inputNode = nodeID;
			gv.addNode(nodeID,rs.getName());

			// input details
			dotInputDetails(gv, gp, rs, gp.getFileManager().getInFileMapping().getPaths(rs));
		}
		
		addDummyNode(gv,0);
		gv.endSubgraph();
	}


	private void addDummyNode(GraphViz gv, int i) {
		gv.setNodeOptions("shape=point,style=invis");
//		gv.addNode("DUMMY_"+i);
		gv.clearNodeOptions();
	}

	/**
	 * @param gv
	 * @param rs
	 * @param paths
	 */
	private void dotInputDetails(GraphViz gv, GumboPlan gp, RelationSchema rs, Set<Path> paths) {
		gv.setNodeOptions("style=\"filled,rounded\",bgcolor=\"#D0C0A0\"");
		for (Path p : gp.getFileManager().getInFileMapping().getPaths(rs)) {
			gv.addNode(getID(p),p.toString());
			gv.addEdge(getID(rs.getName()),getID(p));
		}
		gv.clearNodeOptions();

	}

	private String getID(Object obj) {
		if (nodeids.containsKey(obj)) {
			return nodeids.get(obj);
		} else
		{
			nodeids.put(obj, "n"+counter++);
			return nodeids.get(obj);
		}
	}
	
	public void setDetailsOn(boolean detailsOn) {
		this.detailsOn = detailsOn;
	}

	/**
	 * @param plan
	 * @param string
	 */
	public void savePlan(GumboPlan plan, String file) {
		String dotCode = visualize(plan);
		GraphViz gv = new GraphViz();
		File f = new File(file);
		gv.writeGraphToFile(gv.getGraph(dotCode, "png"), f);
	}

}
