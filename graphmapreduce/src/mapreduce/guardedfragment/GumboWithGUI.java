package mapreduce.guardedfragment;

import java.awt.Dimension;
import java.awt.Font;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JEditorPane;
import javax.swing.JFrame;
import javax.swing.JTextArea;

import mapreduce.guardedfragment.executor.hadoop.HadoopExecutor;
import mapreduce.guardedfragment.executor.spark.SparkExecutor;
import mapreduce.guardedfragment.planner.GFMRPlanner;
import mapreduce.guardedfragment.planner.partitioner.HeightPartitioner;
import mapreduce.guardedfragment.planner.structures.MRPlan;
import mapreduce.guardedfragment.planner.structures.RelationFileMapping;
import mapreduce.guardedfragment.structure.gfexpressions.GFExistentialExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFExpression;
import mapreduce.guardedfragment.structure.gfexpressions.io.DeserializeException;
import mapreduce.guardedfragment.structure.gfexpressions.io.GFInfixSerializer;
import mapreduce.guardedfragment.structure.gfexpressions.io.GFPrefixSerializer;

import org.apache.hadoop.fs.Path;




public class GumboWithGUI extends JFrame {
	
	/* Eclipse tells me that this program does not have serialVersionUID
	 * So I ask Eclipse to generate one.
	 * I don't know what it is for though*/
	private static final long serialVersionUID = 5033027026087017263L;
	
	// GUI variables
	private static JEditorPane editorIQ;
	private static JEditorPane editorIO;
	private static JTextArea textConsole;
	
	private static JButton buttonQC;
	private static JButton buttonSche;
	private static JButton buttonFH;
	private static JButton buttonFS;
	
	private static JCheckBox cbLevel;
	
	// Gumbo's variable
	
	private static Set<GFExpression> inputQuery;
	


	public static void main(String[] args) {
		
		editorIQ = new JEditorPane();
		editorIQ.setEditable(true);
		editorIQ.setFont(new Font("Courier New",1,19));
		
		editorIO = new JEditorPane();
		editorIO.setEditable(true);
		editorIO.setFont(new Font("Courier New",1,19));
		
		textConsole = new JTextArea();
		textConsole.setEditable(false);
		textConsole.setFont(new Font("Courier New",1,19));
		
		buttonQC = new JButton("Query Compiler");		
		buttonSche = new JButton("Jobs constructor");
		buttonFH = new JButton("GUMBO-Hadoop");
		buttonFS = new JButton("GUMBO-Spark");
		cbLevel = new JCheckBox("with schedule");
		
		GumboMainWindow mainwindow = new GumboMainWindow(editorIQ, editorIO, textConsole,buttonQC,
				buttonSche,buttonFH,buttonFS,cbLevel);
			
		Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();

        int height = screenSize.height;
        int width = screenSize.width;
        screenSize.setSize(width*(0.99), height*(0.99));
        int newheight = screenSize.height;
        int newwidth = screenSize.width;
		
		mainwindow.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		mainwindow.setSize(newwidth, newheight);
		mainwindow.setVisible(true);
		
	    buttonQC.addActionListener(new ActionListener() {
	        public void actionPerformed(ActionEvent e) {
	        	textConsole.setText("");
	        	textConsole.append("Compiling the input queries...\n");
	        	
	        	GFInfixSerializer parser = new GFInfixSerializer();
	        	
	        	try {
	        		inputQuery = parser.GetGFExpression(editorIQ.getText());
	        	} catch (DeserializeException exc) {
	        		
	        		StringWriter errors = new StringWriter();
	        		exc.printStackTrace(new PrintWriter(errors));
	        		textConsole.append(errors.toString());
	        		
	    			//exc.printStackTrace();
	    		} catch (Exception exc) {
	    			exc.printStackTrace();
	    		}
				
				//GFExpression gfe = parser.deserialize(query);

	        	textConsole.append("The following queries compiled:\n");
	        	
	        	for (GFExpression gf : inputQuery) {
	        		textConsole.append(gf.toString()+"\n");
	        		
	        	}
	        	
	        	
	        	
	        	/*for (GFExpression gfe : inputQuery) {
	        		textConsole.append(gfe.toString());
	        	}*/
	        	//textConsole.append(editorIQ.getText());
	        	//textConsole.append("\n");
	        	//textConsole.append("Checking the input and output files...\n");
	        	//textConsole.append("Input and output files checked!");
	        	//textConsole.append(editorIO.getText());
	       		//textConsole.append("\n");
	       	}
	    });
		
		
		
	}


}

