/**
 * Created on: 21 Mar 2015
 */
package gumbo.gui.panels;

import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;

/**
 * @author Jonny Daenen
 *
 */
public class SettingsPanel extends JPanel {

	private JButton compileInfix;
	private JButton compileGumboSQL;
	private JButton execHadoop;
	private JButton execSpark;
	private JComboBox<String> partitioners;
	

	String[] partitionerStrings = { "Unit", "Optimal", "Height", "Depth" };
	private JCheckBox queryDetails;
	private JCheckBox edgeDetails;

	public SettingsPanel() {
		super(new GridBagLayout());

		// TODO Auto-generated constructor stub
		


		JPanel settings = new JPanel(new GridBagLayout());
		settings.setBorder( BorderFactory.createCompoundBorder(
				BorderFactory.createTitledBorder("Settings"),
				BorderFactory.createEmptyBorder(5,5,5,5)));
		

		// query input

		// scratch
		JLabel partitionerLabel = new JLabel("Partitioner:");
		partitioners = new JComboBox<String>(partitionerStrings);
		
		// output
		JLabel edgeDetailslabel = new JLabel("Edge details:");
		edgeDetails = new JCheckBox("");
		edgeDetails.setSelected(false);
		
		// output
		JLabel queryDetailsLabel = new JLabel("Query details:");
		queryDetails = new JCheckBox("");
		queryDetails.setSelected(false);


		GridBagConstraints c = new GridBagConstraints();
		c.fill = GridBagConstraints.NONE;
		c.weightx = 0;
		c.weighty = 0;
		c.anchor = GridBagConstraints.FIRST_LINE_END;

		c.fill = GridBagConstraints.NONE;
		c.anchor = GridBagConstraints.LINE_END;
		c.gridx = 0;
		c.gridy = 0;
		c.weightx = 0;
		settings.add(partitionerLabel,c);
		c.anchor = GridBagConstraints.LINE_START;
		c.gridx = 1;
		c.gridy = 0;
		c.weightx = 0;
		c.weighty = 0;
		c.gridwidth = 3;
		settings.add(partitioners,c);
		c.gridwidth = 1;


		c.weightx = 0;
		c.weighty = 0;

		c.fill = GridBagConstraints.NONE;
		c.anchor = GridBagConstraints.LINE_END;
		c.gridx = 0;
		c.gridy = 1;
		c.weightx = 0;
		settings.add(edgeDetailslabel,c);
		c.anchor = GridBagConstraints.LINE_START;
		c.fill = GridBagConstraints.HORIZONTAL;
		c.gridx = 1;
		c.gridy = 1;
		c.weightx = 0;
		settings.add(edgeDetails,c);
		

		c.fill = GridBagConstraints.NONE;
		c.anchor = GridBagConstraints.LINE_END;
		c.gridx = 0;
		c.gridy = 2;
		c.weightx = 0;
		settings.add(queryDetailsLabel,c);
		c.anchor = GridBagConstraints.LINE_START;
		c.fill = GridBagConstraints.HORIZONTAL;
		c.gridx = 1;
		c.gridy = 2;
		c.weightx = 0.5;
		settings.add(queryDetails,c);
		
		JPanel buttons = new JPanel();
		compileInfix = new JButton("Compile Infix");
		compileGumboSQL = new JButton("Compile GumboSQL");
		execHadoop = new JButton("Exec Hadoop");
		execSpark = new JButton("Exec Spark");
		buttons.setLayout(new FlowLayout());
		buttons.add(compileGumboSQL);
		buttons.add(compileInfix);
		buttons.add(execHadoop);
		buttons.add(execSpark);
		
		c.fill = GridBagConstraints.HORIZONTAL;
		c.gridx = 0;
		c.gridy = 1;
		c.weightx = 0;
		add(settings,c);
		
		c.fill = GridBagConstraints.BOTH;
		c.gridx = 0;
		c.gridy = 0;
		c.weightx = 0;
		c.weighty = 0.5;
		add(new JPanel(),c);
		
		c.fill = GridBagConstraints.HORIZONTAL;
		c.anchor = GridBagConstraints.LAST_LINE_START;
		c.gridx = 0;
		c.gridy = 2;
		c.weightx = 0;
		c.weighty = 0.5;
		add(buttons,c);
		
	}

	public JButton getCompileInfixButton() {
		return compileInfix;
	}
	
	public JButton getCompileGumboSQLButton() {
		return compileGumboSQL;
	}

	public JButton getHadoopButton() {
		return execHadoop;
	}

	public JButton getSparkButton() {
		return execSpark;
	}

	
	public JComboBox<String> getPartitionerList() {
		return partitioners;
	}

	public boolean getEdgeDetailsEnabled() {
		return this.edgeDetails.isSelected();
	}

	public boolean getQueryDetailsEnabled() {
		return this.queryDetails.isSelected();
	}
	

}
