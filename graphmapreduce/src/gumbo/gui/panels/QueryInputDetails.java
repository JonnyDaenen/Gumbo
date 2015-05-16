/**
 * Created on: 21 Mar 2015
 */
package gumbo.gui.panels;

import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JEditorPane;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextField;

/**
 * @author jonny
 *
 */
public class QueryInputDetails extends JPanel {
	
	
	private JEditorPane inputEditor;
	private JButton scratchDelete;
	private JButton outDelete;
	private JTextField scratchField;
	private JTextField outField;
	private JButton scratchReset;
	private JButton outReset;
	

	private String defaultOutPath = "/user/cloudera/output";
	private String defaultScratchPath = "/user/cloudera/scratch";

	public QueryInputDetails() {
		super(new GridBagLayout());
		
		this.setBorder( BorderFactory.createCompoundBorder(
				BorderFactory.createTitledBorder("Query I/O"),
				BorderFactory.createEmptyBorder(5,5,5,5)));
		
		// query input
		JLabel label = new JLabel("Input:");
		inputEditor = new JEditorPane();
		inputEditor.setEditable(true);
		//		inputEditor.setFont(new Font("Courier New",0,14));
		inputEditor.setFont(new Font("monospaced", Font.PLAIN, 12));

		JScrollPane scroll = new JScrollPane(inputEditor);
		scroll.setPreferredSize(new Dimension(400,300));
		scroll.setMinimumSize(new Dimension(200, 200));
		
		// scratch
		JLabel scratchlabel = new JLabel("Scratch:");
		scratchField = new JTextField(defaultScratchPath, 20);
		scratchReset = new JButton("reset");
		scratchDelete = new JButton("delete");
		// output
		JLabel outlabel = new JLabel("Output:");
		outField = new JTextField(defaultOutPath, 20);
		outReset = new JButton("reset");
		outDelete = new JButton("delete");
		
		
		GridBagConstraints c = new GridBagConstraints();
		c.fill = GridBagConstraints.HORIZONTAL;
		c.weightx = 0;
		c.weighty = 0;
		c.anchor = GridBagConstraints.FIRST_LINE_END;
		
		c.fill = GridBagConstraints.NONE;
		c.gridx = 0;
		c.gridy = 0;
		c.weightx = 0;
		add(label,c);
		c.anchor = GridBagConstraints.FIRST_LINE_START;
		c.fill = GridBagConstraints.BOTH ;
		c.gridx = 1;
		c.gridy = 0;
		c.weightx = 0.5;
		c.weighty = 0.5;
		c.gridwidth = 3;
		add(scroll,c);
		c.gridwidth = 1;
		

		c.weightx = 0;
		c.weighty = 0;
		
		c.fill = GridBagConstraints.NONE;
		c.gridx = 0;
		c.gridy = 1;
		c.weightx = 0;
		add(outlabel,c);
		c.fill = GridBagConstraints.HORIZONTAL;
		c.gridx = 1;
		c.gridy = 1;
		c.weightx = 0;
		add(outField,c);
		c.fill = GridBagConstraints.HORIZONTAL;
		c.gridx = 2;
		c.gridy = 1;
		c.weightx = 0;
		add(outReset,c);
		c.fill = GridBagConstraints.HORIZONTAL;
		c.gridx = 3;
		c.gridy = 1;
		c.weightx = 0;
		add(outDelete,c);

		c.fill = GridBagConstraints.NONE;
		c.gridx = 0;
		c.gridy = 2;
		c.weightx = 0;
		add(scratchlabel,c);
		c.fill = GridBagConstraints.HORIZONTAL;
		c.gridx = 1;
		c.gridy = 2;
		c.weightx = 0.5;
		add(scratchField,c);
		c.fill = GridBagConstraints.HORIZONTAL;
		c.gridx = 2;
		c.gridy = 2;
		c.weightx = 0;
		add(scratchReset,c);
		c.fill = GridBagConstraints.HORIZONTAL;
		c.gridx = 3;
		c.gridy = 2;
		c.weightx = 0;
		add(scratchDelete,c);
		
		addActions();
		
	}
	
	/**
	 * 
	 */
	private void addActions() {
		outReset.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				outField.setText(defaultOutPath);
				
			}
		});
		
		scratchReset.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				scratchField.setText(defaultScratchPath);
				
			}
		});
	}

	public JTextField getOutputField() {
		return outField;
	}
	
	public JTextField getScratchField() {
		return scratchField;
	}
	
	public JEditorPane getInputField() {
		return inputEditor;
	}

	public JButton getScratchDeleteButton() {
		return scratchDelete;
	}

	/**
	 * @return
	 */
	public JButton getOutputDeleteButton() {
		return outDelete;
	}

}
