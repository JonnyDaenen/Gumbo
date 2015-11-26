/**
 * Created on: 21 Mar 2015
 */
package gumbo.gui.panels;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.Font;

import javax.swing.BorderFactory;
import javax.swing.JEditorPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;

/**
 * @author jonny
 *
 */
public class QueryInputField extends JPanel {

	private JEditorPane inputEditor;

	public QueryInputField() {
		super(new BorderLayout());
		this.setBorder( BorderFactory.createCompoundBorder(
				BorderFactory.createTitledBorder("Query"),
				BorderFactory.createEmptyBorder(5,5,5,5)));
		
		// query input
		inputEditor = new JEditorPane();
		inputEditor.setEditable(true);
		//		inputEditor.setFont(new Font("Courier New",0,14));
		inputEditor.setFont(new Font("monospaced", Font.PLAIN, 12));
		
		JScrollPane scroll = new JScrollPane(inputEditor);
//		scroll.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
		scroll.setPreferredSize(new Dimension(400,300));
		scroll.setMinimumSize(new Dimension(200, 200));
		
		this.add(scroll,BorderLayout.CENTER);
		
	}
	
	public JEditorPane getQueryField() {
		return inputEditor;
	}
	
}
