/**
 * Created on: 21 Mar 2015
 */
package gumbo.gui.panels;

import java.awt.BorderLayout;
import java.awt.Font;

import javax.swing.BorderFactory;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;

/**
 * @author Jonny Daenen
 *
 */
public class ConsolePanel extends JPanel {
	
	JTextArea console;

	public ConsolePanel() {
		super(new BorderLayout());

		// TODO Auto-generated constructor stub
		this.setBorder( BorderFactory.createCompoundBorder(
				BorderFactory.createTitledBorder("Console"),
				BorderFactory.createEmptyBorder(5,5,5,5)));

		// query input
		console = new JTextArea();
		console.setEditable(false);
		//		inputEditor.setFont(new Font("Courier New",0,14));
		console.setFont(new Font("monospaced", Font.PLAIN, 11));

		JScrollPane scroll = new JScrollPane(console);
		add(scroll,BorderLayout.CENTER);
	}

	public JTextArea getConsoleField() {
		return console;
	}
}
