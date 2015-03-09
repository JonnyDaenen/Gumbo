package gumbo.gui.gumbogui;

import java.awt.TextField;

import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;

public class PanelC extends JPanel {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public PanelC(TextField op, JButton ob){
		JLabel lab2 = new JLabel("Output directory:");
		
		add(lab2);
		add(op);
		add(ob);
	}
	
}