package gumbo.gui.gumbogui;

import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.awt.TextField;

import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;

public class PanelC extends JPanel {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public PanelC(String s, TextField op, JButton ob, JButton deleteButton){
		JLabel lab2 = new JLabel(s);
		
		add(lab2);
		add(op);
		add(ob);
		add(deleteButton);
	}
	
	
}