package gumbo.gui.gumboguiMixIO;

import java.awt.TextField;

import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;

class DirOutWindowMixIO extends JPanel {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public DirOutWindowMixIO(TextField op, JButton ob){
		JLabel lab2 = new JLabel("Output directory:");
		
		add(lab2);
		add(op);
		add(ob);
	}
	
}