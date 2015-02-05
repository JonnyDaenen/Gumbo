package mapreduce.guardedfragment.gumbogui.gumboguiMixIO;

import java.awt.TextField;

import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;

class DirInWindowMixIO extends JPanel {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public DirInWindowMixIO(TextField ip, JButton ib){
		JLabel lab1 = new JLabel("   Input directory:");
		
		add(lab1);
		add(ip);
		add(ib);
		
	}
	
}