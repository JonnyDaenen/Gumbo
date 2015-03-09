package gumbo.gui.gumbogui;

import java.awt.GridLayout;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JLabel;
import javax.swing.JPanel;

public class PanelD extends JPanel {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public PanelD(JButton bQC, JButton bS, JButton bFH, JButton bFS, JCheckBox cbL) {
		super();
		setLayout(new GridLayout(2,3,20,-3));
		
		//setPreferredSize(new Dimension(1200, 30));
		
		add(bQC);
		//add(bS);
		add(bFH);
		add(bFS);	
		add(new JLabel(""));
		//add(cbL);		
		add(new JLabel(""));
		add(new JLabel(""));
					
	}

}
