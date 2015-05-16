package gumbo.gui.gumbogui;

import java.awt.FlowLayout;
import java.awt.GridLayout;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;

public class PanelD extends JPanel {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public PanelD(JButton bQC, JButton bS, JButton bFH, JButton bFS, JCheckBox cbL, JComponent extra, JComponent demoList) {
		super();
		setLayout(new GridLayout(2,6,20,-3));
		
//		setLayout(new FlowLayout());
		//setPreferredSize(new Dimension(1200, 30));
		
		add(bQC);
//		add(bS);
		add(bFH);
		add(bFS);	
		add(extra);	
		add(demoList);	
		add(new JLabel(""));
		//add(cbL);		
		add(new JLabel(""));
		add(new JLabel(""));
					
	}

}
