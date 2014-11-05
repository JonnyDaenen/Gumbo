package mapreduce.guardedfragment.gumbogui.gumboguiSeparateIO;

import java.awt.GridLayout;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JLabel;
import javax.swing.JPanel;

public class WindowThreeSeparateIO extends JPanel {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public WindowThreeSeparateIO(JButton bQC, JButton bS, JButton bFH, JButton bFS, JCheckBox cbL) {
		super();
		setLayout(new GridLayout(2,4,20,-3));
		
		//setPreferredSize(new Dimension(1200, 30));
		
		add(bQC);
		add(bS);
		add(bFH);
		add(bFS);	
		add(new JLabel(""));
		add(cbL);		
		add(new JLabel(""));
		add(new JLabel(""));
					
	}

}
