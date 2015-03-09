package gumbo.gui.gumbogui;

import java.awt.FlowLayout;

import javax.swing.JPanel;

public class PanelDC extends JPanel {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public PanelDC (PanelD w, PanelC dw) {
		
		setLayout(new FlowLayout());
		add(dw);
		add(w);
		add(new JPanel());
		setVisible(true);
			
	}

}
