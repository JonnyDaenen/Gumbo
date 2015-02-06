package gumbo.gui.gumboguiMixIO;

import java.awt.BorderLayout;
import java.awt.TextField;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JEditorPane;
import javax.swing.JPanel;
import javax.swing.JTextArea;

public class GumboMainWindowMixIO extends JPanel {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public GumboMainWindowMixIO(JEditorPane editorIQ, JTextArea textConsole, JButton buttonQC, 
			JButton buttonSche, JButton buttonFH, JButton buttonFS, JCheckBox cbLevel,
			TextField ip, TextField op, JButton browseIn, JButton browseOut) {
		
		super();
		setBorder(BorderFactory.createCompoundBorder(
			    BorderFactory.createEmptyBorder(10, 10, 10, 10), // outer border
			    BorderFactory.createEmptyBorder(0,0,0,0)));
				
		TextWindowMixIO textWin = new TextWindowMixIO(new TextWindowMixIOUp(editorIQ), 
				new TextWindowMixIOBot(textConsole));	
				
		ButtonWindowMixIO buttonWin = new ButtonWindowMixIO(buttonQC,buttonSche,
				buttonFH,buttonFS,cbLevel);
		
		
		setLayout(new BorderLayout());

		add(textWin,BorderLayout.NORTH);
		
		JPanel p1 = new JPanel();
		p1.setLayout(new BorderLayout());
		p1.add(new DirInWindowMixIO(ip,browseIn),BorderLayout.NORTH);
	
		JPanel p2 = new JPanel();
		p2.setLayout(new BorderLayout());
		p2.add(new DirOutWindowMixIO(op,browseOut),BorderLayout.NORTH);
		
		JPanel p3 = new JPanel();
		p3.setLayout(new BorderLayout());
		p3.add(buttonWin,BorderLayout.NORTH);
		
		p2.add(p3,BorderLayout.CENTER);
		p1.add(p2,BorderLayout.CENTER);
		add(p1,BorderLayout.CENTER);
		
        setVisible(true);
      
	}

}
