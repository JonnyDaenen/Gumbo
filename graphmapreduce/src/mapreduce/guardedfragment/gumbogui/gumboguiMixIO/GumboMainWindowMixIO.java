package mapreduce.guardedfragment.gumbogui.gumboguiMixIO;

import java.awt.BorderLayout;
import java.awt.ComponentOrientation;
import java.awt.FlowLayout;
import java.awt.TextField;

import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JEditorPane;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JTextArea;

import mapreduce.guardedfragment.gumbogui.gumboguiMixIO.ButtonWindowMixIO;
import mapreduce.guardedfragment.gumbogui.gumboguiMixIO.DirInWindowMixIO;
import mapreduce.guardedfragment.gumbogui.gumboguiMixIO.DirOutWindowMixIO;
import mapreduce.guardedfragment.gumbogui.gumboguiMixIO.TextWindowMixIO;
import mapreduce.guardedfragment.gumbogui.gumboguiMixIO.TextWindowMixIOBot;
import mapreduce.guardedfragment.gumbogui.gumboguiMixIO.TextWindowMixIOUp;

public class GumboMainWindowMixIO extends JPanel {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public GumboMainWindowMixIO(JEditorPane editorIQ, JTextArea textConsole, JButton buttonQC, 
			JButton buttonSche, JButton buttonFH, JButton buttonFS, JCheckBox cbLevel,
			TextField ip, TextField op, JButton browseIn, JButton browseOut) {
		
		super();
		
		
				
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
