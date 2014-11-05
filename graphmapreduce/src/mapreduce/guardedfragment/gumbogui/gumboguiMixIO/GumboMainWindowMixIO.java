package mapreduce.guardedfragment.gumbogui.gumboguiMixIO;

import java.awt.FlowLayout;
import java.awt.TextField;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JEditorPane;
import javax.swing.JFrame;
import javax.swing.JTextArea;

import mapreduce.guardedfragment.gumbogui.gumboguiMixIO.ButtonWindowMixIO;
import mapreduce.guardedfragment.gumbogui.gumboguiMixIO.DirInWindowMixIO;
import mapreduce.guardedfragment.gumbogui.gumboguiMixIO.DirOutWindowMixIO;
import mapreduce.guardedfragment.gumbogui.gumboguiMixIO.TextWindowMixIO;
import mapreduce.guardedfragment.gumbogui.gumboguiMixIO.TextWindowMixIOBot;
import mapreduce.guardedfragment.gumbogui.gumboguiMixIO.TextWindowMixIOUp;

public class GumboMainWindowMixIO extends JFrame {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public GumboMainWindowMixIO(JEditorPane editorIQ, JTextArea textConsole, JButton buttonQC, 
			JButton buttonSche, JButton buttonFH, JButton buttonFS, JCheckBox cbLevel,
			TextField ip, TextField op, JButton browseIn, JButton browseOut) {
		
		super("GUMBO");
		
		
				
		TextWindowMixIO textWin = new TextWindowMixIO(new TextWindowMixIOUp(editorIQ), new TextWindowMixIOBot(textConsole));	
				
		ButtonWindowMixIO buttonWin = new ButtonWindowMixIO(buttonQC,buttonSche,buttonFH,buttonFS,cbLevel);
		
		setLayout(new FlowLayout());
		pack();
		add(textWin);
		add(new DirInWindowMixIO(ip,browseIn));
		add(new DirOutWindowMixIO(op,browseOut));
		add(buttonWin);
        setVisible(true);
        setResizable(false);
      
	}

}
