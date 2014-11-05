package mapreduce.guardedfragment;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.GridLayout;
import java.awt.TextField;
import java.io.IOException;

import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JEditorPane;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTextArea;
import javax.swing.SwingUtilities;
import javax.swing.border.TitledBorder;



public class GumboMainWindow0 extends JFrame {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public GumboMainWindow0(JEditorPane editorIQ, JTextArea textConsole, JButton buttonQC, 
			JButton buttonSche, JButton buttonFH, JButton buttonFS, JCheckBox cbLevel,
			TextField ip, TextField op) {
		
		super("GUMBO");
		
		
				
		TextWindowMix textWin = new TextWindowMix(new TextWindowMixUp(editorIQ), new TextWindowMixBot(textConsole));	
				
		ButtonWindowMix buttonWin = new ButtonWindowMix(buttonQC,buttonSche,buttonFH,buttonFS,cbLevel);
		
		setLayout(new FlowLayout());
		add(textWin);
		add(new DirInWindowMix(ip));
		add(new DirOutWindowMix(op));
		add(buttonWin);
        setVisible(true);
      
	}
	
}


class DirInWindowMix extends JPanel {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public DirInWindowMix(TextField ip){
		JLabel lab1 = new JLabel("   Input directory:");
		
		add(lab1);
		add(ip);
	}
	
}


class DirOutWindowMix extends JPanel {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public DirOutWindowMix(TextField op){
		JLabel lab2 = new JLabel("Output directory:");
		
		add(lab2);
		add(op);
	}
	
}



class TextWindowMix extends JSplitPane {
	

	private static final long serialVersionUID = 1L;

	public TextWindowMix(TextWindowMixUp fw, TextWindowMixBot sw) {
		
		super(JSplitPane.VERTICAL_SPLIT,fw,sw);		
		

		setOneTouchExpandable(false);
		setResizeWeight(0.7);
		setBorder(null);
		
	}
}


class TextWindowMixUp extends JPanel {
	

	private static final long serialVersionUID = 1L;

	public TextWindowMixUp(JEditorPane eIQ) {
		super();
		setLayout(new GridLayout(1,1,10,5));
		
		JScrollPane editorIQScroll = new JScrollPane(eIQ);
		editorIQScroll.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
		editorIQScroll.setPreferredSize(new Dimension(1100, 260));
		//editorIQScroll.setPreferredSize(new Dimension(1240, 210));
		editorIQScroll.setMinimumSize(new Dimension(10, 10));
		
		
		editorIQScroll.setBorder(BorderFactory.createTitledBorder(null, "INPUT QUERIES", 
				TitledBorder.CENTER, TitledBorder.TOP, new Font("courier new",1,14),Color.blue));
		
		add(editorIQScroll);
		
	}
}

class TextWindowMixBot extends JPanel {
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;


	public TextWindowMixBot(JTextArea tC) {
		super();
		setLayout(new GridLayout(1,1,10,5));	
				
		JScrollPane textConsoleScroll = new JScrollPane(tC);
		textConsoleScroll.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
		textConsoleScroll.setPreferredSize(new Dimension(1100, 260));
		textConsoleScroll.setMinimumSize(new Dimension(10, 10));
		textConsoleScroll.setBorder(BorderFactory.createTitledBorder(null, "CONSOLE", 
				TitledBorder.CENTER, TitledBorder.TOP, new Font("Courier new",1,14),Color.blue));
		
		add(textConsoleScroll);
		
        setVisible(true);			
	}
}




class ButtonWindowMix extends JPanel {
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public ButtonWindowMix(JButton bQC, JButton bS, JButton bFH, JButton bFS, JCheckBox cbL) {
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


