package mapreduce.guardedfragment;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.GridLayout;
import java.io.IOException;

import javax.swing.BorderFactory;
import javax.swing.JCheckBox;
import javax.swing.JEditorPane;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JButton;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTextArea;
import javax.swing.SwingUtilities;
import javax.swing.border.TitledBorder;

import com.twitter.chill.Base64.OutputStream;




public class GumboMainWindow1 extends JFrame {
		

	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;


	public GumboMainWindow1(JEditorPane editorIQ, JEditorPane editorI, JEditorPane editorO, JTextArea textConsole, JButton buttonQC, 
			JButton buttonSche, JButton buttonFH, JButton buttonFS, JCheckBox cbLevel) {
		super("GUMBO");
		
		
				
		TextWindow textWin = new TextWindow(new WindowOne(editorIQ),new WindowTwo(editorI, editorO,textConsole));	
				
		WindowThree buttonWin = new WindowThree(buttonQC,buttonSche,buttonFH,buttonFS,cbLevel);
		
		setLayout(new FlowLayout());
		add(textWin);
		add(buttonWin);
        setVisible(true);
      
	}
	
	
	public GumboMainWindow1(JEditorPane editorIQ, JEditorPane editorI, JEditorPane editorO, JTextAreaOutputStream textConsole, JButton buttonQC, 
			JButton buttonSche, JButton buttonFH, JButton buttonFS, JCheckBox cbLevel) {
		super("GUMBO");
				
		TextWindow textWin = new TextWindow(new WindowOne(editorIQ),new WindowTwo(editorI, editorO,textConsole));	
				
		WindowThree buttonWin = new WindowThree(buttonQC,buttonSche,buttonFH,buttonFS,cbLevel);
		
	
		
		setLayout(new FlowLayout());
		add(textWin);
		add(buttonWin);
        setVisible(true);
      
	}
	
}


class TextWindow extends JSplitPane {
	
	//private WindowOne fw;
	//private WindowTwo sw;
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public TextWindow(WindowOne fw, WindowTwo sw) {
		super(JSplitPane.VERTICAL_SPLIT,fw,sw);

		setOneTouchExpandable(false);
		setResizeWeight(0.7);
		setBorder(null);
		
	}
}


class WindowOne extends JPanel {
	
	//private JEditorPane editorIQ;
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public WindowOne(JEditorPane eIQ) {
		super();
		setLayout(new GridLayout(1,1,10,5));		
		

		JScrollPane editorIQScroll = new JScrollPane(eIQ);
		editorIQScroll.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
		editorIQScroll.setPreferredSize(new Dimension(1000, 200));
		//editorIQScroll.setPreferredSize(new Dimension(1240, 210));
		editorIQScroll.setMinimumSize(new Dimension(10, 10));
		
		
		editorIQScroll.setBorder(BorderFactory.createTitledBorder(null, "INPUT QUERIES", 
				TitledBorder.CENTER, TitledBorder.TOP, new Font("courier new",1,14),Color.blue));
		
		add(editorIQScroll);
		
	}
}

class WindowTwo extends JPanel {
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;


	public WindowTwo(JEditorPane eI, JEditorPane eO, JTextArea tC) {
		super();
		setLayout(new GridLayout(1,2,6,3));	
		
		JScrollPane editorInScroll = new JScrollPane(eI);
		editorInScroll.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
		editorInScroll.setPreferredSize(new Dimension(300, 190));
		//editorInScroll.setPreferredSize(new Dimension(300, 190));
		editorInScroll.setMinimumSize(new Dimension(10, 10));
		editorInScroll.setBorder(BorderFactory.createTitledBorder(null, "INPUT FILES", 
				TitledBorder.CENTER, TitledBorder.TOP, new Font("Courier new",1,14),Color.blue));
		
		JScrollPane editorOutScroll = new JScrollPane(eO);
		editorOutScroll.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
		editorOutScroll.setPreferredSize(new Dimension(300, 190));
		editorOutScroll.setMinimumSize(new Dimension(10, 10));
		editorOutScroll.setBorder(BorderFactory.createTitledBorder(null, "OUTPUT FILES", 
				TitledBorder.CENTER, TitledBorder.TOP, new Font("Courier new",1,14),Color.blue));
		
		WindowTwoPrime wtp = new WindowTwoPrime(editorInScroll,editorOutScroll);
		
		add(wtp);
					
/*
		JScrollPane editorIOScroll = new JScrollPane(eIO);
		editorIOScroll.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
		editorIOScroll.setPreferredSize(new Dimension(620, 250));
		editorIOScroll.setMinimumSize(new Dimension(10, 10));
		editorIOScroll.setBorder(BorderFactory.createTitledBorder(null, "INPUT & OUTPUT FILES", 
				TitledBorder.CENTER, TitledBorder.TOP, new Font("Courier new",1,14),Color.blue));
		
		add(editorIOScroll);
		*/
				
		JScrollPane textConsoleScroll = new JScrollPane(tC);
		textConsoleScroll.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
		textConsoleScroll.setPreferredSize(new Dimension(620, 400));
		textConsoleScroll.setMinimumSize(new Dimension(10, 10));
		textConsoleScroll.setBorder(BorderFactory.createTitledBorder(null, "CONSOLE", 
				TitledBorder.CENTER, TitledBorder.TOP, new Font("Courier new",1,14),Color.blue));
		
		add(textConsoleScroll);
		
        setVisible(true);			
	}
	
	
	public WindowTwo(JEditorPane eI, JEditorPane eO, JTextAreaOutputStream tC) {
		super();
		setLayout(new GridLayout(1,2,6,3));	
		
		JScrollPane editorInScroll = new JScrollPane(eI);
		editorInScroll.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
		editorInScroll.setPreferredSize(new Dimension(300, 190));
		editorInScroll.setMinimumSize(new Dimension(10, 10));
		editorInScroll.setBorder(BorderFactory.createTitledBorder(null, "INPUT FILES", 
				TitledBorder.CENTER, TitledBorder.TOP, new Font("Courier new",1,14),Color.blue));
		
		JScrollPane editorOutScroll = new JScrollPane(eO);
		editorOutScroll.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
		editorOutScroll.setPreferredSize(new Dimension(300, 190));
		editorOutScroll.setMinimumSize(new Dimension(10, 10));
		editorOutScroll.setBorder(BorderFactory.createTitledBorder(null, "OUTPUT FILES", 
				TitledBorder.CENTER, TitledBorder.TOP, new Font("Courier new",1,14),Color.blue));
		
		WindowTwoPrime wtp = new WindowTwoPrime(editorInScroll,editorOutScroll);
		
		add(wtp);
					
/*
		JScrollPane editorIOScroll = new JScrollPane(eIO);
		editorIOScroll.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
		editorIOScroll.setPreferredSize(new Dimension(620, 250));
		editorIOScroll.setMinimumSize(new Dimension(10, 10));
		editorIOScroll.setBorder(BorderFactory.createTitledBorder(null, "INPUT & OUTPUT FILES", 
				TitledBorder.CENTER, TitledBorder.TOP, new Font("Courier new",1,14),Color.blue));
		
		add(editorIOScroll);
		*/
				
		JScrollPane textConsoleScroll = new JScrollPane(tC.giveDest());
		textConsoleScroll.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
		textConsoleScroll.setPreferredSize(new Dimension(300, 400));
		textConsoleScroll.setMinimumSize(new Dimension(10, 10));
		textConsoleScroll.setBorder(BorderFactory.createTitledBorder(null, "CONSOLE", 
				TitledBorder.CENTER, TitledBorder.TOP, new Font("Courier new",1,14),Color.blue));
		
		add(textConsoleScroll);
		
        setVisible(true);			
	}
	
}


class WindowTwoPrime extends JSplitPane {
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public WindowTwoPrime(JScrollPane eI, JScrollPane eO) {
			
		super(JSplitPane.VERTICAL_SPLIT,eI,eO);

		setOneTouchExpandable(false);
		setResizeWeight(0.5);
		setBorder(null);
		

	}
}



class WindowThree extends JPanel {
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public WindowThree(JButton bQC, JButton bS, JButton bFH, JButton bFS, JCheckBox cbL) {
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


class JTextAreaOutputStream extends OutputStream
{
    private JTextArea destination;

    public JTextAreaOutputStream (JTextArea destination)
    {   super(null);
        if (destination == null)
            throw new IllegalArgumentException ("Destination is null");

        this.destination = destination;
    }
    
    public JTextArea giveDest() {
    	return destination;
    }

    @Override
    public void write(byte[] buffer, int offset, int length) throws IOException
    {
        final String text = new String (buffer, offset, length);
        SwingUtilities.invokeLater(new Runnable ()
            {
                @Override
                public void run() 
                {
                    destination.append (text);
                }
            });
    }

    @Override
    public void write(int b) throws IOException
    {
        write (new byte [] {(byte)b}, 0, 1);
    }

 
}

