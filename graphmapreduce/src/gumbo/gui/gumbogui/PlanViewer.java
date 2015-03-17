/**
 * Created on: 17 Mar 2015
 */
package gumbo.gui.gumbogui;

import java.awt.BorderLayout;
import java.awt.Component;

import javax.swing.ImageIcon;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;

/**
 * @author Jonny Daenen
 *
 */
public class PlanViewer extends JPanel {
	
	private static final long serialVersionUID = 1;

	JScrollPane scrollPane;
	ImageIcon image;
	
	public PlanViewer() {
		super(new BorderLayout());
		image = new ImageIcon("output/query.png");
		scrollPane = new JScrollPane(new ScrollablePicture(image,10));
		add( scrollPane , BorderLayout.CENTER );
	}

	public void reloadImage() {
		
		// flush the file, as it is buffered
		image.getImage().flush();
//		scrollPane.setViewportView(new ScrollablePicture(image,10));
		repaint();
	}
	
	
}
