/**
 * Created on: 17 Mar 2015
 */
package gumbo.gui.gumbogui;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Image;

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
	ScrollablePicture pic;
	
	public PlanViewer() {
		super(new BorderLayout());
		image = new ImageIcon("output/query.png");
		pic = new ScrollablePicture(image,10);
		scrollPane = new JScrollPane(pic);
		add( scrollPane , BorderLayout.CENTER );
	}

	public void reloadImage() {
		
		// flush the file, as it is buffered
		image.getImage().flush();
		Image newImage = getToolkit().createImage("output/query.png");
		image.setImage(newImage);
		pic.revalidate();
//		pic.repaint();
		
//		scrollPane.setViewportView(new ScrollablePicture(image,10));
//		scrollPane.setViewportView(pic);
//		scrollPane.revalidate();
//		scrollPane.repaint();
		
//		revalidate();
//		repaint();
	}
	
	
}
