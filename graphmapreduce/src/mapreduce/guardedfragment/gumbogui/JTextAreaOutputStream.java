package mapreduce.guardedfragment.gumbogui;

import java.io.IOException;

import javax.swing.JTextArea;
import javax.swing.SwingUtilities;

import com.twitter.chill.Base64.OutputStream;

public class JTextAreaOutputStream extends OutputStream
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