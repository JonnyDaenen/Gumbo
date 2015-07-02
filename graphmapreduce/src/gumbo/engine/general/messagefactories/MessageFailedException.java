package gumbo.engine.general.messagefactories;

public class MessageFailedException extends Exception {

	public MessageFailedException(Exception e) {
		super(e);
	}
}
