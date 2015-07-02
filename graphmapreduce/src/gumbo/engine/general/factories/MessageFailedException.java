package gumbo.engine.general.factories;

public class MessageFailedException extends Exception {

	public MessageFailedException(Exception e) {
		super(e);
	}
}
