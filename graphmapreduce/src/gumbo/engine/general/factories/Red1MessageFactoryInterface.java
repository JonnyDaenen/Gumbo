package gumbo.engine.general.factories;

import java.util.Set;

public interface Red1MessageFactoryInterface {

	public void loadValue(String address, String reply);

	public void sendReplies() throws MessageFailedException;

	public void cleanup() throws MessageFailedException;

	public void addAbort(long incr);

	public void addBuffered(long incr);

	public void setKeys(Set<Integer> keysFound);

}