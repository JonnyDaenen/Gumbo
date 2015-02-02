/**
 * Created: 22 Jan 2015
 */
package mapreduce.guardedfragment.executor.hadoop.mappers;


/**
 * @author Jonny Daenen
 *
 */
public enum GumboMap1Counter {
	REQUEST,
	PROOF_OF_EXISTENCE,
	KEEP_ALIVE_REQUEST,
	KEEP_ALIVE_PROOF_OF_EXISTENCE,
	KEEP_ALIVE_REQUEST_R2,
	KEEP_ALIVE_PROOF_OF_EXISTENCE_R2,

	REQUEST_KEY_BYTES,
	REQUEST_VALUE_BYTES,
	REQUEST_BYTES,
	PROOF_OF_EXISTENCE_BYTES,
	KEEP_ALIVE_REQUEST_BYTES,
	KEEP_ALIVE_PROOF_OF_EXISTENCE_BYTES, 
	KEEP_ALIVE_REQUEST_R2_BYTES,
	KEEP_ALIVE_PROOF_OF_EXISTENCE_R2_BYTES
}
