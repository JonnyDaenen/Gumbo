
public class ByteTest {
	
	public static void main(String[] args) {
		
		
		byte b1 = (byte) 0xFF;
		byte b2 = (byte) 0x7F;
		
		System.out.println(b1);
		System.out.println(b2);
		System.out.println(b1 & 0xFF);
		System.out.println(b2 & 0xFF);
		
		int i1 = 0;
		int i2 = 0;
		
		for (int i = 0; i < 4; i++) {
			i1 = (i1 << 8) | b1;
			i2 = (i2 << 8) | b2;
			System.out.println(i1 + " : " + i2 + ": " + (i1 > i2));
		}
		
		i1 = i2 = 0;
		for (int i = 0; i < 4; i++) {
			i1 = (i1 << 8) | (b1 & 0xFF);
			i2 = (i2 << 8) | (b2 & 0xFF);
			System.out.println(i1 + " : " + i2 + ": " + (i1 > i2));
		}
		
		long l1 = 0;
		long l2 = 0;
		
		for (int i = 0; i < 4; i++) {
			l1 = (l1 << 8) | b1;
			l2 = (l2 << 8) | b2;
			System.out.println(l1 + " : " + l2 + ": " + (l1 > l2));
		}
		
		l1 = l2 = 0;
		for (int i = 0; i < 4; i++) {
			l1 = (l1 << 8) | (b1 & 0xFF);
			l2 = (l2 << 8) | (b2 & 0xFF);
			System.out.println(l1 + " : " + l2 + ": " + (l1 > l2));
		}
		
		//
		
		l1 = 0;
		l2 = 0xFFFFFFFFL;
		int result1 = (int) (l1-l2);
		int result2 = (int) (l2-l1);
		
		System.out.println(l1 + " " + l2 + " " + result1  + " " + result2);
		
	}

}
