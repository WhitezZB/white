package org.star.whie.common.bean;

/********
 * 
 * @author kaynewu
 *
 * 2019年4月18日
 * @param <T>
 */
@SuppressWarnings({"rawtypes","unchecked"})
public class KeyInx implements Comparable<KeyInx>{

	private Comparable value;
	
	private int idx;
	
	public KeyInx() {
		
	}
	
	public KeyInx(Comparable value, int idx) {
		this.value = value;
		this.idx = idx;
	}

	public Comparable getValue() {
		return value;
	}

	public void setValue(Comparable value) {
		this.value = value;
	}

	public int getIdx() {
		return idx;
	}

	public void setIdx(int idx) {
		this.idx = idx;
	}
	
	@Override
		public boolean equals(Object obj) {
			// TODO Auto-generated method stub
			if(obj instanceof KeyInx) {
				return this.value.equals(((KeyInx) obj).getValue()) ;
			}		
			return false;
		}
	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return this.hashCode();
	}

	
	@Override
	public int compareTo(KeyInx o) {
		return this.value.compareTo(o.getValue());
	}
	
	public static void main(String[] args) {
		KeyInx a = new KeyInx(10, 9);
		KeyInx b = new KeyInx(6, 8);
		KeyInx c = new KeyInx(11, 7);
		
		System.out.println(a.compareTo(c));
	}
}
