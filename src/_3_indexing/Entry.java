package _3_indexing;

import _1_basic.Field;

public class Entry {

	private Field f;
	private int page;
	
	public Entry(Field f, int page) {
		this.f = f;
		this.page = page;
	}
	
	public Field getField() {
		return this.f;
	}
	
	public int getPage() {
		return this.page;
	}
	
}
