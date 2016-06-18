package simpledb;

import java.util.ArrayList;

public class GbfieldAndAfield {
	public Field f;
	public ArrayList<Field> list;
	public GbfieldAndAfield(Field f, ArrayList<Field> list) {
		this.f = f;
		this.list = list;
	}
	
	public Field getGbField() {
		return f;
	}
	
	public ArrayList<Field> getAggregateList() {
		return list;
	}
	
	public Type getGfieldType() {
		return f.getType();
	}
	
}