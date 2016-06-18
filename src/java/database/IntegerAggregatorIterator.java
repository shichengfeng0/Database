package simpledb;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;

import simpledb.Aggregator.Op;


public class IntegerAggregatorIterator implements DbIterator {
	boolean open;
	ArrayList<Field> groupValueList;
	ArrayList<Field> aggregateValueList;
	int curPointer;
	TupleDesc tupleDesc;
	boolean noGrouping;
	
	public IntegerAggregatorIterator(Op what, ArrayList<GbfieldAndAfield> groupMap, Type gbfieldtype) {
		curPointer = 0;
		noGrouping = (gbfieldtype == null);
		if (noGrouping) {// no grouping
			Type[] type = new Type[]{Type.INT_TYPE};
			tupleDesc = new TupleDesc(type);
		} else {
			Type[] type = new Type[]{gbfieldtype, Type.INT_TYPE};
			tupleDesc = new TupleDesc(type);
		}
		
		groupValueList = new ArrayList<Field>();
		aggregateValueList = new ArrayList<Field>();
		
		if (noGrouping) {
			
			ArrayList<Field> tmp = new ArrayList<Field>();
			
			for (GbfieldAndAfield gbfa: groupMap) {
				tmp.add(gbfa.getAggregateList().get(0));
			}
			
			if (what.equals(Op.MAX)) {
				int min = Integer.MIN_VALUE;
				for (Field f: tmp) {
					if (min > ((IntField) f).getValue()) {
						min = ((IntField) f).getValue();
					}
				}
				aggregateValueList.add(new IntField(min));
			} else if (what.equals(Op.MIN)) {
				int max = Integer.MAX_VALUE;
				for (Field f: tmp) {
					if (max < ((IntField) f).getValue()) {
						max = ((IntField) f).getValue();
					}
				}
				aggregateValueList.add(new IntField(max));
			} else if (what.equals(Op.COUNT)) {
				aggregateValueList.add(new IntField(tmp.size()));
			} else if (what.equals(Op.AVG)) {
				int sum = 0;
				for (Field f: tmp) {
					sum += ((IntField) f).getValue();
				}
				aggregateValueList.add(new IntField(sum / tmp.size()));
			} else if (what.equals(Op.SUM)) {
				int sum = 0;
				for (Field f: tmp) {
					sum += ((IntField) f).getValue();
				}
				aggregateValueList.add(new IntField(sum));
			} else {
				throw new IllegalArgumentException("no such Op");
			}
				
//			for (GbfieldAndAfield gbfa: groupMap) {
//				aggregateValueList.add(gbfa.getAggregateList().get(0));
//			}
		} else {
			
			for (GbfieldAndAfield gbfa: groupMap) {
				if (what.equals(Op.MAX)) {
					buildMax(gbfa);
				} else if (what.equals(Op.MIN)) {
					buildMin(gbfa);
				} else if (what.equals(Op.COUNT)) {
					buildCount(gbfa);
				} else if (what.equals(Op.AVG)) {
					buildAvg(gbfa);
				} else if (what.equals(Op.SUM)) {
					buildSum(gbfa);
				} else {
					throw new IllegalArgumentException("no such Op");
				}
			}
		}
	}
	
	/**
	 * build average aggregate values
	 * @param gbfa according to this group by filed and list of aggregate fields, 
	 * 		  we insert average value of aggregate fields to our aggregateValueList which will be used later
	 */
	private void buildAvg(GbfieldAndAfield gbfa) {
		int sum = 0;
		ArrayList<Field> list = gbfa.getAggregateList();
		for (Field f: list) {
			sum += ((IntField) f).getValue();
		}
		aggregateValueList.add(new IntField(sum / list.size()));
		groupValueList.add((gbfa.getGbField()));
	}
	
	
	/**
	 * build count aggregate values
	 * @param gbfa according to this group by filed and list of aggregate fields, 
	 * 		  we insert number of value of the aggregate fields to our aggregateValueList which will be used later
	 */
	private void buildCount(GbfieldAndAfield gbfa) {
		ArrayList<Field> list = gbfa.getAggregateList();
		aggregateValueList.add(new IntField(list.size()));
		groupValueList.add((gbfa.getGbField()));
	}
	
	/**
	 * build sum aggregate values
	 * @param gbfa according to this group by filed and list of aggregate fields, 
	 * 		  we insert sum value of the aggregate fields to our aggregateValueList which will be used later
	 */
	private void buildSum(GbfieldAndAfield gbfa) {
		int sum = 0;
		ArrayList<Field> list = gbfa.getAggregateList();
		for (Field f: list) {
			sum += ((IntField) f).getValue();
		}
		aggregateValueList.add(new IntField(sum));
		groupValueList.add((gbfa.getGbField()));
	}
	
	/**
	 * build min aggregate values
	 * @param gbfa ccording to this group by filed and list of aggregate fields, 
	 * 		  we insert min value from the aggregate fields to our aggregateValueList which will be used later
	 */
	private void buildMin(GbfieldAndAfield gbfa) {
		int min = Integer.MAX_VALUE;
		ArrayList<Field> list = gbfa.getAggregateList();
		for (Field f: list) {
			IntField tmp = (IntField) f;
			if (min > tmp.getValue()) {
				min = tmp.getValue();
			}
		}
		aggregateValueList.add(new IntField(min));
		groupValueList.add((gbfa.getGbField()));
	}
	
	/**
	 * build max aggregate values
	 * @param gbfa according to this group by filed and list of aggregate fields, 
	 * 		  we insert max value from the aggregate fields to our aggregateValueList which will be used later
	 */
	private void buildMax(GbfieldAndAfield gbfa) {
		int max = Integer.MIN_VALUE;
		ArrayList<Field> list = gbfa.getAggregateList();
		for (Field f: list) {
			IntField tmp = (IntField) f;
			if (max < tmp.getValue()) {
				max = tmp.getValue();
			}
		}
		aggregateValueList.add(new IntField(max));
		groupValueList.add((gbfa.getGbField()));
		
	}
	
	@Override
	public void open() throws DbException, TransactionAbortedException {
		open = true;		
	}

	@Override
	public boolean hasNext() throws DbException, TransactionAbortedException {
		if (!open) {
			throw new IllegalStateException("not open yet");
		}
		return curPointer < aggregateValueList.size();
	}

	@Override
	public Tuple next() throws DbException, TransactionAbortedException,
			NoSuchElementException {
		if (hasNext()) {
			if (noGrouping) {
				Tuple tuple = new Tuple(tupleDesc);
				tuple.setField(0, aggregateValueList.get(curPointer));
				curPointer++;
				return tuple;
			} else {
				// has grouping
				Tuple tuple = new Tuple(tupleDesc);
				tuple.setField(0, groupValueList.get(curPointer));
				tuple.setField(1, aggregateValueList.get(curPointer));
				curPointer++;
				return tuple;
			}
		}
		
		throw new NoSuchElementException("no more tuples");
		
	}

	@Override
	public void rewind() throws DbException, TransactionAbortedException {
		curPointer = 0;
	}

	@Override
	public TupleDesc getTupleDesc() {
		return tupleDesc;
	}

	@Override
	public void close() {
		open = false;
	}

}
