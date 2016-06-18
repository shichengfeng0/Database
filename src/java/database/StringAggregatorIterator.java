package simpledb;

import java.util.ArrayList;
import java.util.NoSuchElementException;

import simpledb.Aggregator.Op;

public class StringAggregatorIterator implements DbIterator {
	boolean open;
	ArrayList<Field> groupValueList;
	ArrayList<Field> aggregateValueList;
	int curPointer;
	boolean noGrouping;
	TupleDesc tupleDesc;
	
	public StringAggregatorIterator(Op what, ArrayList<GbfieldAndAfield> groupMap, Type gbfieldtype) {
		curPointer = 0;
		this.noGrouping = (gbfieldtype == null);
		if (noGrouping) {
			Type[] type = new Type[]{Type.STRING_TYPE};
			tupleDesc = new TupleDesc(type);
		} else {
			Type[] type = new Type[]{gbfieldtype, Type.INT_TYPE};
			tupleDesc = new TupleDesc(type);
		}
		
		groupValueList = new ArrayList<Field>();
		aggregateValueList = new ArrayList<Field>();
		
		if (noGrouping) {
			for (GbfieldAndAfield gbfa: groupMap) {
				aggregateValueList.add(gbfa.getAggregateList().get(0));
			}
		} else {
			for (GbfieldAndAfield gbfa: groupMap) {
				ArrayList<Field> list = gbfa.getAggregateList();
				aggregateValueList.add(new IntField(list.size()));
				groupValueList.add((gbfa.getGbField()));
			}
		}
		
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
