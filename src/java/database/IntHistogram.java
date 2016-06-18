package simpledb;

import java.util.ArrayList;
import java.util.Iterator;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
	ArrayList<Integer>[] histogramArray;
	int min;
	int max;
	int totalNum; //used to keep track of the total number of elements in the IntHistorgram
	
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    @SuppressWarnings("unchecked")
	public IntHistogram(int buckets, int min, int max) {
    	this.min = min;
    	this.max = max;
    	totalNum = 0;
    	histogramArray = new ArrayList[buckets];
    	
    	//initialize the array
    	for (int i = 0; i < buckets; i++) {
    		histogramArray[i] = new ArrayList<Integer>();
    	}
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	totalNum++;
    	histogramArray[findIndex(v)].add(v);
    }
    
    /**
     * find the bucket index where the value v should be put into
     * @param v the value to be put into the bucket
     * @return the bucket index where the value v should be put into
     */
    private int findIndex(int v) {
    	if (v < min) {
    		return 0;
    	}
    	if (v >= max) {
    		return histogramArray.length - 1;
    	}
    	
    	double width = (max - min) * 1.0 / histogramArray.length;
    	int index = (int) ((v - min) / width);
    	
    	return index;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	int bucketIndex = findIndex(v);
    	ArrayList<Integer> array = histogramArray[bucketIndex];
		Iterator<Integer> itr = array.iterator();
		
    	if (op.toString().equals("=")) {
    	    int num = 0;
    		while (itr.hasNext()) {
    			if (itr.next() == v) {
    				num++;
    			}
    		}
    		return num * 1.0 / totalNum;
    	} else if (op.toString().equals(">")) {
    		int num = 0;
    		while (itr.hasNext()) {
    			if (itr.next() > v) {
    				num++;
    			}
    		}
    		for (int i = bucketIndex + 1; i < histogramArray.length; i++) {
    			num += histogramArray[i].size();
    		}
    		
    		return num * 1.0 / totalNum;
    	} else if (op.toString().equals(">=")) {
    		int num = 0;
    		while (itr.hasNext()) {
    			if (itr.next() >= v) {
    				num++;
    			}
    		}
    		
    		for (int i = bucketIndex + 1; i < histogramArray.length; i++) {
    			num += histogramArray[i].size();
    		}
    		
    		return num * 1.0 / totalNum;
    	} else if (op.toString().equals("<")) {
    		int num = 0;
    		while (itr.hasNext()) {
    			if (itr.next() < v) {
    				num++;
    			}
    		}
    		for (int i = 0; i < bucketIndex; i++) {
    			num += histogramArray[i].size();
    		}
    		return num * 1.0 / totalNum;
    		
    	} else if (op.toString().equals("<=")) {
    		int num = 0;
    		while (itr.hasNext()) {
    			if (itr.next() <= v) {
    				num++;
    			}
    		}
    		for (int i = 0; i < bucketIndex; i++) {
    			num += histogramArray[i].size();
    		}
    		return num * 1.0 / totalNum;
    	} else if (op.toString().equals("<>")) {
    		int num = 0;
    		for (int i = 0; i < histogramArray.length; i++) {
    			ArrayList<Integer> al = histogramArray[i];
    			Iterator<Integer> itr2 = al.iterator();
    			while (itr2.hasNext()) {
    				if (itr2.next() != v) {
    					num++;
    				}
    			}
    		}
    		return num * 1.0 / totalNum;
    	}
    	
    	return -1.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
    	String result = "";
        for (int i = 0; i < histogramArray.length; i++) {
        	result += "bucket " + i + ": ";
        	for (int j = 0; j < histogramArray[i].size(); j++) {
        		result += histogramArray[i].get(j) + " ";
        	}
        	result += "\n";
        }
        return result;
    }
}
