package _2_query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import _1_basic.Field;
import _1_basic.IntField;
import _1_basic.StringField;
import _1_basic.Tuple;
import _1_basic.TupleDesc;
import _1_basic.Type;

/**
 * A class to perform various aggregations, by accepting one tuple at a time
 * @author Doug Shook
 *
 */
public class Aggregator {
	
	AggregateOperator operator;
	boolean groupBy;
	TupleDesc td;
	Type aggregatedType; //the type of the column to be aggregated
	ArrayList<Field> groupKeys; //keep the order that group keys occurs
	HashMap<Field, ArrayList<Field>> aggregation; //<group key, group values> 
	//*For each group key, tuples with this key will be merged together into this pair.
	//*For group values, it is an ArrayList of values from the merged tuples.
	HashMap<Field, Field> resultMap; //<group key, result of a group>
	//*stores the designated aggregation operation result
	
	public Aggregator(AggregateOperator o, boolean groupBy, TupleDesc td) {
		//your code here
		this.operator = o;
		this.groupBy = groupBy;
		this.td = td;
		this.aggregatedType = null;
		this.groupKeys = new ArrayList<Field>();
		this.aggregation = new HashMap<Field, ArrayList<Field>>();
		this.resultMap = new HashMap<Field, Field>();
	}

	/**
	 * Merges the given tuple into the current aggregation
	 * @param t the tuple to be aggregated
	 */
	public void merge(Tuple t) {
		//your code here
//		if(t.getDesc()!=this.td) {return;}
		Field groupKey; //the value in the group field of a tuple
		Field tupleValue; //the value in the aggregated field of a tuple
		if(groupBy) {
			groupKey = t.getField(0); //first column in the data point in this HW
			tupleValue = t.getField(1); //second column in the data point in this HW
			if(this.aggregatedType == null) {
				this.aggregatedType = this.td.getType(1);
			}
		} else {
			groupKey = new IntField(0); 
			//*If not groupBy, then we can assume all the tuples have same value in groupField
			tupleValue = t.getField(0);
			if(this.aggregatedType == null) {
				this.aggregatedType = this.td.getType(0);
			}
		}
		if(!aggregation.containsKey(groupKey)) { //new groupKey occurs, create a new group
			groupKeys.add(groupKey);
			aggregation.put(groupKey, new ArrayList<Field>());
		}
		aggregation.get(groupKey).add(tupleValue); //add to the aggregation 		
	}
	
	/**
	 * Returns the result of the aggregation
	 * @return a list containing the tuples after aggregation
	 */
	public ArrayList<Tuple> getResults() {
		//your code here
		Iterator<Field> groupIter = aggregation.keySet().iterator();
		while(groupIter.hasNext()) { //for each group
			Boolean started = false;
			Field group = groupIter.next();
			ArrayList<Field> values = aggregation.get(group); //values in a group
			if(this.aggregatedType.equals(Type.INT)) { //INT case
				int result = 0;
				for(Field valueRaw:values) { //for each value in a group
					int value = ((IntField)valueRaw).getValue();
					if(!started) {
						if(this.operator.equals(AggregateOperator.MAX) || this.operator.equals(AggregateOperator.MIN)) {
							result = value;
						} else {
							result = 0;
						}
						started = true;
					}
					if(this.operator.equals(AggregateOperator.MAX)) {
						result = value > result ? value : result;
					}
					if(this.operator.equals(AggregateOperator.MIN)) {
						result = value < result ? value : result;
					}
					if(this.operator.equals(AggregateOperator.AVG)) {
						result += value;
					}
					if(this.operator.equals(AggregateOperator.COUNT)) {
						result ++;
					}
					if(this.operator.equals(AggregateOperator.SUM)) {
						result += value;
					}
				}// end of for each value in a group
				if(operator == AggregateOperator.AVG) {
					result /= values.size();
				}
				resultMap.put(group, new IntField(result));
				
			} //end of INT case
			else if(this.aggregatedType.equals(Type.STRING)) { //String case
				String result = "";
				int count = 0;
				for(Field valueRaw:values) { //for each value in a group
					String value = ((StringField)valueRaw).getValue();
					if(!started) { //initialize the value
						if(this.operator.equals(AggregateOperator.MAX) || this.operator.equals(AggregateOperator.MIN)) {
							result = value;
						}
						else {
							value = ((StringField)valueRaw).getValue();
						}
						started = true;
					}
					if(this.operator.equals(AggregateOperator.MAX)) {
//						System.out.println("max, result = "+result+", value = "+value+", >0 = "+(value.compareTo(result) > 0));
						result = value.compareTo(result) > 0 ? value : result;
					}
					if(this.operator.equals(AggregateOperator.MIN)) {
						result = value.compareTo(result) < 0 ? value : result;
					}
					if(this.operator.equals(AggregateOperator.COUNT)) {
						count ++;
					}
//					System.out.println(result);
				}// end of for each value in a group
				if(this.operator.equals(AggregateOperator.COUNT)) {
					resultMap.put(group, new IntField(count));
				}
				else {
					resultMap.put(group, new StringField(result));
				}
				
				
			} //end of String case
		} 
		
		ArrayList<Tuple> resultTuples = new ArrayList<>();
		groupIter = aggregation.keySet().iterator();
		for(int i = 0; i < this.groupKeys.size(); i++){
			Tuple resultTuple = new Tuple(td);
			Field groupKey = this.groupKeys.get(i);
//			System.out.println("groupKey = "+groupKey.toString());
			if(groupBy) {
				resultTuple.setField(0, groupKey);
				resultTuple.setField(1, resultMap.get(groupKey));
//				System.out.println("true, "+resultMap.get(groupKey).toString());
			}else {
				resultTuple.setField(0, resultMap.get(groupKey));
//				System.out.println("false, "+resultMap.get(groupKey).toString());
			}
			resultTuples.add(resultTuple);
		}
		
		
		return resultTuples;
	}

}
