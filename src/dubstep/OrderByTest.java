package dubstep;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;


//import dubstep.IndexBuilder.MyComparator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class OrderByTest extends Main{
	public static customEval customeval = new customEval();
	List<Integer> col;
	List<Boolean> bol;
	List<Integer> recordNumbersOrdered;
	

	OrderByTest(){
		col=new ArrayList<Integer>();
		bol=new ArrayList<Boolean>();
		recordNumbersOrdered=new ArrayList<Integer>();
		orderByFlyIndexes=new HashMap<PrimitiveValue,ArrayList<Integer>>();
		orderByOtherColumns=new ArrayList<HashMap<Integer,PrimitiveValue>>();
		orderByFlyIndexes.clear();
		List<OrderByElement> orderByColList = orderByToBeWorkedOn;
		int t=0;
		for(OrderByElement OE: orderByColList){
			String ColName=OE.getExpression().toString();
			if(ColName.contains(".")) {
				String[] groupSplit = ColName.split("\\.");
				ColName = groupSplit[groupSplit.length - 1];
			}
			int columnIndex = Main.workingDataSchema.indexOf(ColName);
			col.add(columnIndex);
			bol.add(OE.isAsc());
			if(t!=0){
				orderByOtherColumns.add(new HashMap<Integer,PrimitiveValue>());
			}
			t++;
		} 	
	}


	public void eachTime() throws IOException, SQLException{
		/*
		for(int i=0;i<col.size();i++){
			PrimitiveValue pv = Main.workingRecord.get(col.get(i));
			if(i==0){
				ArrayList<Integer> tempAL;
				if(orderByFlyIndexes.containsKey(pv)) tempAL=orderByFlyIndexes.get(pv);
				else tempAL=new ArrayList<Integer>();
				tempAL.add(recordNumber);
				orderByFlyIndexes.put(pv, tempAL);
			}
			else{
				orderByOtherColumns.get(i-1).put(recordNumber, pv);
			}
		}
		/*
		if(lastRecord) {
			actualOrdering();	
			orderByPrint();
		}*/
	}
	
	public void actualOrdering(){
		//System.out.println("ACTUAL ORDERING");
		ArrayList<PrimitiveValue> sortedKeys=new ArrayList(orderByFlyIndexes.keySet());
		Collections.sort(sortedKeys, new MyComparator());
		if(!bol.get(0)) Collections.reverse(sortedKeys);
		for(PrimitiveValue eachPV: sortedKeys){
			ArrayList<Integer> eachList=orderByFlyIndexes.get(eachPV);
			if(col.size()==1 || eachList.size()==1) recordNumbersOrdered.addAll(eachList);
			else recordNumbersOrdered.addAll(secondarySorting(eachList,1));
		}
		//System.out.println(recordNumbersOrdered.toString());
	}
	
	public ArrayList<Integer> secondarySorting(ArrayList<Integer> received,int level){
		
		HashMap<PrimitiveValue,ArrayList<Integer>> secondary=new  HashMap<PrimitiveValue,ArrayList<Integer>>();
		ArrayList<Integer> toBeReturned=new ArrayList<>();
		
		for(int temp:received){
			PrimitiveValue pv = orderByOtherColumns.get(level-1).get(temp);
			ArrayList<Integer> tempAL;
			if(!secondary.containsKey(pv)) tempAL=new ArrayList<Integer>();
			else tempAL=secondary.get(pv);
			tempAL.add(temp);
			secondary.put(pv, tempAL);
		}
		
		ArrayList<PrimitiveValue> sortedKeys=new ArrayList(secondary.keySet());
		Collections.sort(sortedKeys, new MyComparator());
		
		if(!bol.get(level)){
			Collections.reverse(sortedKeys);
		}
		
		for( PrimitiveValue pv:sortedKeys){
			ArrayList<Integer> eachList=secondary.get(pv);
			if(col.size()-level==1 || eachList.size()==1) toBeReturned.addAll(eachList);
			else toBeReturned.addAll(secondarySorting(eachList,level+1));	
		}
		
		
		return toBeReturned;
		//MinorThan minorThan = new MinorThan();
		//super();
	}
	
	static class MyComparator implements Comparator{
		
		@Override
		public int compare(Object obj1, Object obj2){
			try {
				//System.out.println("Minor than to string "+new MinorThan((PrimitiveValue)obj1,(PrimitiveValue)obj2));
				//System.out.println("Greater than to string "+new GreaterThan((PrimitiveValue)obj1,(PrimitiveValue)obj2).toString());
				if(customeval.eval(new MinorThan((PrimitiveValue)obj1,(PrimitiveValue)obj2)).toBool()){
					return -1;
				}
				else if(customeval.eval(new GreaterThan((PrimitiveValue)obj1,(PrimitiveValue)obj2)).toBool()){
					return 1;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}			
			return 0;
		}

	}
	
	
	public void orderByPrint() throws IOException, SQLException{
		long temp=0;
		Iterator<Integer> sortedRecordNumbers = recordNumbersOrdered.iterator();
		List<SelectItem> selectedItems = Main.selectedColumns;
		PrimitiveValue currentVal;
		while(sortedRecordNumbers.hasNext()){
			temp++;
			recordNumber = sortedRecordNumbers.next();
			try (Stream<String> lines = Files.lines(Paths.get("data/"+tableName+".csv"))) {
				currentRecord = lines.skip(recordNumber-1).findFirst().get();
				Main.tupleFetcher();
				Iterator<SelectItem> selectColumnsIterator = selectedItems.iterator();
				while(selectColumnsIterator.hasNext()){
					SelectItem selectedItem = selectColumnsIterator.next();
					SelectExpressionItem expressionItemformatted = (SelectExpressionItem) selectedItem;
					Expression finalformattedExpression = (Expression) expressionItemformatted.getExpression();
					//customeval.setWorkingTuple(tupleRecord);
					currentVal = customeval.eval(finalformattedExpression);
					if(selectColumnsIterator.hasNext()) System.out.print(currentVal.toString() + "|");
					else System.out.println(currentVal.toString());
					//System.out.println(currentRecord);
				}
				//Main.tupleFetcher();
			}
			//System.out.println();
			if(limitBreakData.containsKey(presentSelectCount)){
				if(limit==temp) break;
			}
		}	
	}
		
}
