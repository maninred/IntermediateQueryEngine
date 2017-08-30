package dubstep;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class GroupByAggregation extends Main{
	Object obj = null;
	static StringBuilder tempHashString = new StringBuilder();
	static boolean aggregatePrinted = false;
	static Expression savedExpression;
	public static void groupByAggregate( int[] columns) throws SQLException, IOException {	
		/*
		for(int pos: columns){
			//System.out.println("Position----> " + pos + "Values in the Tuple " + Main.workingRecord.toString());
			String s = Main.workingRecord.get(pos).toString();
			tempHashString.append(s);
		}
		//groupbyTuples();
		tempHashString.setLength(0); //Clearing the String
		aggregatePrinted = false;
		if(lastRecord) {
			evaluateforAggreation(Main.selectedColumns);
			if(!aggregatePrinted) groupedTupledPrinter();
		}*/
		
	}
	
	/*public static void groupbyTuples(){
		if(groupByMasterData.containsKey(tempHashString.toString())){
			List<ArrayList<PrimitiveValue>> currentRecords = groupByMasterData.get(tempHashString.toString());
			currentRecords.add(Main.workingRecord);
			groupByMasterData.put(tempHashString.toString(), currentRecords);
		}
		else{
			List<ArrayList<PrimitiveValue>> currentRecord = new ArrayList<ArrayList<PrimitiveValue>>();
			currentRecord.add(Main.workingRecord);
			groupByMasterData.put(tempHashString.toString(), currentRecord);
		}
	}*/
	
	public static void groupedTupledPrinter(){
		//System.out.println("Here it comes!");
		Iterator tempItertaor = groupByMasterData.entrySet().iterator();
		while(tempItertaor.hasNext()){
			Map.Entry pair = (Map.Entry) tempItertaor.next();
			List<ArrayList<PrimitiveValue>> tuplePrinter = (List<ArrayList<PrimitiveValue>>) pair.getValue();
			Iterator printer = tuplePrinter.iterator();
			while(printer.hasNext()){
				List<PrimitiveValue> tuples = (List<PrimitiveValue>)printer.next();
				Iterator columnIterator = tuples.iterator();
				while(columnIterator.hasNext()){
					System.out.print(columnIterator.next().toString() + "|");
				}
				System.out.println();
			}
		}
		
		groupByMasterData.clear();
	}
	
	public static void evaluateforAggreation(List<SelectItem> selectItems) throws SQLException, IOException{
		
		
		SelectExpressionItem expression = (SelectExpressionItem) selectItems.get(0);
		PrimitiveValue[] results = new PrimitiveValue[selectItems.size()];
		//Expression finalexpression = (Expression) expression.getExpression();
		Iterator<SelectItem> selectColumnsIterator = selectItems.iterator();
		ArrayList<Expression> expressionsonGroupBy = new ArrayList<Expression>(selectItems.size());
		ArrayList<String> functionsofGroupBy = new ArrayList<String>(selectItems.size());
		
			while(selectColumnsIterator.hasNext()){
				SelectItem selectedItem = selectColumnsIterator.next();
				SelectExpressionItem expressionItemformatted = (SelectExpressionItem) selectedItem;
				Expression finalformattedExpression = (Expression) expressionItemformatted.getExpression();
				if(finalformattedExpression instanceof Function){
					Function function = (Function) finalformattedExpression;
					//System.out.println("Function  " + function.getName().toString());
					if(function.getParameters() != null){
						savedExpression = (Expression)function.getParameters().getExpressions().get(0);
					}
					else{
						savedExpression = finalformattedExpression;
					}
					//System.out.println("Expression " + savedExpression);
					functionsofGroupBy.add(function.getName().toString());
					expressionsonGroupBy.add(savedExpression);
				}
				else if(finalformattedExpression instanceof Expression){
					functionsofGroupBy.add("NULL");
					expressionsonGroupBy.add(finalformattedExpression);
				}
				else break;
			}
			
			boolean functionEvaluation = true;
			Iterator functionsEvaluator = functionsofGroupBy.iterator();
			while(functionsEvaluator.hasNext()){
				String function = functionsEvaluator.next().toString();
				if(!function.equals("NULL")) {
					functionEvaluation = true;
					break;
				}
				else {
					functionEvaluation = false; 
				}
			}
			
			if(functionEvaluation == true){
				aggregatePrinted = true;
				Iterator masterGropedDataIterator = groupByMasterData.entrySet().iterator();
				while(masterGropedDataIterator.hasNext()){
					Map.Entry pair = (Map.Entry) masterGropedDataIterator.next();
					ArrayList<ArrayList<PrimitiveValue>> tupletobeEvalauted = (ArrayList<ArrayList<PrimitiveValue>>) pair.getValue();
					results = aggregateFunctionCalculation.aggregateFunctionHelper(tupletobeEvalauted,expressionsonGroupBy,functionsofGroupBy);
					for(PrimitiveValue result: results){
						System.out.print(result.toString() + "|");
					}
					//End of All Aggregation
					System.out.println();
				}
			}
		//System.out.println("Should get out from this place");
	}
}
