package dubstep;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class GroupByAggregationTest extends Main{
	Object obj = null;
	static StringBuilder tempHashString = new StringBuilder();
	static boolean aggregatePrinted = false;
	static Expression savedExpression;
	static int columnPosition = 0;
	static PrimitiveValue primitiveValueFetch;
	static customEval customeval = new customEval();
	
	public static void groupByAggregate(int[] columns) throws SQLException, IOException {
		/*
		//PrimitiveValue[] primitiveColumns = new PrimitiveValue[columns.length];
		for(int i=0; i < columns.length; i++){
			//System.out.println("Position----> " + i + "Values in the Tuple " + Main.workingRecord.toString());
			primitiveValueFetch = Main.workingRecord.get(columns[i]);
			String s = Main.workingRecord.get(columns[i]).toString();
			if(i == columns.length -1 ) {
				tempHashString.append(s);
				//primitiveColumns[i++] = primitiveValueFetch;
			}
			else if(columns.length > 1){
				tempHashString.append(s).append("|");
				//primitiveColumns[i++] = primitiveValueFetch;
			}
		}
		//groupedByKeysforOrdering.add(primitiveColumns);
		groupbyTuples();
		tempHashString.setLength(0); //Clearing the String
		aggregatePrinted = false;
		
		/*if(lastRecord) {
			Collections.sort(groupedByKeysforOrdering,new MyComparator());
		}*/
	
	}

	
	public static void groupbyTuples(){
		//System.out.println("Hash Values Passed " + tempHashString.toString());
		//System.out.println("Record Number " + recordNumber);
		if(groupByMasterData.containsKey(tempHashString.toString())){
			ArrayList<Integer> currentRecord = groupByMasterData.get(tempHashString.toString());
			currentRecord.add(recordNumber);
			groupByMasterData.put(tempHashString.toString(), currentRecord);
		}
		else{
			ArrayList<Integer> currentRecord = new ArrayList<Integer>();
			currentRecord.add(recordNumber);
			groupByMasterData.put(tempHashString.toString(), currentRecord);
		}
	}
	
	public static void temporaryPrintTuples(){
		Iterator printIterator = groupByMasterData.entrySet().iterator();
		while(printIterator.hasNext()){
			Map.Entry pair = (Map.Entry) printIterator.next();
			//System.out.println("Hash Map Keys" + pair.getKey() + "HashMap Values " + pair.getValue().toString());
		}
	}
	
	public static void groupedTupledPrinter() throws IOException{
		//System.out.println("Here it comes!");
		Iterator tempItertaor = groupByMasterData.entrySet().iterator();
		while(tempItertaor.hasNext()){
			@SuppressWarnings("rawtypes")
			Map.Entry pair = (Map.Entry) tempItertaor.next();
			ArrayList<Integer> tuplePrinter = (ArrayList<Integer>) pair.getValue();
			Iterator<Integer> printer = tuplePrinter.iterator();
			String currentLine;
			while(printer.hasNext()){
				Integer recordNumber = printer.next();
				try (Stream<String> lines = Files.lines(Paths.get("data/"+tableName+".csv"))) {
					currentRecord = lines.skip(recordNumber-1).findFirst().get();
					Main.tupleFetcher();
				}
				//Iterator columnIterator = tupleRecord.iterator();
				/*
				while(columnIterator.hasNext()){
					System.out.print(columnIterator.next().toString() + "|");
				}
				*/
				System.out.println();
			}
		}
		groupByMasterData.clear();
	}
	
	public static void evaluateforAggreation(List<SelectItem> selectItems) throws SQLException, IOException{	
		//System.out.println("Func agg invoked");
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
				//System.out.println("One Entry");
				Iterator masterGropedDataIterator = groupByMasterData.entrySet().iterator();
				while(masterGropedDataIterator.hasNext()){
					//System.out.println("Iteration Identifier");
					ArrayList<ArrayList<PrimitiveValue>> tuplesGrouped = new ArrayList<ArrayList<PrimitiveValue>>();
					Map.Entry pair = (Map.Entry) masterGropedDataIterator.next();
					//System.out.println("Keys " + pair.getKey().toString());
					ArrayList<Integer> tuplestobeEvalauted = (ArrayList<Integer>) pair.getValue();
					Iterator<Integer> tupleIterate = tuplestobeEvalauted.iterator();
					//System.out.println("Tuples to be Evalauted " + tuplestobeEvalauted.toString());
					while(tupleIterate.hasNext()){
						Integer recordNumber = tupleIterate.next();
						try (Stream<String> lines = Files.lines(Paths.get("data/"+tableName+".csv"))) {
							currentRecord = lines.skip(recordNumber-1).findFirst().get();
							Main.tupleFetcher();
							//System.out.println("Current Record " + tupleRecord.toString());
							//tuplesGrouped.add(tupleRecord);
						}
					}
					//System.out.println("Tuples Sent for grouping " + tuplesGrouped);
					results = aggregateFunctionCalculation.aggregateFunctionHelper(tuplesGrouped,expressionsonGroupBy,functionsofGroupBy);
					//System.out.println("Return from aggregateFunction");
					for(PrimitiveValue result: results){
						System.out.print(result.toString() + "|");
					}
					tuplesGrouped.clear();
					//End of All Aggregation
					System.out.println();
					//masterGropedDataIterator.next();
				}
			}
		//System.out.println("Should get out from this place");
	}
}

