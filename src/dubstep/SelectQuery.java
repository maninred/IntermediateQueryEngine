package dubstep;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
//import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
//import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
//import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.*;

public class SelectQuery extends Main{
	public static PrimitiveValue value;
	public static LinkedHashMap<String, String> SchemaDetails;
	public static Function function;
	public static int pos = 0;
	public static boolean aggregateEvaluation = false;
	public static customEval evalobj;
	public static customEval expressionEvaluator = new customEval();
	public static customEval customeval = new customEval();
	public static boolean setfirstTimeAggregates;
	//static Iterator outerit;
	public static int columnID;
	public static boolean firstColumn = false;
	public static boolean finalColumn = false;
	public static Column currentColumn;
	public static PrimitiveValue[] tempFilteredData;
	public static CustomGreaterThan customGreaterThan;
	public static CustomDivision customDivision;
	public static CustomAddtion customAddtion;
	public static CustomMinorThan customMinorThan;
	//public static PrimitiveValue[] aggregateResultsList;
	SelectQuery() throws SQLException, IOException
	{	
		HandleExpressionandColumns(tableName, selectedColumns);
	}
	public static void HandleExpressionandColumns(String tableName ,List<SelectItem> selectItems) throws SQLException, IOException{
		SelectItem selectedItem;
		SelectExpressionItem expressionItemformatted;
		Expression finalformattedExpression;
		//ArrayList<PrimitiveValue> primtiveValuesRawData;
		//long rowCount = 0;
		//System.out.println("Length of Array " + aggregateResultsList.length);
		//getSchemaDetails = (LinkedHashMap<String, String>) tableDetails.get(tableName);
		if(!bufferFromExternalClass){
//			LinkedHashMap<String, String> getSchemaDetails;
//			List<ArrayList<PrimitiveValue>> tempWorkingData;
//			getSchemaDetails = (LinkedHashMap<String, String>) tableDetails.get(tableName);
			//System.out.println(tupleRecord.toString());
			if(orderByPrimaryKey == true) {
				//new OrderBy();
			}
			//workingTuple = tupleRecord.toArray(new PrimitiveValue[0]);
		}

	pos = 0;
	
	if(groupBy && orderPresent && presentSelectCount == 0){
		if(lastRecord){
			FileSplitandMerge.fileSplit();
			FileSplitandMerge.lastRecordsortandFlush();
		}
		else{
			FileSplitandMerge.fileSplit();
		}
	}
	
	else if(groupBy && presentSelectCount == 0) {
		//System.out.println("Group By");
		if(lastRecord){
			FileSplitandMerge.fileSplit();
			FileSplitandMerge.lastRecordsortandFlush();
		}
		else{
			FileSplitandMerge.fileSplit();
		}
		//GroupByAggregationTest.groupByAggregate(positionsToBeGrouped);
		
	}
	
	else if(orderPresent && presentSelectCount == 0){
		if(lastRecord) {
			FileSplitandMerge.fileSplit();
			FileSplitandMerge.lastRecordsortandFlush();
		}
		else{
			//System.out.println("Present Select count " + presentSelectCount);
			//System.out.println("Selected Columns List in select Query " + selectedColumns.toString());
			FileSplitandMerge.fileSplit();
		}
	}

		
	else if(!selectItems.get(0).toString().equals("*")){
		SelectExpressionItem expression = (SelectExpressionItem) selectItems.get(0);
		Expression finalexpression = (Expression) expression.getExpression();
		Iterator<SelectItem> selectColumnsIterator = selectItems.iterator();
		//Aggregate Queries
		columnID = 0;
		if(finalexpression instanceof Function){
			firstColumn = true;
			while(selectColumnsIterator.hasNext()){
				selectedItem = selectColumnsIterator.next();
				expressionItemformatted = (SelectExpressionItem) selectedItem;
				finalformattedExpression = (Expression) expressionItemformatted.getExpression();
				aggregateEvaluation = true;
				function = (Function) finalformattedExpression;
				if(!selectColumnsIterator.hasNext()) finalColumn = true;
				if(function.getParameters() != null)
					ExpressionHandler((Expression)function.getParameters().getExpressions().get(0));
				evaluateAggregateFunction(function.getName().toString());
				pos++;
				firstColumn = false;
				finalColumn = false;
			}

		}

		//Queries with Expression and Select statements
		else if(finalexpression instanceof Expression){
			firstColumn = true;
			while(selectColumnsIterator.hasNext()){
				selectedItem = selectColumnsIterator.next();
				//System.out.println("SelectedItem " + selectedItem.toString());
				expressionItemformatted = (SelectExpressionItem) selectedItem;
				//System.out.println("Trying out Expression Item " + expressionItemformatted);
				finalformattedExpression = (Expression) expressionItemformatted.getExpression();
				if(!selectColumnsIterator.hasNext()) {
					finalColumn = true;
				}

				ExpressionHandler(finalformattedExpression);
				if(presentSelectCount == 0) //if(End of the columns - Send a signal)
					if(selectColumnsIterator.hasNext()) 
						System.out.print("|");
					else 
						System.out.println();
				firstColumn = false;
				finalColumn = false;
			}
		}
	}
	else{
		//Process All the columns
		//workingRecord = workingRecord;
		if(presentSelectCount == 0) printWorkingTuple();
	}
}

//	public static void cleanupWorkingSchema(){
//		while(outerit.hasNext()){
//			outerit.next();
//			outerit.remove();
//		}
//	}

	public static void ExpressionHandler(Expression finalformattedExpression) throws SQLException, IOException{
		//readBuffer = line.split("\\|");
		value = expressionEvaluator.eval(finalformattedExpression);
		if(presentSelectCount != 0) buildFeedData(); 
		else if(aggregateEvaluation == false && presentSelectCount == 0) printPrimitiveValue();

	}

	public static void printWorkingTuple(){
		//Iterator<PrimitiveValue> printingRecord = workingRecord.iterator();
		for(int i=0; i < workingRecord.length; i++){
			if(i != workingRecord.length - 1)
				System.out.print(workingRecord[i].toRawString() + "|");
			else 
				System.out.print(workingRecord[i].toRawString());
		}
		System.out.println();
//		while(printingRecord.hasNext()){
//			if(printingRecord.hasNext()) System.out.print(printingRecord.next().toString() + "|");
//			else System.out.print(printingRecord.toString());
//		}
		
	}
	public static void buildFeedData(){
		//System.out.println("Here!!!!!!");
		if(firstColumn) tempFilteredData = new PrimitiveValue[selectedColumns.size()];
		//System.out.println("Before" + tempFilteredData);
		tempFilteredData[columnID] = value;
		columnID++;
		if(finalColumn) {
			//workingRecord.clear();
			workingRecord = tempFilteredData;
			//System.out.println("Working Data Set Tuple" + workingData.toString());
		}
	}

	public static void buildFeedSchema(List<SelectItem> selectItems){
		ArrayList<String> tempSchema = new ArrayList<String>();
		//SelectExpressionItem expression = (SelectExpressionItem) selectItems.get(0);
		//Expression finalexprbuildFeedSchemaession = (Expression) expression.getExpression();
		Iterator<SelectItem> selectColumnsIterator = selectItems.iterator();
		while(selectColumnsIterator.hasNext()){
			String s = selectColumnsIterator.next().toString();
			if(workingDataSchema.contains(s)){
				//System.out.println("Columns to Schema Feed: " + s);
				tempSchema.add(s);
			}
		}
		workingDataSchema.clear();
		workingDataSchema.addAll(tempSchema);
		//System.out.println("Working Schema " + workingDataSchema.toString());
		tempSchema.clear();	
	}
	public static PrimitiveValue getPrimitiveValue() throws InvalidPrimitive{
		return value;
	}

	public static void printPrimitiveValue() throws InvalidPrimitive{
		System.out.print(value.toString());
	}
	
	public static void populateColumns(){
		getSchemaDetails = (LinkedHashMap<String, String>) tableDetails.get(tableName);
		
	}
	/*
	public static void ColumnsHandler(SelectItem selectItem){
		//System.out.println("Inside columns Handler");
		getSchemaDetails = (LinkedHashMap<String, String>) tableDetails.get(tableName);
		columnPositions = new ArrayList<Integer>();
		int pos = 0;
			currentColumn = selectItem.toString();
			if(getSchemaDetails.containsKey(currentColumn)){
				pos = new ArrayList<String>(getSchemaDetails.keySet()).indexOf(currentColumn);
				columnPositions.add(pos);
				columnValidation = true;
			}
			else{
				System.out.println("The Column " + currentColumn + " does not exist! ");
				columnValidation = false;
				}
	}

	 */
	public static void executeSelectAll(){
		/*
		 * The BufferedReader Pointer can be set at other place.
		 * Example - Consider evaluating a where condition - 
		 * The pointer of the file read would be set by the SelectWhere Class.
		 */	
		String[] readBuffer = line.split("\\|");
		for(String data: readBuffer){
			if (data != readBuffer[0]){
				System.out.print("|");
			}
			System.out.print(data);				
		}
	}

	
	public static void evaluateAggregateFunction(String functionName) throws SQLException{
		if(aggregateResultsList == null){
			aggregateResultsList = new double[sel.get(0).getSelectItems().size()];
			setfirstTimeAggregates = true;
		}
		if(functionName.equalsIgnoreCase("COUNT")) executeCount();
		else if(functionName.equalsIgnoreCase("AVG")) {
			if(!lastRecord) executeSum();
			else {
				executeSum();
				executeAvg();
			}
		}
		else if(functionName.equalsIgnoreCase("MIN")) executeMin();
		else if(functionName.equalsIgnoreCase("MAX")) executeMax();
		else if(functionName.equalsIgnoreCase("SUM")) executeSum();
	}

	
	public static void executeMin() throws SQLException{
//		if(customMinorThan == null)
//			customMinorThan = new CustomMinorThan(value, aggregateResultsList[pos]);
//		else
//			customMinorThan.getResult(value , aggregateResultsList[pos]);
//		
//		if(aggregateResultsList[pos] == null)
//			aggregateResultsList[pos] = value;
//		else if(customeval.eval(customMinorThan).toBool())
//			aggregateResultsList[pos] = value;
		if(setfirstTimeAggregates)
			aggregateResultsList[pos] = value.toDouble();
		else
			aggregateResultsList[pos] = Math.min(value.toDouble(), aggregateResultsList[pos]);
	}
	
	public static void executeCount() throws SQLException{
		
		if(setfirstTimeAggregates)
			aggregateResultsList[pos] = 1;
		else
			aggregateResultsList[pos] += 1;
			
	}

	public static void executeMax() throws SQLException{
//		if(customGreaterThan == null)
//			customGreaterThan = new CustomGreaterThan(value, aggregateResultsList[pos]);
//		else
//			customGreaterThan.getResult(value , aggregateResultsList[pos]);
//		
//		
//		if(aggregateResultsList[pos] == null)
//			aggregateResultsList[pos] = value;
//		else if(customeval.eval(customGreaterThan).toBool())
//			aggregateResultsList[pos] = value;
		
		if(setfirstTimeAggregates)
			aggregateResultsList[pos] = value.toDouble();
		else
			aggregateResultsList[pos] = Math.max(value.toDouble(), aggregateResultsList[pos]);
	}

	public static void executeSum() throws SQLException{
//		if(customAddtion == null)
//			customAddtion = new CustomAddtion(value, aggregateResultsList[pos]);
//		else
//			customAddtion.getResult(value , aggregateResultsList[pos]);
//		
//		if(aggregateResultsList[pos] == null)
//			aggregateResultsList[pos] = value;
//		else
//			aggregateResultsList[pos] = customeval.eval(customAddtion);
		if(setfirstTimeAggregates)
			aggregateResultsList[pos] = value.toDouble();
		else
			aggregateResultsList[pos] += value.toDouble();
	}
	
	public static void executeAvg() throws SQLException{	
			aggregateResultsList[pos] =  aggregateResultsList[pos] / avgCount;
	}

	public static void aggregatePrint() throws SQLException{
		boolean printedAggregateResult = true;
		if(aggregateResultsList == null) printedAggregateResult = false;
		if(aggregateEvaluation && aggregateResultsList != null){
			for(int j=0;j<aggregateResultsList.length;j++){
				System.out.print(aggregateResultsList[j]);
				if(j!=aggregateResultsList.length-1)System.out.print("|");
				else System.out.println();
			}
		}
		if(aggregateEvaluation && (!printedAggregateResult)) {
			SelectExpressionItem expression = (SelectExpressionItem) selectedColumns.get(0);
			Expression finalexpression = (Expression) expression.getExpression();
			Iterator<SelectItem> selectColumnsIterator = selectedColumns.iterator();
			//Aggregate Queries
			if(finalexpression instanceof Function){
				firstColumn = true;
				SelectItem selectedItem;
				SelectExpressionItem expressionItemformatted;
				Expression finalformattedExpression;
				while(selectColumnsIterator.hasNext()){
					selectedItem = selectColumnsIterator.next();
					expressionItemformatted = (SelectExpressionItem) selectedItem;
					finalformattedExpression = (Expression) expressionItemformatted.getExpression();
					aggregateEvaluation = true;
					function = (Function) finalformattedExpression;
					if(function.getName().equals("COUNT")) System.out.print(0);
					else System.out.print("|");					
				}
			}
			System.out.println();
			//System.out.println();
		}
		aggregateEvaluation = false;
	}
}
