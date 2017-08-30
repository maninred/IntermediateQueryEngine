package dubstep;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;


public class GroupandOrderIndexes extends Main{

	public static String firstColumnName;
	public static int firsttColumnNum;
	public static boolean firstColumnAsc;
	public static int numberOfOrderBy;
	public static ListIterator<Long> descendingOrderIterator;
	public static ListIterator<Long> ascendingOrderIterator;
	public static CustomEquals customEquals;

	private static String[] colNames;
	private static Boolean[] colBool;
	private static ArrayList<Integer> colPos;
	private static int num;
	private static customEval indexEval = new customEval();
	private static CustomMinorThan customMinorcomp;
	private static CustomGreaterThan customMajorcomp;
	private static ArrayList<PrimitiveValue[]> bufferPV; 
	private static PrimitiveValue previous;


	private static SelectItem selectItem;
	private static SelectExpressionItem expressionItemformatted;
	private static Expression finalformattedExpression;
	private static ArrayList<PrimitiveValue> currentTupleinMerge;
	private static Map<Integer,String> aggregateResultMap;
	private static int groupbycount;
	private static ArrayList<PrimitiveValue> TempAL;
	private static int countRecords;
	
	private static boolean limitExists;
	private static int limitRows;

	private static Function function;
	//private static Map<Integer,String> posToFuntionMap = new HashMap<Integer,String>();

	private static boolean lastRow;

	public static customEval expressionEvaluator;

	public static PrimitiveValue value;
	
	public static Expression aggregatedWhereClause;
	
	//private static PrimitiveValue[] currentRecordInMem;

	public static void gatherData() throws SQLException{
		firstColumnName=orderByToBeWorkedOn.get(0).getExpression().toString();
		firstColumnAsc=orderByToBeWorkedOn.get(0).isAsc();
		firsttColumnNum=workingDataSchema.indexOf(firstColumnName);
		numberOfOrderBy=orderByToBeWorkedOn.size();
		//System.out.println("Number of Order by elements " + numberOfOrderBy);
		currentTupleinMerge = new ArrayList<PrimitiveValue>();
		aggregateResultMap	= new HashMap<Integer,String>();
		bufferPV = new ArrayList<PrimitiveValue[]>();
		ArrayList<Long> firstOrderByIndex = new ArrayList<>(indexesToBeUsed.get(firstColumnName));
		customMinorcomp = new CustomMinorThan(new LongValue(1), new LongValue(1));
		customMajorcomp=new CustomGreaterThan(new LongValue(1), new LongValue(1));

		selectedColumns=schemaList.get(0);

		assigningColumnAttributes();

		if(!firstColumnAsc)
			descendingOrderIterator = firstOrderByIndex.listIterator(firstOrderByIndex.size());
		else
			ascendingOrderIterator = firstOrderByIndex.listIterator();

		if(groupBy) {
			//Execute first Order by and then Group by
			if(!firstColumnAsc) orderByHelperDescending();
			else orderByHelperAscending();
		}
		else {
			//Execute Order By
			if(!firstColumnAsc) orderByHelperDescending();
			else orderByHelperAscending();
		}
	}

	@SuppressWarnings("unchecked")
	public static void assigningColumnAttributes() throws SQLException{
		colNames=new String[numberOfOrderBy];
		colBool=new Boolean[numberOfOrderBy];
		if(colPos!= null) colPos.clear();
		else colPos = new ArrayList<Integer>();
		int i=0;
		for(OrderByElement OE: orderByToBeWorkedOn){
			colNames[i]=OE.getExpression().toString();
			colBool[i]=OE.isAsc();
			colPos.add(Main.workingDataSchema.indexOf(colNames[i]));
			i++;
		}

		schemaDetailsforGroupbyandOrdering = new LinkedHashMap<String, String>();
		schemaDetailsforGroupbyandOrdering=(LinkedHashMap<String, String>) tableDetails.get(tableName);
		previous=null;
		lastRow=false;
		num=0;
		
		aggregatedWhereClause=null;
		List<Expression> whereExpression=new ArrayList<Expression>();
		Expression eachWhere;
		for(PlainSelect eachplainselect: sel){
			eachWhere=eachplainselect.getWhere();
			if(eachWhere!=null) whereExpression.add(eachWhere);
		}
		for(Expression eachWhereExpression: whereExpression){
			if(aggregatedWhereClause==null) aggregatedWhereClause=eachWhereExpression;
			else{
				aggregatedWhereClause=new AndExpression(aggregatedWhereClause,eachWhereExpression);
			}
		}
		countRecords = 0;
		PlainSelect outermostSelect = sel.get(0);
		Limit l = outermostSelect.getLimit();
		limitExists = false;
		if(l != null) {
			limitExists = true;
			limitRows = (int) l.getRowCount();
		}
	}
	
	
	public static boolean whereClauseEvaluationInMem() throws InvalidPrimitive, SQLException{
		return indexEval.eval(aggregatedWhereClause).toBool();
	}

	public static void orderByHelperAscending() throws SQLException{
		while(ascendingOrderIterator.hasNext()){
			if(limitExists && limitRows == countRecords) break; //Limit Implementation
			long currentIndexValue = ascendingOrderIterator.next();
			if(!ascendingOrderIterator.hasNext()) lastRow=true;
			workingRecord = overallData.get(currentIndexValue);
			for(int i=0; i < workingRecord.length; i++){
				workingRecordMapper.put(workingDataSchema.get(i), workingRecord[i]);
			}
			if(aggregatedWhereClause !=null && !whereClauseEvaluationInMem()){
				if(lastRow && numberOfOrderBy>1) bufferingMethod(-1);
				if(lastRow && numberOfOrderBy==1){
					if(groupBy)printForGroupAggregate();
				}
				continue;
			}
			if(numberOfOrderBy>1){
				bufferingMethod(currentIndexValue);
			}
			else{
				if(!groupBy)
					processTuplesForOrdering(workingRecord);
				else
					processTuplesForGrouping(workingRecord);
			}
		}
	}

	public static void orderByHelperDescending() throws SQLException{
		while(descendingOrderIterator.hasPrevious()){
			if(limitExists && limitRows == countRecords) break;
			long currentIndexValue=descendingOrderIterator.previous();
			if(!descendingOrderIterator.hasPrevious()) lastRow=true;
			workingRecord = overallData.get(currentIndexValue);
			for(int i=0; i < workingRecord.length; i++){
				workingRecordMapper.put(workingDataSchema.get(i), workingRecord[i]);
			}
			//workingRecord=overallData.get(currentIndexValue);
			if(aggregatedWhereClause !=null && !whereClauseEvaluationInMem()){
				if(lastRow && numberOfOrderBy>1) bufferingMethod(-1);
				if(lastRow && numberOfOrderBy==1){
					if(groupBy)printForGroupAggregate();
				}
				continue;
			}
			if(numberOfOrderBy>1){
				bufferingMethod(currentIndexValue);
			}
			else{
				if(!groupBy)
					processTuplesForOrdering(workingRecord);
				else
					processTuplesForGrouping(workingRecord);
			}
		}

	}

	public static void bufferingMethod(long currentIndexValue) throws SQLException{
		if(currentIndexValue==-1){
			if(!groupBy){
				for(PrimitiveValue[] bufferedPrimitiveValue: bufferPV)
					processTuplesForOrdering(bufferedPrimitiveValue);
				bufferPV.clear();
			}
			else{
				for(PrimitiveValue[] bufferedPrimitiveValue: bufferPV)
					processTuplesForGrouping(bufferedPrimitiveValue);
				bufferPV.clear();
			}
			return;
		}
		PrimitiveValue[] eachPV=overallData.get(currentIndexValue);
		if(previous==null) previous =eachPV[firsttColumnNum];

		if(indexEval.eval(new EqualsTo(previous,eachPV[firsttColumnNum])).toBool()){
			bufferPV.add(eachPV);
		}
		else{
			if(bufferPV.size() > 1)
				Collections.sort(bufferPV, new tieBreakComparator());
			if(!groupBy){
				for(PrimitiveValue[] bufferedPrimitiveValue: bufferPV)
					processTuplesForOrdering(bufferedPrimitiveValue);
			}
			else{
				for(PrimitiveValue[] bufferedPrimitiveValue: bufferPV)
					processTuplesForGrouping(bufferedPrimitiveValue);
			}
			bufferPV.clear();
			previous=eachPV[firsttColumnNum];
			bufferPV.add(eachPV);
		}

		if(lastRow && bufferPV.size()>0){
			if(bufferPV.size() > 1)
				Collections.sort(bufferPV, new tieBreakComparator());
			if(!groupBy){
				for(PrimitiveValue[] bufferedPrimitiveValue: bufferPV)
					processTuplesForOrdering(bufferedPrimitiveValue);
				bufferPV.clear();
			}
			else{
				for(PrimitiveValue[] bufferedPrimitiveValue: bufferPV)
					processTuplesForGrouping(bufferedPrimitiveValue);
				bufferPV.clear();
			}

		}

	}

	public static void processTuplesForOrdering(PrimitiveValue[] valueofTuple) throws SQLException{
		if(limitExists && limitRows == countRecords) return;
		//Contains All the columns
		if(selectedColumns.get(0).toString().equals("*")){
			for(PrimitiveValue primValue : valueofTuple){
				System.out.print(primValue + "|");	
			}
			System.out.println();
			countRecords++;
		}
		else{
			Iterator<SelectItem> selectColumnsIterator = selectedColumns.iterator();
			while(selectColumnsIterator.hasNext()){
				selectItem = selectColumnsIterator.next();
				expressionItemformatted = (SelectExpressionItem) selectItem;
				finalformattedExpression = (Expression) expressionItemformatted.getExpression();
				customEval.setWorkingTuple(valueofTuple);
				ExpressionHandler(finalformattedExpression);
				if(selectColumnsIterator.hasNext()) 
					System.out.print("|");
				else
					System.out.println();
					//System.out.println();
			}
			countRecords++;
		}
	}


	public static void processTuplesForGrouping(PrimitiveValue[] valueofTuple) throws SQLException{
		//Contains All the columns
		//if(!selectedColumns.get(0).toString().equals("*")){
		Iterator<SelectItem> selectColumnsIterator = selectedColumns.iterator();
		int it=0;
		for(int i:colPos){
			currentTupleinMerge.add(valueofTuple[i]);
		}

		while(selectColumnsIterator.hasNext()){
			selectItem = selectColumnsIterator.next();
			expressionItemformatted = (SelectExpressionItem) selectItem;
			finalformattedExpression = (Expression) expressionItemformatted.getExpression();
			customEval.setWorkingTuple(valueofTuple);
			if(finalformattedExpression instanceof Function){
				function = (Function) finalformattedExpression;
				if(!aggregateResultMap.containsKey(it)) aggregateResultMap.put(it,function.getName().toLowerCase());
				if(function.getName().toLowerCase().equals("count")) currentTupleinMerge.add(new LongValue(1));
				else currentTupleinMerge.add(getExpressionFromHandler((Expression)function.getParameters().getExpressions().get(0)));
			}
			else{
				currentTupleinMerge.add(getExpressionFromHandler(finalformattedExpression));
			}
			//System.out.println("Expression Sent " + finalformattedExpression.toString());
			//System.out.println("Value of the Tuple " + valueofTuple);
			if(!selectColumnsIterator.hasNext()){ 
				twoPVcomparison();
			}
			it++;
		}
	}


	public static void ExpressionHandler(Expression expression) throws SQLException{
		if(expressionEvaluator == null)
			expressionEvaluator = new customEval();
		value = expressionEvaluator.eval(expression);
		//System.out.println("Value After Eval " + value.toString()); 
		printPrimitiveValue();

	}


	public static void printPrimitiveValue(){
		System.out.print(value.toRawString());
	}


	public static PrimitiveValue getExpressionFromHandler(Expression exp) throws SQLException{
		if(expressionEvaluator == null) {
			expressionEvaluator = new customEval();
		}
		value = expressionEvaluator.eval(exp);
		//System.out.println("Value After Eval " + value.toString()); 
		return value;

	}

	public static void twoPVcomparison() throws InvalidPrimitive, SQLException{
		//First Group by Occurs Here
		if(TempAL == null || TempAL.isEmpty()){
			TempAL = new ArrayList<PrimitiveValue>();
			TempAL.addAll(currentTupleinMerge);
			currentTupleinMerge.clear();
			//updateTheOld = true;
			groupbycount = 1;
		}
		else{
			boolean updateTheOld=true;
			//System.out.println(colPos.toString());
			int j=0;
			//Compares the columns to be grouped by - Preload the column positions on which group by has to be performed
			for(int i : colPos){
				if(!TempAL.get(j).toRawString().equals(currentTupleinMerge.get(j).toRawString())){
					//System.out.println("TempAL.get(i) "+TempAL.get(i));
					//System.out.println("currentTupleinMerge.get(i) "+currentTupleinMerge.get(i));
					updateTheOld=false;
					break;
				}
				j++;
			}
			if(!updateTheOld){
				printForGroupAggregate();
				groupbycount = 1;
				TempAL.addAll(currentTupleinMerge);
				currentTupleinMerge.clear();
			}
			else{
				//The Overall Group count increases
				groupbycount++;
				int colSize = colPos.size();
				for(int i : aggregateResultMap.keySet()){
					PrimitiveValue tempALvalue = TempAL.get(i+colSize);
					PrimitiveValue incomingValue = currentTupleinMerge.get(i+colSize);
					switch(aggregateResultMap.get(i).toLowerCase()){
					case "count": {
						TempAL.set(i+colSize, new LongValue(groupbycount)); break;
					}
					case "min": {
						customMinorcomp.getResult(incomingValue, tempALvalue);
						if(indexEval.eval(customMajorcomp).toBool()) 
							TempAL.set(i+colSize,incomingValue); 
						break;
					}
					case "max": {
						customMajorcomp.getResult(incomingValue, tempALvalue);
						if(indexEval.eval(customMajorcomp).toBool())
							TempAL.set(i+colSize,incomingValue);
						break;
					}
					case "sum": {
						TempAL.set(i+colSize,indexEval.eval(new Addition(incomingValue,tempALvalue))); break;
					}
					case "avg": {
						TempAL.set(i+colSize,indexEval.eval(new Addition(incomingValue,tempALvalue))); break;
					}
					}

				}
				currentTupleinMerge.clear();
			}
		}
		if(lastRow){
			printForGroupAggregate();
			TempAL.clear();
			aggregateResultMap.clear();
		}
		
	}

	public static void printForGroupAggregate() throws SQLException{
		if(limitExists && limitRows == countRecords) return;
		//System.out.println("Calling output");
		//System.out.println("Temp AL " + TempAL.toString());
		PrimitiveValue pv;
		for(int i=colPos.size();i<TempAL.size();i++){
			pv = TempAL.get(i);
			if(aggregateResultMap.containsKey(i-colPos.size()) && aggregateResultMap.get(i-colPos.size()).equals("avg")) System.out.print(indexEval.eval(new CustomDivision(pv,new LongValue(groupbycount))).toRawString());
			else {
				System.out.print(pv.toRawString());
			}
			if(i == TempAL.size()-1) 
			{	
				countRecords++;
				System.out.println();
			}
			else System.out.print("|");
		}
		TempAL.clear();
//		if(lastRow) aggregateResultMap.clear();
	}			

	static class tieBreakComparator implements Comparator<PrimitiveValue[]>{

		@Override
		public int compare(PrimitiveValue[] s1_parsed, PrimitiveValue[] s2_parsed) {
			int traversal=1;
			while(traversal<numberOfOrderBy){
				try {
					String posOfThePV = new ArrayList<String>(Main.schemaDetailsforGroupbyandOrdering.keySet()).get(colPos.get(traversal)).toString();
					String dataOfThePV = Main.schemaDetailsforGroupbyandOrdering.get(posOfThePV);
					//String dataOfThePV=dataTypes[colPos.get(traversal)].toString().toLowerCase();
					if(dataOfThePV.equals("string") || dataOfThePV.equals("char") || dataOfThePV.equals("varchar")){
						if(!colBool[traversal])  num=s2_parsed[colPos.get(traversal)].toString().compareTo(s1_parsed[colPos.get(traversal)].toString());
						else  num=s1_parsed[colPos.get(traversal)].toString().compareTo(s2_parsed[colPos.get(traversal)].toString());

						if(num==0){
							traversal++;
							continue;
						}
						else
							return num;
					}
					else if(dataOfThePV.equals("date")){
						if(indexEval.eval(customMajorcomp.getResult(s1_parsed[colPos.get(traversal)],s2_parsed[colPos.get(traversal)])).toBool()){
							if(!colBool[traversal]) return -1;
							return 1;
						}
						else if(indexEval.eval(customMinorcomp.getResult(s1_parsed[colPos.get(traversal)],s2_parsed[colPos.get(traversal)])).toBool()){
							if(!colBool[traversal]) return 1;
							return -1;
						}
						else 
							traversal++;
					}
					else{
						//if(indexEval.eval(customMajorcomp.getResult(s1_parsed[colPos.get(traversal)],s2_parsed[colPos.get(traversal)])).toBool()){
						if(s1_parsed[colPos.get(traversal)].toDouble() > s2_parsed[colPos.get(traversal)].toDouble()){    
							if(!colBool[traversal]) return -1;
							return 1;
						}
						else if(s1_parsed[colPos.get(traversal)].toDouble() < s2_parsed[colPos.get(traversal)].toDouble()){
							if(!colBool[traversal]) return 1;
							return -1;
						}
						else
							traversal++;
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			return 0;
		}

	}



}
