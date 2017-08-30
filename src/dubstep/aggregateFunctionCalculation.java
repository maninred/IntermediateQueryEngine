package dubstep;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;

public class aggregateFunctionCalculation {
		static PrimitiveValue[] aggregateResultsList;
		static int primitiveValueResultIndex = 0;
		static PrimitiveValue currentVal = null;
		static customEval customeval;
		static boolean lastRecordintheGroup = false;
		static Iterator<ArrayList<PrimitiveValue>> listofValues;
		static ArrayList<ArrayList<PrimitiveValue>> tempCopyOfList;
		static ArrayList<PrimitiveValue> currentTupleinGroupBy;
		static String function;
		public static CustomGreaterThan customGreaterThan;
		public static CustomDivision customDivision;
		public static CustomAddtion customAddtion;
		public static CustomMinorThan customMinorThan;
		
	public static PrimitiveValue[] aggregateFunctionHelper(ArrayList<ArrayList<PrimitiveValue>> listtobeGrouped, ArrayList<Expression> Expressions, ArrayList<String> functions) throws SQLException, IOException{
		
		lastRecordintheGroup = false;
		customeval = new customEval();
		tempCopyOfList = listtobeGrouped;
		listofValues = listtobeGrouped.iterator(); 
		aggregateResultsList = new PrimitiveValue[Main.selectedColumns.size()];
		//Assuming that the Index will be looped over in order

		while(listofValues.hasNext()){
			currentTupleinGroupBy = listofValues.next();
			if(!listofValues.hasNext()) lastRecordintheGroup = true;//Sends a signal that it's processing the last record
			//System.out.println("Current Tuple " + currentTupleinGroupBy.toString());
//			int index = 0;
			Iterator<String> functionIterator = functions.iterator();
			Iterator<Expression> expressionIterator = Expressions.iterator();
			while(functionIterator.hasNext()){
				function = functionIterator.next();
				//System.out.println("Function Called " + function);
				if(!function.equalsIgnoreCase("COUNT")){
					ExpressionHandler(expressionIterator.next());
					evaluateAggregateFunction(function);
					primitiveValueResultIndex++;
				}
				else{
					expressionIterator.next(); //Pass Over the Count function onto next function
					evaluateAggregateFunction(function);
					primitiveValueResultIndex++;
				}
					
			}
			primitiveValueResultIndex = 0; //Reset for next Row
			
		}
		return aggregateResultsList;
	}
	

	public static PrimitiveValue[] hybridAggregationEvalaution(ArrayList<ArrayList<PrimitiveValue>> listtobeGrouped, ArrayList<Expression> Expressions, ArrayList<String> functions) throws SQLException, IOException{
		aggregateResultsList = new PrimitiveValue[Main.selectedColumns.size()];
		lastRecordintheGroup = false;
		customeval = new customEval();
		tempCopyOfList = listtobeGrouped;
		listofValues = listtobeGrouped.iterator(); 
		
		
		aggregateResultsList = new PrimitiveValue[Main.selectedColumns.size()];
		while(listofValues.hasNext()){
			currentTupleinGroupBy = listofValues.next();
			if(!listofValues.hasNext()) lastRecordintheGroup = true;//Sends a signal that it's processing the last record
			//System.out.println("Current Tuple " + currentTupleinGroupBy.toString());
//			int index = 0;
			Iterator<String> functionIterator = functions.iterator();
			Iterator<Expression> expressionIterator = Expressions.iterator();
			while(functionIterator.hasNext()){
				function = functionIterator.next();
				if(function.equalsIgnoreCase("NULL")){
					//Just call Eval Lib with the Expression
					ExpressionHandler(expressionIterator.next());
				}
				else if(!function.equalsIgnoreCase("COUNT")){
					ExpressionHandler(expressionIterator.next());
					evaluateAggregateFunction(function);
					primitiveValueResultIndex++;
				}
				else{
					expressionIterator.next(); //Pass Over the Count function onto next function
					evaluateAggregateFunction(function);
					primitiveValueResultIndex++;
				}	
			}
			primitiveValueResultIndex = 0; //Reset for next Row	
		}
		return aggregateResultsList;
	}
	public static void ExpressionHandler(Expression finalformattedExpression) throws SQLException, IOException{
		//readBuffer = line.split("\\|");
		//customeval.setWorkingTuple(currentTupleinGroupBy);
		currentVal = customeval.eval(finalformattedExpression);
		if(function.equalsIgnoreCase("NULL")) aggregateResultsList[primitiveValueResultIndex] = currentVal;
		//System.out.println("Value After Eval " + value.toString());
	}
	
	public static void evaluateAggregateFunction(String functionName) throws SQLException{
		if(functionName.equalsIgnoreCase("COUNT")) executeCount();
		else if(functionName.equalsIgnoreCase("AVG")) {
			if(lastRecordintheGroup) { 
				executeSum();
				executeAvg(); 
			}
			else executeSum();
		}
		else if(functionName.equalsIgnoreCase("MIN")) executeMin();
		else if(functionName.equalsIgnoreCase("MAX")) executeMax();
		else if(functionName.equalsIgnoreCase("SUM")) executeSum();
	}
	
	public static void executeMin() throws SQLException{
		if(customMinorThan == null)
			customMinorThan = new CustomMinorThan(currentVal, aggregateResultsList[primitiveValueResultIndex]);
		else
			customMinorThan.getResult(currentVal , aggregateResultsList[primitiveValueResultIndex]);
		
		if(aggregateResultsList[primitiveValueResultIndex] == null)
			aggregateResultsList[primitiveValueResultIndex] = currentVal;
		else if(customeval.eval(customMinorThan).toBool())
			aggregateResultsList[primitiveValueResultIndex] = currentVal;
	}
	
	public static void executeCount() throws SQLException{
		
		if(aggregateResultsList[primitiveValueResultIndex] == null)
			aggregateResultsList[primitiveValueResultIndex] = customeval.eval(new LongValue(1));
		else
			aggregateResultsList[primitiveValueResultIndex] = customeval.eval(new Addition(aggregateResultsList[primitiveValueResultIndex], new LongValue(1)));	
			
	}

	public static void executeMax() throws SQLException{
		if(customGreaterThan == null)
			customGreaterThan = new CustomGreaterThan(currentVal, aggregateResultsList[primitiveValueResultIndex]);
		else
			customGreaterThan.getResult(currentVal , aggregateResultsList[primitiveValueResultIndex]);
		
		
		if(aggregateResultsList[primitiveValueResultIndex] == null)
			aggregateResultsList[primitiveValueResultIndex] = currentVal;
		else if(customeval.eval(customGreaterThan).toBool())
			aggregateResultsList[primitiveValueResultIndex] = currentVal;
	}

	public static void executeSum() throws SQLException{
		if(customAddtion == null)
			customAddtion = new CustomAddtion(currentVal, aggregateResultsList[primitiveValueResultIndex]);
		else
			customAddtion.getResult(currentVal , aggregateResultsList[primitiveValueResultIndex]);
		
		if(aggregateResultsList[primitiveValueResultIndex] == null)
			aggregateResultsList[primitiveValueResultIndex] = currentVal;
		else
			aggregateResultsList[primitiveValueResultIndex] = customeval.eval(customAddtion);
	}
	
	public static void executeAvg() throws SQLException{	
			aggregateResultsList[primitiveValueResultIndex] = customeval.eval(new Division(aggregateResultsList[primitiveValueResultIndex], new LongValue(tempCopyOfList.size())));
	}


	/*
	public static void executeCount() throws SQLException{
		if(aggregateResultsList[primitiveValueResultIndex] == null)
			aggregateResultsList[primitiveValueResultIndex] = customeval.eval(new LongValue(1));
		else
			aggregateResultsList[primitiveValueResultIndex] = customeval.eval(new Addition(aggregateResultsList[primitiveValueResultIndex], new LongValue(1)));	
	}

	public static void executeMin() throws SQLException{
		if(aggregateResultsList[primitiveValueResultIndex] == null)
			aggregateResultsList[primitiveValueResultIndex] = currentVal;
		else if(customeval.eval(new MinorThan(currentVal, aggregateResultsList[primitiveValueResultIndex])).toBool())
			aggregateResultsList[primitiveValueResultIndex] = currentVal;
	}

	public static void executeMax() throws SQLException{
		if(aggregateResultsList[primitiveValueResultIndex] == null)
			aggregateResultsList[primitiveValueResultIndex] = currentVal;
		else if(customeval.eval(new GreaterThan(currentVal, aggregateResultsList[primitiveValueResultIndex])).toBool())
			aggregateResultsList[primitiveValueResultIndex] = currentVal;
			
	}

	public static void executeAvg() throws SQLException{
			//System.out.println("Result Before Average " + aggregateResultsList[primitiveValueResultIndex].toString());
			//System.out.println("Size used for division " + tempCopyOfList.size());
			aggregateResultsList[primitiveValueResultIndex] = customeval.eval(new Division(aggregateResultsList[primitiveValueResultIndex], new LongValue(tempCopyOfList.size())));
	}
	
	public static void executeSum() throws SQLException{
		if(aggregateResultsList[primitiveValueResultIndex] == null)
			aggregateResultsList[primitiveValueResultIndex] = currentVal;
		else
			aggregateResultsList[primitiveValueResultIndex] = customeval.eval(new Addition(currentVal, aggregateResultsList[primitiveValueResultIndex]));
	}*/

}
