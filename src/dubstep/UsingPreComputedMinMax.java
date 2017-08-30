package dubstep;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class UsingPreComputedMinMax extends Main{


	public static HashMap<SelectItem,Double> alreadyComputed;
	public static HashMap<SelectItem,Double> toBeComputed;
	public static ArrayList<SelectItem> selectItems;
	public static ArrayList<SelectItem> toBeComputedList;
	public static ArrayList<SelectItem> alreadyComputedList;
	public static customEval preCompute=new customEval();


	UsingPreComputedMinMax(List<SelectItem> selectItems) throws InvalidPrimitive, SQLException{
		Iterator<SelectItem> selectIt=selectItems.iterator();
		alreadyComputed=new HashMap<SelectItem,Double>();
		toBeComputed=new HashMap<SelectItem,Double>();
		toBeComputedList=new ArrayList<SelectItem>();
		alreadyComputedList=new ArrayList<SelectItem>();
		this.selectItems=new ArrayList<SelectItem>();
		while(selectIt.hasNext()){
			SelectItem eachSelectItem=selectIt.next();
			this.selectItems.add(eachSelectItem);
			distinguishPreCompute(eachSelectItem);
		}
		computeAlreadyComputedList();
		computeToBeComputed();
		printAll();

	}


	void distinguishPreCompute(SelectItem eachSelectItem){
		SelectExpressionItem expressionItemformatted = (SelectExpressionItem) eachSelectItem;
		Expression finalformattedExpression = (Expression) expressionItemformatted.getExpression();
		Function function = (Function) finalformattedExpression;
		String functionName=function.getName().toString().toLowerCase();
		String functionExpression="";
		if(function.getParameters()!=null)
			functionExpression=function.getParameters().getExpressions().get(0).toString();
		if(functionName.equals("count")) alreadyComputedList.add(eachSelectItem);
		else if(functionExpression.contains("*")||functionExpression.contains("-")) toBeComputedList.add(eachSelectItem);
		else alreadyComputedList.add(eachSelectItem);
	}

	void computeAlreadyComputedList(){

		for(SelectItem alreadycomputed:alreadyComputedList){
			SelectExpressionItem expressionItemformatted = (SelectExpressionItem) alreadycomputed;
			Expression finalformattedExpression = (Expression) expressionItemformatted.getExpression();
			Function function = (Function) finalformattedExpression;
			String functionName=function.getName().toString().toLowerCase();
			if(functionName.equals("count")){
				alreadyComputed.put(alreadycomputed, (double)preComputeCount);
			}
			else{
				Expression functionExpression=function.getParameters().getExpressions().get(0);
				List<String> expressionParsed=expressionParser(functionExpression);
				double onceDouble=doubleCalculation(expressionParsed);

				HashMap<String,Integer> expressionHMCount=countOfEach(expressionParsed);
				ArrayList<String> toBeSent=new ArrayList<String>(expressionHMCount.keySet());
				Collections.sort(toBeSent);

				if(functionName.equals("sum")){
					double sum=0.0;
					for(String each:expressionHMCount.keySet()){
						sum+=preComputeSum.get(each)*expressionHMCount.get(each);
					}
					sum+=(onceDouble*preComputeCount);
					alreadyComputed.put(alreadycomputed, sum);
				}
				else if(functionName.equals("min")){
					double min=0.0;
					min=preComputeMinMax.get(toBeSent).minValue;
					for(String each:expressionHMCount.keySet()){
						min+=(expressionHMCount.get(each)-1)*preComputeMinMax.get(toBeSent).minList.get(each);
					}
					min+=onceDouble;
					alreadyComputed.put(alreadycomputed, min);
				}
				else if(functionName.equals("max")){
					double max=0.0;
					max=preComputeMinMax.get(toBeSent).maxValue;
					for(String each:expressionHMCount.keySet()){
						max+=(expressionHMCount.get(each)-1)*preComputeMinMax.get(toBeSent).maxList.get(each);
					}
					max+=onceDouble;
					alreadyComputed.put(alreadycomputed, max);
				}
				else if(functionName.equals("avg")){

					double sum=0.0;
					for(String each:expressionHMCount.keySet()){
						sum+=preComputeSum.get(each)*expressionHMCount.get(each);
					}
					sum+=(onceDouble*preComputeCount);
					alreadyComputed.put(alreadycomputed, (sum/(double)preComputeCount));

				}

			}

		}

	}

	List<String> expressionParser(Expression functionExpression){
		String[] expString=functionExpression.toString().split("\\+");
		List<String> eachExpList=new ArrayList<String>();
		for(String each:expString)
			eachExpList.add(each.trim().toUpperCase());
		return eachExpList;
	}

	double doubleCalculation(List<String> expressionParsed){
		double returnedDouble=0.0;
		for(String each: expressionParsed){
			double d=0.0;
			try  
			{  
				d = Double.parseDouble(each);  
			}  
			catch(NumberFormatException nfe)  
			{  
				continue;  
			}  
			returnedDouble+=d;
		}

		return returnedDouble;
	}

	HashMap<String,Integer> countOfEach(List<String> expressionParsed){
		HashMap<String,Integer> hmRet=new HashMap<String,Integer>();
		for(String each: expressionParsed){
			try  
			{  
				Double.parseDouble(each);  
			}  
			catch(NumberFormatException nfe)  
			{  
				hmRet.put(each, hmRet.getOrDefault(each, 0)+1); 
			}  
		}
		return hmRet;
	}

	void computeToBeComputed() throws InvalidPrimitive, SQLException{
		ArrayList<String> others=new ArrayList<String>();
		ArrayList<Expression> otherExp=new ArrayList<Expression>(); 
		for(SelectItem each : toBeComputedList){
			SelectExpressionItem expressionItemformatted = (SelectExpressionItem) each;
			Expression finalformattedExpression = (Expression) expressionItemformatted.getExpression();
			Function function = (Function) finalformattedExpression;
			String functionName=function.getName().toString().toLowerCase();
			others.add(functionName);
			otherExp.add((Expression)function.getParameters().getExpressions().get(0));			
		}


		while(inmemoryTupleIterator.hasNext()){
			tupleRecord=overallData.get(inmemoryTupleIterator.next());
			workingRecord = tupleRecord;
			Main.mapWorkingRecords();
			for(int i=0;i<others.size();i++){
				String each=others.get(i).toLowerCase();
				double eachCal=preCompute.eval(otherExp.get(i)).toDouble();
				if(!toBeComputed.containsKey(toBeComputedList.get(i))){
					toBeComputed.put(toBeComputedList.get(i), eachCal);
				}
				else{
					if(each.equals("min")){
						toBeComputed.put(toBeComputedList.get(i),Math.min(eachCal, toBeComputed.get(toBeComputedList.get(i))) );
					}
					else if(each.equals("max")){
						toBeComputed.put(toBeComputedList.get(i),Math.max(eachCal, toBeComputed.get(toBeComputedList.get(i))) );
					}
					else if(each.equals("sum")){
						toBeComputed.put(toBeComputedList.get(i),eachCal+toBeComputed.get(toBeComputedList.get(i)));
					}
					else if(each.equals("avg")){
						toBeComputed.put(toBeComputedList.get(i),eachCal+toBeComputed.get(toBeComputedList.get(i)));
					}
				}
			}
			
		}
		
		//for avg
		for(int i=0;i<others.size();i++){
			if(others.get(i).equals("avg")){
				toBeComputed.put(toBeComputedList.get(i), toBeComputed.get(toBeComputedList.get(i))/(double)preComputeCount);
				
			}
		}
		
		
	}


	void printAll(){
		Iterator<SelectItem> itFinal=selectItems.iterator();
		while(itFinal.hasNext()){
			SelectItem eachSI=itFinal.next();
			if(alreadyComputedList.contains(eachSI)){
				System.out.print(alreadyComputed.get(eachSI));
			}
			else if(toBeComputedList.contains(eachSI)){
				System.out.print(toBeComputed.get(eachSI));
			}
			if(itFinal.hasNext()) System.out.print("|");
			else System.out.println();
		}
	}
}
