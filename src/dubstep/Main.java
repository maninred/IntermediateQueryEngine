package dubstep;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Queue;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public class Main {
	public static String inputQuery;
	public static String tableName;
	public static String line;
	public static BufferedReader br = null;
	public static Scanner scan;
	public static int columnIndex;
	public static String[] readBuffer;
	public static boolean bufferFromExternalClass = false;
	public static boolean selectAll = false;
	public static boolean columnValidation = false;
	public static List<Integer> averagePositions;
	public static String currentColumn;
	public static PlainSelect plainselect;
	public static Expression columnExpression;
	public static Expression whereclauseExpression;
	public static boolean endofAggregate = false;
	@SuppressWarnings("rawtypes")
	public static HashMap<String, HashMap> tableDetails;
	public static LinkedHashMap<String, String> tableSchema; 
	public static LinkedHashMap<String, String> getSchemaDetails;
	public static List<SelectItem> selectedColumns;
	public static ArrayList<Integer> columnPositions;
	public static double aggregateResultsList[];
	public static Statement query;
	public static PrimitiveValue count;
	public static double avgCount;
	public static PrimitiveValue workingTuple[];
	//public static HashMap<String, List<ArrayList<PrimitiveValue>>> tableData = new HashMap<String, List<ArrayList<PrimitiveValue>>>();
	public static HashMap<String, ArrayList<Integer>> tableDataIndex; 
	public static List<ArrayList<PrimitiveValue>> workingData;
	public static ArrayList<Integer> workingDataIndex;
	public static ArrayList<String> workingDataSchema;
	public static List<ArrayList<PrimitiveValue>> feedData;
	public static ArrayList<String> feedDataSchema;
	public static ArrayList<PlainSelect> sel = new ArrayList<PlainSelect>();
	public static ArrayList<ArrayList<SelectItem>> schemaList;
	public static List<ArrayList<String>> workingSchemaList;
	public static int totalSelectCount=0;
	public static int presentSelectCount=0;
	public static boolean limitPresent=false;
	public static boolean whereCondition = true;
	public static long limit=0;
	public static int itemIndex = 0;
	public static int recordNumber = 0;
	public static ListIterator<Integer> tupleIterator;
	public static PrimitiveValue[] tupleRecord;
	public static PrimitiveValue[] workingRecord;
	public static HashMap<String, PrimitiveValue> workingRecordMapper = new HashMap<String, PrimitiveValue>();
	public static HashMap<Integer, HashMap<PrimitiveValue,Integer>> masterIndex; //Used for Storing the Multiple Indexes
	public static HashMap<Object,Integer> primaryKeyIndex;
	public static HashMap<Integer,Long> limitBreakData = new HashMap<Integer,Long>();
	public static HashMap<Integer,Long> limitBreakLimitData = new HashMap<Integer,Long>();
	public static Object[] sortedPrimaryKey;
	public static boolean orderByPrimaryKey = false;
	public static ObjectPrimitiveValueMapping obj;
	public static int[] positionsToBeGrouped;
	public static String[] dataTypes;
	public static String currentRecord;
	public static boolean lastRecord = false;
	public static BufferedReader fileReader;
	public static StringReader input;
	public static String pattern = "(\\b)[A-Za-z_]+(\\.)";
	public static customEval expressionEvaluator = new customEval();
	//public static boolean firstRecord = false;


	public static List<String> primaryKeys;
	public static List<String> indexKeys;

	public static TreeMap<PrimitiveValue,ArrayList<PrimitiveValue>> secondayIndex;
	public static HashMap<String,ArrayList<Long>> indexesToBeUsed;
	public static HashMap<Integer,String> secondayIndexNumber;
	//public static HashMap<String,Object[]> sortedSecondaryIndexes;

	//public static HashMap<Integer,String> modifiedIndexesNumber;
	//public static HashMap<String,HashMap<Object,ArrayList<Integer>>> modifiedIndexes;
	//public static HashMap<String,Object[]> sortedModifiedIndexes;

	public static boolean orderPresent=false;
	public static HashMap<Integer,List<OrderByElement>> orderByMasterData;
	public static List<OrderByElement> orderByExpression;
	public static List<OrderByElement> orderByToBeWorkedOn;
	public static HashMap<PrimitiveValue,ArrayList<Integer>> orderByFlyIndexes;
	public static List<HashMap<Integer,PrimitiveValue>>orderByOtherColumns;
	public static OrderByTest newTryOrder;



	public static HashMap<String, ArrayList<Integer>> groupByMasterData;
	public static boolean groupBy = false;
	public static List<Column> groupByExpression;
	public static GroupByAggregationTest groupbyInvoker;

	//Group by and Order By
	public static boolean groupByandOrderBy = false;
	public static ArrayList<PrimitiveValue[]> groupedByKeysforOrdering;
	public static boolean requireSchemaCopy = false;
	public static LinkedHashMap<String, String> schemaDetailsforGroupbyandOrdering;
	public static int limitforGroupByOrderBy = 0;
	public static boolean limitonGroupOrder = false;
	public static boolean onDiskPresent= false; 
	public static Iterator<Long> inmemoryTupleIterator;
	
	public static int sizeofatuple;

	public static String[] splitSchema;
	public static Expression AggregateWhereClause;

	public static LinkedHashMap<Long,PrimitiveValue[]> overallData;
	public static boolean aggregateInOuterMost;
	
	
	// pre computation	
	
	public static int preComputeCount;
	public static HashMap<String,Double> preComputeSum;
	public static HashMap<ArrayList<String>,MinMaxPreCompute> preComputeMinMax;
	public static ArrayList<ArrayList<String>> allCombinationsList;
	
	
	
	//pre-compute on disk try
	public static ArrayList<String> precomputeForOndisk;
	public static HashMap<String,String> compute3; 
	public static HashMap<String,Integer> compute2=new HashMap<String,Integer>();
	public static boolean directComp;
	
	public static ArrayList<String> tablesInSelect;
	


	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String[] args) throws ParseException, SQLException, IOException {
		scan = new Scanner(System.in);
		for (int i = 0; i < args.length; i++) {
			if(args[i].equals("--in-mem")){
				onDiskPresent=false;
				FileSplitandMerge.recordLimitOnMemory = 49999;
			}
			else if(args[i].equals("--on-disk")){
				FileSplitandMerge.recordLimitOnMemory = 9999;
				onDiskPresent=true;
			}
			else{
				onDiskPresent=false;
				FileSplitandMerge.recordLimitOnMemory = 49999;
			}
		}
		while(true){	
			System.out.print("$>");
			Main.splitSchema = new String[2];
			FileSplitandMerge.part = 0;
			String tempr="";

			while(scan.hasNext()){
				tempr += scan.nextLine().trim()+" " ;
				if(tempr.contains(";")) break;
			}
			inputQuery = tempr.replaceAll(pattern,"");
			//System.out.println(inputQuery);
			input = new StringReader(inputQuery);
			//System.out.println("Input " + inputQuery.toString());
			CCJSqlParser parser= new CCJSqlParser(input);
			query = parser.Statement();
			schemaList = new ArrayList<ArrayList<SelectItem>>();

			if(query instanceof Select){
				tablesInSelect=new ArrayList<String>();
				directComp=false;
				orderPresent=false;
				lastRecord = false;
				sizeofatuple=0;
				fileReader = new BufferedReader(new FileReader("data/"+tableName+".csv"));
				schematoDataTypeLoader();
				limitPresent = false;
				limit=0;
				//workingData = new ArrayList(tableData.get(tableName));
				//workingDataIndex = new ArrayList(tableDataIndex.get(tableName));
				orderByMasterData = new HashMap<Integer,List<OrderByElement>>();
				orderByMasterData.clear();
				Select select=(Select) query;
				plainselect = (PlainSelect) select.getSelectBody();
				FromItem fromItem = plainselect.getFromItem();
				Table table = (Table)fromItem;
				tableName = table.getWholeTableName();
				tablesInSelect.add(tableName);
				for(Join eachTable: plainselect.getJoins()){
					tablesInSelect.add(eachTable.toString());
				}

				while(!(fromItem instanceof Table)){
					Limit l = plainselect.getLimit();
					totalSelectCount++;
					if(l!=null){
						limitBreakData.put(totalSelectCount-1, 0L);
						limit = l.getRowCount();
						limitBreakLimitData.put(totalSelectCount-1, limit);
					}

					List<OrderByElement> orderByColList = plainselect.getOrderByElements();


					if(orderByColList!=null){
						orderByMasterData.clear();
						orderByMasterData.put(totalSelectCount-1, orderByColList);
						//System.out.println("ORDER BY "+orderByColList.get(0).getExpression().toString());
					}

					sel.add(plainselect);
					schemaList.add((ArrayList<SelectItem>) plainselect.getSelectItems()); 
					SubSelect ts = (SubSelect) fromItem;
					plainselect = (PlainSelect) ts.getSelectBody();
					fromItem = plainselect.getFromItem();
				}

				sel.add(plainselect);
				schemaList.add((ArrayList<SelectItem>) plainselect.getSelectItems());
				//Evaluate for Aggregate Function 
				//This is a functionality implemented to check for Evalauting the Aggregate function
				if(!plainselect.getSelectItems().get(0).toString().equals("*")){
					SelectExpressionItem expression = (SelectExpressionItem) plainselect.getSelectItems().get(0);
					Expression finalexpression = (Expression) expression.getExpression();
					if(finalexpression instanceof Function) 
						SelectQuery.aggregateEvaluation = true;
				}
				PlainSelect outerselect = sel.get(0);
				orderByToBeWorkedOn = outerselect.getOrderByElements();
				groupByExpression = outerselect.getGroupByColumnReferences();

				
				// where clause checking and bypassing				
				AggregateWhereClause=null;
				boolean whetherToBreak=whereClauseAggregateEvalation();
				aggregateInOuterMost=false;
				if(whetherToBreak){
					resetFlagsandLists();
					continue;
				}
				if(AggregateWhereClause==null && sel.size()==1){
					List<SelectItem> selectItems=sel.get(0).getSelectItems();
					SelectExpressionItem expressionItemformatted = (SelectExpressionItem) selectItems.get(0);
					Expression finalformattedExpression = (Expression) expressionItemformatted.getExpression();
					if(finalformattedExpression instanceof Function)
						aggregateInOuterMost=true;
				}
				
				
				
				Limit l = outerselect.getLimit();

				if(l!=null){
					limitBreakData.put(totalSelectCount, 0L);
					limit = l.getRowCount();
					limitforGroupByOrderBy = (int) limit;
					limitonGroupOrder = true;
					limitBreakLimitData.put(totalSelectCount, limit);
				}

				if(orderByToBeWorkedOn != null) {
					orderPresent = true;
					requireSchemaCopy = true;
				}

				if(groupByExpression != null){
					groupBy = true;
					requireSchemaCopy = true;
				}

				if(groupBy&& orderPresent){
					boolean columnGroupandOrderbyPresent = false;
					if(groupByExpression.size()!=orderByToBeWorkedOn.size()){

						for(Column col:groupByExpression){
							columnGroupandOrderbyPresent = false;
							for(OrderByElement OBE:orderByToBeWorkedOn){
								if(col.toString().equals(OBE.getExpression().toString())){
									columnGroupandOrderbyPresent = true;
								}
							}
							if(!columnGroupandOrderbyPresent){	
								OrderByElement orderBYExp=new OrderByElement();
								orderBYExp.setExpression((Expression)col);
								orderBYExp.setAsc(true);
								orderByToBeWorkedOn.add(orderBYExp);		
							}

						}

					}

				}

				else if(groupBy&& !orderPresent){
					orderByToBeWorkedOn = new ArrayList<OrderByElement>();
					for(Column col:groupByExpression){
						OrderByElement orderBYExp=new OrderByElement();
						orderBYExp.setExpression((Expression)col);
						orderBYExp.setAsc(true);
						orderByToBeWorkedOn.add(orderBYExp);
					}
					orderPresent=true;
				}

				workingDataSchema = new ArrayList(getSchemaDetails.keySet());
				//buildSchema = new BuildSchemaDetails(); //Builds the Schema for all the Inner Queries - Stores the Schema in a static list
				workingSchemaList = BuildSchemaDetails.buildSchemaPlan();		
				//PlainSelect lastPlainSelect = plainselect;
				orderByPrimaryKey = true; //Temporarily Doing it
				//tupleIterator = workingDataIndex.listIterator();
				if(onDiskPresent && !groupBy &&  ( orderPresent || outerselect.getWhere()!=null) ){
					
					new onDiskPreCompute();
					SelectQuery.aggregateEvaluation=false;
					
				}
				else if(!onDiskPresent && (groupBy || orderPresent) && (indexesToBeUsed.containsKey(orderByToBeWorkedOn.get(0).getExpression().toString()))){
					//System.out.println("index using provoked");
					GroupandOrderIndexes.gatherData();
				}
				else if(!onDiskPresent && !groupBy && !orderPresent && aggregateInOuterMost){
					if(!onDiskPresent){
						inmemoryTupleIterator = overallData.keySet().iterator(); 
					}
					new UsingPreComputedMinMax(sel.get(0).getSelectItems());
				}
				else{
					
					if(groupBy && onDiskPresent){	
						fileReader=new BufferedReader(new FileReader("data/temporary/RETURNFLAG_LINESTATUS.csv"));
						directComp=true;
					}
					
					inmemoryTupleIterator=null;
					if(!onDiskPresent){
						inmemoryTupleIterator = overallData.keySet().iterator(); 
					}
					tupleIterator:

						while((onDiskPresent &&(currentRecord = fileReader.readLine()) != null) ||(!onDiskPresent && inmemoryTupleIterator.hasNext()) ){
							if(onDiskPresent){
								currentRecord = currentRecord.trim();
								int len=currentRecord.length();
								if(sizeofatuple<len) sizeofatuple=len;
								if(len==0) continue;
								//while(tupleIterator.hasNext()){		
								whereCondition = true;
								recordNumber++; //Incremented for every Tuple Read
								tupleFetcher();
								if(!fileReader.ready()) {
									lastRecord = true;
								}
							}
							else{
								tupleRecord=overallData.get(inmemoryTupleIterator.next());
								if(!inmemoryTupleIterator.hasNext()) {
									lastRecord = true;
								}
							}

							
							//tupleRecord = workingData.get(tupleIterator.next());
							workingRecord = tupleRecord;
							mapWorkingRecords();
							//System.out.println("Working Record in Main " + workingRecord.toString() + "Tuple Record " + tupleRecord);
							presentSelectCount = totalSelectCount;
							workingDataSchema = new ArrayList(getSchemaDetails.keySet());
							//System.out.println("Working Data Schema in Main " + workingDataSchema.toString());
							ListIterator<PlainSelect> selectQueryIterator = sel.listIterator(sel.size());
							ListIterator<ArrayList<String>> innerQuerySchemaDetails = workingSchemaList.listIterator();
							//System.out.println("----------End of Schema for 1 Query-------");
							boolean limitTemp=false;

							QueryIterator:		
								while(selectQueryIterator.hasPrevious()){ 
									//tupleRecord = workingData.get(tupleIterator.nextIndex() - 1);
									if(presentSelectCount != totalSelectCount) {
										workingDataSchema.clear();
										if(innerQuerySchemaDetails.hasNext()) 
											workingDataSchema = new ArrayList(innerQuerySchemaDetails.next());
										//System.out.println("Working Schema " + workingDataSchema.toString());
									}

									plainselect = selectQueryIterator.previous();
									if(totalSelectCount==0 || presentSelectCount==totalSelectCount) {
										Table table1 = (Table)fromItem;
										tableName = table1.getWholeTableName();
									}
									selectedColumns = plainselect.getSelectItems();
									whereclauseExpression = plainselect.getWhere();


									if(!onDiskPresent && whereclauseExpression.toString().contains("EVENT_ID =")){
										Scanner in = new Scanner(whereclauseExpression.toString()).useDelimiter("[^0-9]+");
										int integer = in.nextInt();
										tupleRecord=overallData.get((long) integer);
										workingRecord = tupleRecord;
										mapWorkingRecords();
										SelectQuery.HandleExpressionandColumns(tableName, selectedColumns);
										break tupleIterator;
									}
									
									
									if(whereclauseExpression != null){
										SelectWhere.whereClauseEvaluator(); 
										if(!whereCondition){
											break QueryIterator;
										}
									}
									else if (whereclauseExpression == null){
										avgCount++;
										SelectQuery.HandleExpressionandColumns(tableName, selectedColumns);
									}

									//

									if((!groupBy && !orderPresent) && limitBreakData.containsKey(presentSelectCount)){
										long t = limitBreakData.get(presentSelectCount)+1;
										//System.out.println(presentSelectCount+" ---> "+t);
										if(t == limitBreakLimitData.get(presentSelectCount)) limitTemp=true;
										limitBreakData.put(presentSelectCount, t);
									}
									//if(presentSelectCount==0 && limitTemp) break tupleIterator;
									if(presentSelectCount == 0 && limitTemp) break tupleIterator;
									presentSelectCount--;
								}
							if(SelectQuery.setfirstTimeAggregates == true)
								SelectQuery.setfirstTimeAggregates = false;
							//totalSelectCount = 0;
							//tupleRecord.clear();
							//firstRecord = false;
							presentSelectCount = 0;
							itemIndex++;
						}
				}
				resetFlagsandLists();
			}	
			else if(query instanceof CreateTable){
				new CreateQuery();
			}
			else{
				throw new java.sql.SQLException("The following query doesn't start with CREATE OR SELECT "+query);
			}
		}
		//}
	}

	public static void mapWorkingRecords(){
		for(int i=0; i < workingRecord.length; i++){
			workingRecordMapper.put(workingDataSchema.get(i), workingRecord[i]);
		}
		
	}
	public static void resetFlagsandLists() throws SQLException, IOException{

		if(orderByFlyIndexes!=null)
			orderByFlyIndexes.clear();
		if(orderByOtherColumns!=null)
			orderByOtherColumns.clear();
		if(orderByFlyIndexes!=null)
			orderByFlyIndexes.clear();
		if(orderByMasterData!=null)
			orderByMasterData.clear();
		limitonGroupOrder = false;
		orderByPrimaryKey = false;
		totalSelectCount = 0;
		FileSplitandMerge.tupleCount = 0;
		FileSplitandMerge.fileopenRequired = true;
		itemIndex = 0;
		recordNumber = 0;
		schemaList.clear();
		sel.clear();
		if((!groupBy) && (!orderPresent) && !aggregateInOuterMost)
			SelectQuery.aggregatePrint();
		SelectQuery.aggregateEvaluation=false;
		orderPresent = false;
		groupBy = false;
		groupByandOrderBy = false;
		avgCount = 0;
		limitBreakLimitData.clear();
		limitBreakData.clear();
		if(workingSchemaList != null)
			workingSchemaList.clear();
		//workingRecord.clear();
		fileReader.close();
		input.close();
		//if(inmemoryTupleIterator != null && aggregateResultsList != null){
		aggregateResultsList = null;
			//System.gc();
		//}	
	}



	public static void groupByPreprocessing() {

		if(groupByExpression != null){
			//System.out.println("Group By Invoked");
			groupBy = true;
			groupByMasterData = new HashMap<String, ArrayList<Integer>>();
			positionsToBeGrouped = new int[groupByExpression.size()];
			Iterator<Column> groupByExpressionIterator = groupByExpression.iterator();
			int index = 0;
			columnIndex = 0;
			while(groupByExpressionIterator.hasNext()){
				String groupByColumn = groupByExpressionIterator.next().toString();
				//				if(groupByColumn.contains(".")) {
				//					String[] groupSplit = groupByColumn.split("\\.");
				//					groupByColumn = groupSplit[groupSplit.length - 1];
				//				}
				int columnIndex = Main.workingDataSchema.indexOf(groupByColumn);
				positionsToBeGrouped[index++] = columnIndex;
			}
			obj = new ObjectPrimitiveValueMapping();
		}
	}

	public static void tupleFetcher() {
		tupleRecord = new PrimitiveValue[dataTypes.length];
		PrimitiveValue currentColumnValue = null;
		StringTokenizer st = new StringTokenizer(currentRecord, "|");
		String s;
		int j=0;
		while(j < dataTypes.length) {
			s = st.nextToken();
			if(s.length() == 0) continue;
			if(j < dataTypes.length){
				switch(dataTypes[j].toLowerCase()){
				case "int": currentColumnValue = new LongValue(Long.parseLong(s.trim()));  break;
				case "decimal": currentColumnValue = new DoubleValue(Double.parseDouble(s.trim())); break;
				case "date": currentColumnValue = new DateValue((s.trim()));  break;
				case "string":
				case "varchar":
				case "char": currentColumnValue = new StringValue(s); break;
				//default: System.out.println("CurrentColumns Value " + currentColumnValue.toString());
				}
				//System.out.println(currentColumnValue.toString());
				tupleRecord[j] = currentColumnValue;
			}
			j++;
		}
	}

	public static void schematoDataTypeLoader(){
		Collection<String> c = getSchemaDetails.values();
		Iterator<String> itr = c.iterator();
		dataTypes = new String[c.size()];
		int i = 0;
		while (itr.hasNext()) {
			String s = itr.next();
			//System.out.println("Data Types Loaded: " + s);
			dataTypes[i++] =  s;
		}
	}
	
	public static boolean whereClauseAggregateEvalation(){
		ArrayList<Expression> allWhereExpression=new ArrayList<Expression>();
		ArrayList<ArrayList<String>> bothExp=new ArrayList<ArrayList<String>>();
		
		for(PlainSelect eachSelect: sel){
			Expression eachWhere=eachSelect.getWhere();
			if(eachWhere!=null){
				if(AggregateWhereClause==null) AggregateWhereClause=eachWhere;
				else AggregateWhereClause=new AndExpression(AggregateWhereClause,eachWhere);
				allWhereExpression.addAll(splitWhere(eachWhere));
			}
		}
		if(allWhereExpression.size()==0) return false;
		for(Expression exp:allWhereExpression){
			ArrayList<String> eachExp=new ArrayList<String>(); 
			eachExp.add(((BinaryExpression)exp).getLeftExpression().toString());
			eachExp.add(((BinaryExpression)exp).getRightExpression().toString());
			Collections.sort(eachExp);
			if(bothExp.contains(eachExp)) return true;
			else bothExp.add(eachExp);
		}
		return false;
	}
	
	public static ArrayList<Expression> splitWhere(Expression eachWhere){
		ArrayList<Expression> toBeReturned=new ArrayList<Expression>();
		Queue<Expression> newQueue=new LinkedList<Expression>(); 
		newQueue.add(eachWhere);
		
		while(!newQueue.isEmpty()){
			Expression polled=newQueue.poll();
			if(polled instanceof AndExpression){
				newQueue.add(((AndExpression) polled).getLeftExpression());
				newQueue.add(((AndExpression) polled).getRightExpression());
			}
			else{
				toBeReturned.add(polled);
			}
		}
		return toBeReturned;
	}
}

