package dubstep;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.PriorityQueue;
import java.util.Map;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class FileSplitandMerge extends Main{
	static int tupleCount = 0;
	static int part = 0;
	static boolean fileopenRequired = true;
	static boolean fileWrite = false;
	//private static Scanner[] reader;
	//private static BufferedReader file;
	//private static BufferedWriter splitFile;
	//private static String[] bufferedString;
	private static PrimitiveValue[][] bufferedPrimitiveValue;
	private static BufferedReader[] secondLevelReader;
	//private static Scanner[] secondLevelScanner;

	private static int numberOfOrderBy=0;
	//recordLimitOnMemoryprivate static int recordsLimit = 10000;
	public static int recordLimitOnMemory;
	private static String[] colNames;
	private static Boolean[] colBool;
	private static ArrayList<Integer> colPos;
	private static PriorityQueue<TupleFileIdMap> indexPriorityQueue;
	//private static ArrayList<String> SchemaDetailsForSorting;
	//private static StringBuilder sb;
	//private static String strtoWrite;
	private static TupleFileIdMap eachTuple;
	private static SelectItem selectItem;
	private static SelectExpressionItem expressionItemformatted;
	private static Expression finalformattedExpression;
	private static PrimitiveValue value;
	private static customEval expressionEvaluator;
	private static ArrayList<Integer> columnstobeSelected;
	private static StringBuilder fileRecord = new StringBuilder();
	public static CustomMinorThan customMinorcomp;
	public static CustomGreaterThan customMajorcomp;

	//private static boolean firstColumn = false;
	//private static boolean finalColumn = false;
	private static PrimitiveValue[] values;
	private static int num = 0;
	private static int countRecords = 0;

	//Group By
	private static ArrayList<PrimitiveValue> TempAL;
	private static ArrayList<PrimitiveValue> incomingAL=new ArrayList<PrimitiveValue>();
	private static Map<Integer,String> posToFuntionMap = new HashMap<Integer,String>();

	private static int groupbycount;
	private static boolean lastRecordtobeMerged = false;


	private static customEval fileSplitEval=new customEval();

	public static void fileSplit() throws IOException, SQLException{
		if(requireSchemaCopy) {
			if (Files.isDirectory(Paths.get("data/"+tableName+"/"))) {
				Arrays.stream(new File("data/"+tableName+"/").listFiles()).forEach(File::delete);
			}
			part = 0;
			customMinorcomp = new CustomMinorThan(new DoubleValue(1), new DoubleValue(1)); 
			customMajorcomp = new CustomGreaterThan(new DoubleValue(1), new DoubleValue(1));
			lastRecordtobeMerged = false;
			schemaDetailsforGroupbyandOrdering = new LinkedHashMap<String, String>();
			buildDataType();
			countRecords = 0;
			requireSchemaCopy = false;
			//Create the Schema DataTypes
		}
		if(directComp){
			printValuesForGroupBy(workingRecord);
			if(lastRecord) printForGroupAggregate();
		}
		else{
		if(fileopenRequired){
			//bufferedString = new String[recordLimitOnMemory];
			bufferedPrimitiveValue = new PrimitiveValue[recordLimitOnMemory][workingRecord.length];
			if (!Files.isDirectory(Paths.get("data/"+tableName+"/"))) {
				Path path = Paths.get("data/"+tableName+"/");
				Files.createDirectories(path);
			}
			//Path splitFile = Paths.get("data/"+tableName+"/"+"Part"+ part +".csv");
			fileopenRequired = false;
			fileWrite = false;
			//strtoWrite = buildDatatoWrite();
			bufferedPrimitiveValue[tupleCount++] = tupleRecord;
			//bufferedString[tupleCount++] = currentRecord;
		}

		else if(tupleCount < recordLimitOnMemory){
			//strtoWrite = buildDatatoWrite();
			bufferedPrimitiveValue[tupleCount++] = tupleRecord;
			//bufferedString[tupleCount++] = currentRecord;
			//System.out.println("Tuple Count Increased to " + tupleCount);
		}

		if(tupleCount == recordLimitOnMemory) {
			//System.out.println("Flushing to Disk" + tupleCount);
			recordSort();
			fileWrite();
			fileWrite = true;
			//bufferedString = new String[recordLimitOnMemory];
			bufferedPrimitiveValue = new PrimitiveValue[recordLimitOnMemory][workingRecord.length];
			part++;
			fileopenRequired = true;
			tupleCount = 0;
		}
		}
	}

	public static void buildDataType(){
		//Iterator<PrimitiveValue> workingRecord = Main.workingRecord.iterator();
		Iterator<String> workingSchemaIterator = Main.workingDataSchema.iterator();
		for(PrimitiveValue s: Main.workingRecord){
			//PrimitiveValue s = workingRecord.next();
			if(s instanceof LongValue) {
				schemaDetailsforGroupbyandOrdering.put(workingSchemaIterator.next().toString().toLowerCase(), "int");
			}
			else if (s instanceof DoubleValue) {
				schemaDetailsforGroupbyandOrdering.put(workingSchemaIterator.next().toString().toLowerCase(),"decimal");
			}
			else if(s instanceof DateValue) {
				schemaDetailsforGroupbyandOrdering.put(workingSchemaIterator.next().toString().toLowerCase(),"date");
			}
			else{
				schemaDetailsforGroupbyandOrdering.put(workingSchemaIterator.next().toString().toLowerCase(),"string");
			}
		}

		numberOfOrderBy=orderByToBeWorkedOn.size();
		colNames=new String[numberOfOrderBy];
		colBool=new Boolean[numberOfOrderBy];
		String formattedColName;
		//String[] splittedColName = new String[2];
		int i=0;
		for(OrderByElement OE: orderByToBeWorkedOn){
			formattedColName = OE.getExpression().toString();
			//			if(formattedColName.contains(".")){
			//				splittedColName = formattedColName.split("\\.");
			//				formattedColName = splittedColName[1];
			//			}
			colNames[i]=formattedColName;
			colBool[i]=OE.isAsc();
			i++;
		}

		int colIndex = 0;
		if(colPos!= null) colPos.clear();
		else colPos = new ArrayList<Integer>();
		if(columnstobeSelected != null) columnstobeSelected.clear();
		else columnstobeSelected = new ArrayList<Integer>();
		for(String col: colNames){
			//System.out.println("Main Working Schema " + Main.workingDataSchema.toString());
			colIndex = Main.workingDataSchema.indexOf(col);
			//System.out.println("Column Index " + colIndex);
			colPos.add(colIndex); //Adds the Column Position to be Ordered based upon
		}
		//SelectExpressionItem expression = (SelectExpressionItem) selectedColumns;
		//Expression finalexpression = (Expression) expression.getExpression();
		Iterator<SelectItem> selectColumnsIterator = selectedColumns.iterator();
		//System.out.println("Selected Columns " + selectedColumns.toString());
		//		int j = 0;
		String formatColName = ""; 
		while(selectColumnsIterator.hasNext()){
			//String colName;
			//String[] splitColName = new String[2];
			formatColName = selectColumnsIterator.next().toString();
			//			if(formatColName.contains(".")){
			//				splitColName = formatColName.split("\\.");
			//				formatColName = splitColName[1];
			//			}
			//System.out.println("ColumnName " + formatColName);
			columnstobeSelected.add(Main.workingDataSchema.indexOf(formatColName));
		}

	}

	/*
	public static String buildDatatoWrite(){
		if(sb == null) sb = new StringBuilder();
		else sb.setLength(0);
		Iterator<PrimitiveValue> streamRecord = Main.workingRecord.iterator();
		while(streamRecord.hasNext()){
			if(sb.length() == 0) sb.append(streamRecord.next().toRawString());
			else if(sb.length() > 0) sb.append("|").append(streamRecord.next().toRawString());
		}
		//System.out.println("Building Data to write!" + sb.toString());
		return sb.toString();
	}*/

	public static void lastRecordsortandFlush() throws IOException, SQLException{	
		if(lastRecord){
			//if(lastRecord) 
			//System.out.println("Last Record!");
			if((!(bufferedPrimitiveValue == null))){
				if(((bufferedPrimitiveValue.length >0))){	
					//bufferedPrimitiveValue = Arrays.stream(bufferedPrimitiveValue).filter(s -> (s != null && s.length > 0)).toArray(PrimitiveValue[][]::new);
					List<PrimitiveValue[]> items = new ArrayList<PrimitiveValue[]>();
					for(PrimitiveValue[] input : bufferedPrimitiveValue) {
						if (input[0] != null) {
							items.add(input);
						}
					}
					bufferedPrimitiveValue = new PrimitiveValue[items.size()][workingRecord.length];
					bufferedPrimitiveValue = items.toArray(new PrimitiveValue[items.size()][workingRecord.length]);
					tupleCount = 0;
					setSelectedColumns();//Getting the Expression from Outermost select
					recordSort();
					fileWrite(); //Final Write
					fileMergeandSort();
					fileopenRequired = true;
				}
			}
			bufferedPrimitiveValue = new PrimitiveValue[0][0];
		}
		//Final Delete
		if(bufferedPrimitiveValue != null && bufferedPrimitiveValue.length> 0) {
			Arrays.stream(new File("data/"+tableName+"/").listFiles()).forEach(File::delete);
			schemaDetailsforGroupbyandOrdering.clear();
		}

	}

	public static void setSelectedColumns(){
		ListIterator<PlainSelect> selectQueryIterator = sel.listIterator();
		PlainSelect plainselect = selectQueryIterator.next();
		selectedColumns = plainselect.getSelectItems();
	}

	public static void recordSort(){
		//Arrays.sort(bufferedString, new recordComparator());
		Arrays.sort(bufferedPrimitiveValue, new recordComparator());
	}

	public static void fileWrite() throws IOException{
		//String tempTuple;
		//StringBuilder sb = new StringBuilder();
		Path splitFile = Paths.get("data/"+tableName+"/"+"Part"+ part +".csv");
		for(int i = 0; i <bufferedPrimitiveValue.length; i++){
			if(bufferedPrimitiveValue[i] == null) continue;
			for(int j=0; j <bufferedPrimitiveValue[i].length; j++){
				fileRecord.append(bufferedPrimitiveValue[i][j].toRawString());
				if(j!=bufferedPrimitiveValue[i].length-1)fileRecord.append("|"); 
			}
			fileRecord.append("\n");
			//fileRecord.append(bufferedPrimitiveValue[i] + "\n");
			//if(i != bufferedString.length - 1) splitFile.write(tempTuple + "\n");
			//splitFile.write(tempTuple + "\n");
			//else splitFile.write(tempTuple);
		}
		Files.write(splitFile,fileRecord.toString().getBytes());
		fileRecord.setLength(0);
		//System.gc(); //Trying to Garbage Collect
		//splitFile.close();
	}

	public static void fileMergeandSort() throws IOException, SQLException{
		//System.out.println("Merge Sort Called!");
		secondLevelReader = new BufferedReader[part+1];
		indexPriorityQueue=new PriorityQueue<TupleFileIdMap>(new objectComparator()); 
		//Declare Custom Variables for comparasion
		TupleFileIdMap TFM;
		for(int i=0;i<=part;i++){
			if(onDiskPresent){
				secondLevelReader[i]=new BufferedReader(new FileReader("data/"+tableName+"/"+"Part"+ i +".csv"));
			}
			else{
				secondLevelReader[i]=new BufferedReader(new FileReader("data/"+tableName+"/"+"Part"+ i +".csv"));
			}
			TFM = new TupleFileIdMap(i,tupleParserforPriniting(secondLevelReader[i].readLine()));
			indexPriorityQueue.add(TFM);
		}
		//int lastTraversed=0;
		int partsDone=0;
		List<Integer> fileTraversalOver=new ArrayList<Integer>();
		//BufferedWriter outputBW=new BufferedWriter(new FileWriter("data/"+tableName+"/"+"final.csv")); 
		Merger:
			while(partsDone<=part){
				if(limitforGroupByOrderBy == countRecords && limitonGroupOrder) {
					if(TempAL != null)
						TempAL.clear();
					break Merger;
				}
				eachTuple=indexPriorityQueue.poll();
				if(values == null) values = new PrimitiveValue[workingRecord.length];
				//System.out.println("Each Tuples " + eachTuple.tuple);
				values = eachTuple.tuple;
				if(groupBy){
					//System.out.println("Boolean " + limitonGroupOrder);
					printValuesForGroupBy(values);

				}
				else{
					printValuesafterOrdering(values);
				}
				//outputBW.write(eachTuple.tuple);
				//outputBW.newLine();
				int partNumber = eachTuple.partId;
				if(!fileTraversalOver.contains(partNumber)){
					if(secondLevelReader[partNumber].ready()){
						indexPriorityQueue.add(new TupleFileIdMap(partNumber,tupleParserforPriniting(secondLevelReader[partNumber].readLine())));
					}else{
						partsDone++;
						fileTraversalOver.add(partNumber);
						if(partsDone>part && groupBy){
							lastRecordtobeMerged=true;
							printForGroupAggregate();
							//posToFuntionMap.clear();
						}
					}
				}
			}
		//Closing the BufferReaders
		for(int i = 0; i <= part; i++){
			secondLevelReader[i].close();
		}
		//System.gc();
	}

	public static void printValuesForGroupBy(PrimitiveValue[] valueofTuple) throws SQLException{
		if(!selectedColumns.get(0).toString().equals("*")){
			Iterator<SelectItem> selectColumnsIterator = selectedColumns.iterator();
			int it=0;
			//int total = selectedColumns.size();
			for(int i:colPos){
				incomingAL.add(valueofTuple[i]);
			}
			while(selectColumnsIterator.hasNext()){
				//System.out.println("inside group by ");
				selectItem = selectColumnsIterator.next();
				expressionItemformatted = (SelectExpressionItem) selectItem;
				finalformattedExpression = (Expression) expressionItemformatted.getExpression();
				customEval.setWorkingTuple(valueofTuple);
				if(finalformattedExpression instanceof Function){
					Function function = (Function) finalformattedExpression;
					if(!posToFuntionMap.containsKey(it)) posToFuntionMap.put(it,function.getName().toLowerCase());
					if(function.getName().toLowerCase().equals("count")) incomingAL.add(new LongValue(1));
					else incomingAL.add(getExpressionFromHandler((Expression)function.getParameters().getExpressions().get(0)));
				}
				else{
					incomingAL.add(getExpressionFromHandler(finalformattedExpression));
				}
				//System.out.println("Expression Sent " + finalformattedExpression.toString());
				//System.out.println("Value of the Tuple " + valueofTuple);
				if(!selectColumnsIterator.hasNext()){ 
					//System.out.println("Calling------------------>");
					//System.out.println("TempAL "+TempAL.toString());
					//System.out.println("Incoming "+incomingAL.toString());
					twoPVcomparison();
				}
				it++;
				//				firstColumn = false;
				//				finalColumn = false;
			}
		}

	}


	public static void twoPVcomparison() throws InvalidPrimitive, SQLException{
		if(TempAL == null || TempAL.isEmpty()){
			TempAL = new ArrayList<PrimitiveValue>();
			TempAL.addAll(incomingAL);
			incomingAL.clear();
			groupbycount = 1;
		}
		else{
			boolean updateTheOld=true;
			//System.out.println(colPos.toString());
			int j=0;
			for(int i : colPos){
				//System.out.println("TempAL"+TempAL.toString());
				//System.out.println("incomingAL"+incomingAL.toString());
				if(!TempAL.get(j).toRawString().equals(incomingAL.get(j).toRawString())){
					//System.out.println("TempAL.get(i) "+TempAL.get(i));
					//System.out.println("incomingAL.get(i) "+incomingAL.get(i));
					updateTheOld=false;
					break;
				}
				j++;
			}

			if(!updateTheOld){
				printForGroupAggregate();
				groupbycount=1;
				TempAL.addAll(incomingAL);
				incomingAL.clear();
			}
			else{
				groupbycount++;
				int colSize=colPos.size();
				for(int i:posToFuntionMap.keySet()){
					PrimitiveValue tempALvalue=TempAL.get(i+colSize);
					PrimitiveValue incomingValue=incomingAL.get(i+colSize);
					switch(posToFuntionMap.get(i).toLowerCase()){
					case "count": {
						TempAL.set(i+colSize, new LongValue(groupbycount)); break;
					}
					case "min": {
						customMinorcomp.getResult(incomingValue, tempALvalue);
						if(fileSplitEval.eval(customMajorcomp).toBool()) 
							TempAL.set(i+colSize,incomingValue); 
						break;
					}
					case "max": {
						customMajorcomp.getResult(incomingValue, tempALvalue);
						if(fileSplitEval.eval(customMajorcomp).toBool())
							TempAL.set(i+colSize,incomingValue);
						break;
					}
					case "sum": {
						TempAL.set(i+colSize,fileSplitEval.eval(new Addition(incomingValue,tempALvalue))); break;
					}
					case "avg": {
						TempAL.set(i+colSize,fileSplitEval.eval(new Addition(incomingValue,tempALvalue))); break;
					}
					}

				}
				incomingAL.clear();
			}	
		}		
	}

	public static void printForGroupAggregate() throws SQLException{
		//System.out.println("Calling output");
		//System.out.println("Temp AL " + TempAL.toString());
		PrimitiveValue pv;
		for(int i=colPos.size();i<TempAL.size();i++){
			pv=TempAL.get(i);
			if(posToFuntionMap.containsKey(i-colPos.size()) && posToFuntionMap.get(i-colPos.size()).equals("avg")) System.out.print(fileSplitEval.eval(new CustomDivision(pv,new LongValue(groupbycount))).toRawString());
			else {
				System.out.print(pv.toRawString());
				//System.out.println("Count " + countRecords);
			}
			if(i==TempAL.size()-1) 
			{	
				countRecords++;
				System.out.println();
			}
			else System.out.print("|");
		}
		TempAL.clear();
		if(lastRecordtobeMerged) posToFuntionMap.clear();
	}

	public static PrimitiveValue getExpressionFromHandler(Expression exp) throws SQLException{
		if(expressionEvaluator == null) {
			expressionEvaluator = new customEval();
		}
		value = expressionEvaluator.eval(exp);
		//System.out.println("Value After Eval " + value.toString()); 
		return value;

	}

	public static void printValuesafterOrdering(PrimitiveValue[] valueofTuple) throws SQLException{
		//System.out.println("selectedColumns" + selectedColumns.toString());
		if(selectedColumns.get(0).toString().equals("*")){
			//System.out.print(eachTuple.tuple);
			int i=0;
			for(PrimitiveValue tupleColumnValue: eachTuple.tuple){
				System.out.print(tupleColumnValue);
				if(i!=eachTuple.tuple.length-1) System.out.print("|");
				i++;
			}
			System.out.println();
			countRecords++;
		}
		//		else if(!selectedColumns.get(0).toString().equals("*")){
		else {
			Iterator<SelectItem> selectColumnsIterator = selectedColumns.iterator();
			while(selectColumnsIterator.hasNext()){
				selectItem = selectColumnsIterator.next();
				expressionItemformatted = (SelectExpressionItem) selectItem;
				finalformattedExpression = (Expression) expressionItemformatted.getExpression();
				//System.out.println("Expression Sent " + finalformattedExpression.toString());
				//System.out.println("Value of the Tuple " + valueofTuple);
				customEval.setWorkingTuple(valueofTuple);
				ExpressionHandler(finalformattedExpression);
				if(selectColumnsIterator.hasNext()) 
					System.out.print("|");
				else 
					System.out.println();
				//				firstColumn = false;
				//				finalColumn = false;
			}
			countRecords++;
		}	
	}

	public static void ExpressionHandler(Expression exp) throws SQLException{
		if(expressionEvaluator == null) {
			expressionEvaluator = new customEval();
		}
		value = expressionEvaluator.eval(exp);
		//System.out.println("Value After Eval " + value.toString()); 
		printPrimitiveValue();

	}

	public static void printPrimitiveValue(){
		System.out.print(value.toRawString());
	}

	static class recordComparator implements Comparator<PrimitiveValue[]>{

		@Override
		public int compare(PrimitiveValue[] s1_parsed, PrimitiveValue[] s2_parsed) {
			//            PrimitiveValue[] s1_parsed = tuppleParser(s1);
			//            PrimitiveValue[] s2_parsed = tuppleParser(s2);

			int traversal=0;

			while(traversal < numberOfOrderBy){
				try {
					String posOfThePV = new ArrayList<String>(Main.schemaDetailsforGroupbyandOrdering.keySet()).get(colPos.get(traversal)).toString().toLowerCase();
					String dataOfThePV = Main.schemaDetailsforGroupbyandOrdering.get(posOfThePV);
					//System.out.println("Data of the PV Tuple Comparator" + dataOfThePV);
					//String dataOfThePV = dataTypes[colPos.get(traversal)].toString().toLowerCase();
					if(dataOfThePV.equals("string") || dataOfThePV.equals("char") || dataOfThePV.equals("varchar")){
						if(!colBool[traversal]) num=s2_parsed[colPos.get(traversal)].toString().compareTo(s1_parsed[colPos.get(traversal)].toString());
						else num =s1_parsed[colPos.get(traversal)].toString().compareTo(s2_parsed[colPos.get(traversal)].toString());
						if(num==0){
							traversal++;
							continue;
						}
						else
							return num;
					}
					else if(dataOfThePV.equals("date")){
						if(fileSplitEval.eval(customMajorcomp.getResult(s1_parsed[colPos.get(traversal)],s2_parsed[colPos.get(traversal)])).toBool()){
							if(!colBool[traversal]) return -1;
							return 1;
						}
						else if(fileSplitEval.eval(customMinorcomp.getResult(s1_parsed[colPos.get(traversal)],s2_parsed[colPos.get(traversal)])).toBool()){
							if(!colBool[traversal]) return 1;
							return -1;
						}
						else 
							traversal++;
					}
					else{

						//if(fileSplitEval.eval(customMajorcomp.getResult(s1_parsed[colPos.get(traversal)],s2_parsed[colPos.get(traversal)])).toBool()){
						if(s1_parsed[colPos.get(traversal)].toDouble() > s2_parsed[colPos.get(traversal)].toDouble()){
							if(!colBool[traversal]) return -1;
							return 1;
						}
						else if((s1_parsed[colPos.get(traversal)].toDouble() < s2_parsed[colPos.get(traversal)].toDouble())){
							if(!colBool[traversal]) return 1;
							return -1;
						}
						else
							traversal++;
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			return 0;
		}

	}



	static class objectComparator implements Comparator<TupleFileIdMap>{

		@Override
		public int compare(TupleFileIdMap s1, TupleFileIdMap s2) {
			PrimitiveValue[] s1_parsed = s1.tuple;
			PrimitiveValue[] s2_parsed = s2.tuple;

			int traversal=0;
			while(traversal<numberOfOrderBy){
				try {
					String posOfThePV = new ArrayList<String>(Main.schemaDetailsforGroupbyandOrdering.keySet()).get(colPos.get(traversal)).toString().toLowerCase();
					String dataOfThePV = Main.schemaDetailsforGroupbyandOrdering.get(posOfThePV);
					//System.out.println("Data of the PV Tuple Comparator " + dataOfThePV);
					//String dataOfThePV=dataTypes[colPos.get(traversal)].toString().toLowerCase();
					if(dataOfThePV.equals("string") || dataOfThePV.equals("char") || dataOfThePV.equals("varchar")){
						if(!colBool[traversal])  num=s2_parsed[colPos.get(traversal)].toString().compareTo(s1_parsed[colPos.get(traversal)].toString());
						else  num=s1_parsed[colPos.get(traversal)].toString().compareTo(s2_parsed[colPos.get(traversal)].toString());

						if(num==0){
							traversal++;
							if(traversal >= numberOfOrderBy){
								if(s1.partId >= s2.partId) return 1;
								else return -1;
							}
						}
						else
							return num;
					}
					else if(dataOfThePV.equals("date")){
						if(fileSplitEval.eval(customMajorcomp.getResult(s1_parsed[colPos.get(traversal)],s2_parsed[colPos.get(traversal)])).toBool()){
							if(!colBool[traversal]) return -1;
							return 1;
						}
						else if(fileSplitEval.eval(customMinorcomp.getResult(s1_parsed[colPos.get(traversal)],s2_parsed[colPos.get(traversal)])).toBool()){
							if(!colBool[traversal]) return 1;
							return -1;
						}
						else 
							traversal++;
						if(traversal >= numberOfOrderBy){
							if(s1.partId >= s2.partId) return 1;
							else return -1;
						}
					}
					else{
						//if(fileSplitEval.eval(customMajorcomp.getResult(s1_parsed[colPos.get(traversal)],s2_parsed[colPos.get(traversal)])).toBool()){
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
						if(traversal >= numberOfOrderBy){
							if(s1.partId >= s2.partId) return 1;
							else return -1;
						}
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			return 0;
		}

	}


	public static PrimitiveValue[] tuppleParser(PrimitiveValue[] tuple){
		PrimitiveValue[] parsedPV=new PrimitiveValue[numberOfOrderBy];
		int index = 0;
		//StringTokenizer st = new StringTokenizer(tuple, "|");
		//		String[] s = tuple.split("\\|");
		String columnName;
		int j = 0;
		int i=0;
		//Iterator<String> schemaColumnIterator = Main.workingDataSchema.iterator();
		for(String orderbyColumn : colNames){
			columnName = orderbyColumn.toString();
			j = new ArrayList<String>(Main.schemaDetailsforGroupbyandOrdering.keySet()).indexOf(columnName.toLowerCase());
			//			if(colPos.contains(j)){
			//				switch(schemaDetailsforGroupbyandOrdering.get(columnName.toLowerCase()).toLowerCase()){
			//				case "int": 	parsedPV[index++] = new LongValue(Long.parseLong(s[j]));  break;
			//				case "decimal": parsedPV[index++]  = new DoubleValue(Double.parseDouble(s[j])); break;
			//				case "date": 	parsedPV[index++]  = new DateValue((s[j]));  break;
			//				case "string":
			//				case "varchar":
			//				case "char":	parsedPV[index++]  = new StringValue(s[j]); break;	
			//				//default: System.out.println("CurrentColumns Value " + currentColumnValue.toString());
			//				}
			//			}
			parsedPV[i++]=tuple[j];
		}

		return parsedPV;
	}


	public static PrimitiveValue[] tupleParserforPriniting(String tuple){
		//int index = 0;
		//System.out.println("Data Types Length " + dataTypes.length);
		//System.out.println("Col Positions " + columnstobeSelected.toString());
		String[] s = tuple.split("\\|");
		PrimitiveValue[] printformattedPV = new PrimitiveValue[s.length];
		String columnName;
		int j = 0;
		int i=0;
		//Iterator<Integer> columnsIterator = columnstobeSelected.iterator();
		Iterator<String> schemaColumnIterator = Main.workingDataSchema.iterator();
		//System.out.println("Main Schema " + columnIterator.next());
		while(schemaColumnIterator.hasNext()){
			columnName = schemaColumnIterator.next().toString();
			j = new ArrayList<String>(Main.schemaDetailsforGroupbyandOrdering.keySet()).indexOf(columnName.toLowerCase());
			switch(schemaDetailsforGroupbyandOrdering.get(columnName.toLowerCase()).toLowerCase()){
			case "int": printformattedPV[i]=(new LongValue(Long.parseLong(s[j])));  break;
			case "decimal": printformattedPV[i]=(new DoubleValue(Double.parseDouble(s[j]))); break;
			case "date": printformattedPV[i]=(new DateValue(s[j]));  break;
			case "string":
			case "varchar":
			case "char": printformattedPV[i]=(new StringValue(s[j])); break;
			}
			i++;
		}
		//System.out.println("Returned Tuple" + printformattedPV.toString());
		return printformattedPV;
	}

}
