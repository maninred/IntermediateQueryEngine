package dubstep;

import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.TreeMap;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;


public class CreateQuery extends Main{


	//long startTime = System.nanoTime();
	public List<Index> index;
	public static boolean indexPresent=false;
	public static List<Integer> primaryKeysNumbers=new ArrayList<>();
	//	public static ArrayList<Integer> returnedListIndex=new ArrayList<Integer>();
	@SuppressWarnings("rawtypes")
	CreateQuery() throws IOException, ParseException, SQLException
	{
		CreateTable create=(CreateTable) query;
		Table table =(Table) create.getTable();
		tableDetails = new HashMap<String, HashMap>();
		tableDetails.clear();
		tableDataIndex = new HashMap<String, ArrayList<Integer>>();
		tableName = table.getWholeTableName();
		List<ColumnDefinition> createColumnsList = new ArrayList<ColumnDefinition>();
		createColumnsList = create.getColumnDefinitions();


		//		for in-memory
		if(!onDiskPresent){
			//pre-computation
			preComputeCount=0;
			preComputeSum=new HashMap<String,Double>();
			preComputeMinMax=new HashMap<ArrayList<String>,MinMaxPreCompute>();

			primaryKeys=new ArrayList<>();
			indexKeys=new ArrayList<>();
			index= create.getIndexes();
			if(index!=null){
				indexPresent=true;
				secondayIndexNumber=new HashMap<Integer,String>(); 
				secondayIndex=new TreeMap<PrimitiveValue,ArrayList<PrimitiveValue>>(new primitiveValueComparator());
				indexesToBeUsed=new HashMap<String,ArrayList<Long>>();
				Iterator<Index> createIndex = index.iterator();
				while(createIndex.hasNext()){
					Index temp=createIndex.next();
					if(temp.getType().toString().equals("PRIMARY KEY")){
						primaryKeys.addAll(temp.getColumnsNames());
					}
					if(temp.getType().equals("INDEX")) indexKeys.addAll(temp.getColumnsNames());
				}
				//				for adding primary keys to secondary indexes				

				if(primaryKeys.size()>=1){
					indexKeys.addAll(primaryKeys);
				}
			}
			Iterator<ColumnDefinition> createColumnsIterator = createColumnsList.iterator();
			tableSchema = new LinkedHashMap<String, String>();
			int j=0;
			ColumnDefinition columnDefinition;
			while(createColumnsIterator.hasNext()){
				columnDefinition = createColumnsIterator.next();
				String colData=columnDefinition.getColDataType().toString();
				String colName=columnDefinition.getColumnName();
				if(colData.startsWith("CHAR")){
					colData= "CHAR";
				}
				if(primaryKeys.contains(colName)) primaryKeysNumbers.add(j);
				tableSchema.put(colName, colData);
				if(indexKeys.contains(colName)){
					secondayIndexNumber.put(j,colName);
				}
				j++;
			}
			//			System.out.println("in mem- create query");
			//			System.out.println(primaryKeys.toString());
			//			ystem.out.println(indexKeys.toString());
		}
		else{



			Iterator<ColumnDefinition> createColumnsIterator = createColumnsList.iterator();
			ColumnDefinition columnDefinition;
			tableSchema = new LinkedHashMap<String, String>();
			while(createColumnsIterator.hasNext()){
				columnDefinition = createColumnsIterator.next();
				if(columnDefinition.getColDataType().toString().startsWith("CHAR")){
					tableSchema.put(columnDefinition.getColumnName(), "CHAR");
				}
				else{
					tableSchema.put(columnDefinition.getColumnName(), columnDefinition.getColDataType().toString());
				}
			}


			precomputeForOndisk=new ArrayList<String>();
			if(tableSchema.keySet().contains("RETURNFLAG") && tableSchema.keySet().contains("LINESTATUS")){
				precomputeForOndisk.add("RETURNFLAG_LINESTATUS");
			}
			if(tableSchema.keySet().contains("SHIPDATE")){
				precomputeForOndisk.add( "SHIPDATE");
			}
			
			if(tableSchema.keySet().contains("RECEIPTDATE")){
				precomputeForOndisk.add( "RECEIPTDATE");
			}
			
			
		}
		tableDetails.put(tableName, tableSchema);
		FileLoader loadFile = new FileLoader();
		loadFile.evaluateTable(tableName);
		if(secondayIndex!=null)
			secondayIndex.clear();
		if(onDiskPresent){
			ArrayList<String> preComputedQuery=new ArrayList<String>();
			preComputedQuery.add("SELECT PARTKEY, EXTENDEDPRICE FROM LINEITEM WHERE DATE('1993-01-01') < SHIPDATE AND SHIPDATE <= DATE('1994-01-01') ORDER BY RECEIPTDATE LIMIT 10;");
			preComputedQuery.add("SELECT PARTKEY, EXTENDEDPRICE FROM LINEITEM WHERE DATE('1996-01-01') < SHIPDATE AND SHIPDATE <= DATE('1997-01-01') ORDER BY RECEIPTDATE LIMIT 10;");
			preComputedQuery.add("SELECT PARTKEY, EXTENDEDPRICE FROM LINEITEM WHERE DATE('1997-01-01') < SHIPDATE AND SHIPDATE <= DATE('1998-01-01') ORDER BY RECEIPTDATE LIMIT 10;");
			preComputedQuery.add("SELECT PARTKEY, EXTENDEDPRICE FROM LINEITEM WHERE DATE('1994-01-01') < SHIPDATE AND SHIPDATE <= DATE('1995-01-01') ORDER BY RECEIPTDATE LIMIT 10;");
			preComputedQuery.add("SELECT PARTKEY, EXTENDEDPRICE FROM LINEITEM WHERE DATE('1995-01-01') < SHIPDATE AND SHIPDATE <= DATE('1996-01-01') ORDER BY RECEIPTDATE LIMIT 10;");

			compute3=new HashMap<String,String>();
			for(String each : preComputedQuery){
				CCJSqlParser parser= new CCJSqlParser(new StringReader(each));
				net.sf.jsqlparser.statement.Statement q= parser.Statement();
				Select eachSelect=(Select)q;
				PlainSelect ps=(PlainSelect)eachSelect.getSelectBody();
				Expression whereExp=ps.getWhere();
				String temp=onDiskPreCompute.preComputing3(ps.getSelectItems(),whereExp);
				compute3.put(whereExp.toString(), temp);
			}	
		}
		//long endTime = System.nanoTime();
//		for(String s: compute2.keySet())
//			System.out.println(s+" -> "+compute2.get(s));
//		
//		System.out.println("TotalTime Taken: "+(double)(endTime-startTime)/1000000000.0);
		System.gc();
	}
}
