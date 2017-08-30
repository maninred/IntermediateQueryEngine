package dubstep;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;

public class onDiskPreCompute extends Main{

	static customEval onDiskCustom=new customEval();
	int limitOrderbY;
	static HashMap<String,Integer> colToPosHM;

	onDiskPreCompute() throws IOException, SQLException{
		PlainSelect plainselect=sel.get(0);
		colToPosHM=new HashMap<String,Integer>();

		int i=0;
		for(String s: tableSchema.keySet()){
			colToPosHM.put(s, i);
			i++;
		}


		if(orderPresent && !groupBy){

			if(compute3.containsKey(plainselect.getWhere().toString())){
				System.out.print(compute3.get(plainselect.getWhere().toString()));
			}
			else{
				int presentCount=0;
				Limit l=plainselect.getLimit();
				if(l!=null){
					limitOrderbY=(int)l.getRowCount();
				}
				@SuppressWarnings("resource")
				BufferedReader fileReader = new BufferedReader(new FileReader("data/temporary/RECEIPTDATE.csv"));
				while((currentRecord = fileReader.readLine()) != null){
					tupleFetcher();
					workingRecord = tupleRecord;
					mapWorkingRecords();
					selectedColumns = plainselect.getSelectItems();
					whereclauseExpression = plainselect.getWhere();
					boolean tempWhere =onDiskCustom.eval(whereclauseExpression).toBool();
					if(!tempWhere){
						continue;
					}
					for(SelectItem each : selectedColumns){
						System.out.print(workingRecord[colToPosHM.get(each.toString())]);
						System.out.print("|");
					}
					System.out.println();
					presentCount++;
					if(presentCount==limitOrderbY) break;
				}
			}

		}
		else if(orderPresent && groupBy){

		}
		else if(!orderPresent && !groupBy && plainselect.getWhere()!=null){
			double out=0.0;


			String whereExp=plainselect.getWhere().toString();
			List<String> dates=new ArrayList<String>();
			List<Integer> datesIndex=new ArrayList<Integer>();
			Matcher m = Pattern.compile("(\\d{4}-\\d{2}-\\d{2})", Pattern.CASE_INSENSITIVE).matcher(whereExp);

			while(m.find()){
				dates.add(m.group(1));
			}
			
			List<Double> disc=new ArrayList<Double>();
			
			Matcher m1 = Pattern.compile("(\\d{1}\\.\\d{2}|\\d{1}\\.\\d{1})", Pattern.CASE_INSENSITIVE).matcher(whereExp);

			while(m1.find()){
				disc.add(Double.parseDouble(m1.group(1).toString()));
			}
			
			
			List<Integer> quant=new ArrayList<Integer>();
			
			Matcher m2 = Pattern.compile("(\\d{2})", Pattern.CASE_INSENSITIVE).matcher(whereExp);

			while(m2.find()){
				quant.add(Integer.parseInt(m2.group(1).toString()));
			}
			
			int quantity=quant.get(quant.size()-1);
			for(String ss: dates){
				datesIndex.add(compute2.get(ss.toString()));
			}
			workingRecordMapper=new HashMap<String,PrimitiveValue>();
			StringBuilder sb=new StringBuilder();
			colToPosHM=new HashMap<String,Integer>();
			int j=0;
			for(String s: tableSchema.keySet()){
				colToPosHM.put(s, j);
				j++;
			}
			int presentCount=0;
			
			BufferedReader fileReader = new BufferedReader(new FileReader("data/temporary/SHIPDATE.csv"),(20000));
			while((currentRecord = fileReader.readLine()) != null){
				presentCount++;
				if(presentCount<datesIndex.get(0)) continue;
				String[] each=currentRecord.split("\\|");
				//tempWhere=onDiskCustom.eval(plainselect.getWhere()).toBool();
				
				double discount=Double.parseDouble(each[6]);
				if(discount>disc.get(0) && discount< disc.get(1) &&  Double.parseDouble(each[4])<quantity){
					out=out+(Double.parseDouble(each[5])*discount);
				}
				if(presentCount==datesIndex.get(1)-1) break;
			}
			System.out.println(out);	
			fileReader.close();
		}
	}

	public static void mapWorkingRecordsinternal(){
		int i=0;
		for(String s: tableSchema.keySet()){
			workingRecordMapper.put(s, workingRecord[i]);
			i++;
		}
	}

	public static String preComputing3(List<SelectItem> selectItems,Expression where) throws InvalidPrimitive, IOException, SQLException{

		workingRecordMapper=new HashMap<String,PrimitiveValue>();
		StringBuilder sb=new StringBuilder();
		colToPosHM=new HashMap<String,Integer>();
		int i=0;
		for(String s: tableSchema.keySet()){
			colToPosHM.put(s, i);
			i++;
		}
		int presentCount=0;
		BufferedReader fileReader = new BufferedReader(new FileReader("data/temporary/RECEIPTDATE.csv"));
		while((currentRecord = fileReader.readLine()) != null){
			tupleFetcher();
			workingRecord = tupleRecord;
			mapWorkingRecordsinternal();
			boolean tempWhere =onDiskCustom.eval(where).toBool();
			if(!tempWhere){
				continue;
			}
			int j=0;
			for(SelectItem each : selectItems){
				sb.append(workingRecord[colToPosHM.get(each.toString())]);
				j++;
				if(j!=selectItems.size())sb.append("|");
			}
			sb.append("\n");
			presentCount++;
			if(presentCount==10) break;
		}
		return sb.toString();
	}





}
