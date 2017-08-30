package dubstep;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.PriorityQueue;
import java.util.StringTokenizer;

import dubstep.FileSplitandMerge.objectComparator;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.StringValue;

public class FileLoader extends Main{

	PrimitiveValue currentColumnValue = null;
	private static customEval fileLoader=new customEval();
	public static CustomMinorThan customMinorcomp; 
	public static CustomGreaterThan customMajorcomp;
	public static ArrayList<String> preCoputeDoubleList;
	public static HashMap<String, Integer> colNameToPosition;
	public static int numberOfOrderBy;
	public static ArrayList<String> send;
	public static ArrayList<Integer> positionOfsend;
	public static ArrayList<String> dataOfsend;
	public static customEval fileSplitEval=new customEval();
	ArrayList<ArrayList<String>> overall;

	public void allCombinationsIntialization(ArrayList<String> preCoputeDoubleList){
		FileLoader.preCoputeDoubleList=preCoputeDoubleList;
		int total=preCoputeDoubleList.size();
		overall=new ArrayList<ArrayList<String>>();
		for(int i=1;i<=total;i++){
			overall.addAll(new ArrayList<ArrayList<String>>(combine(total,i)));
		}
		allCombinationsList=overall;
		for(ArrayList<String> eachList: overall){
			preComputeMinMax.put(eachList, new MinMaxPreCompute(eachList));
		}
	}


	public static ArrayList<ArrayList<String>> combine(int n, int k) {
		ArrayList<ArrayList<String>> combs = new ArrayList<ArrayList<String>>();
		combine(combs, new ArrayList<String>(), 1, n, k);
		return combs;
	}


	public static void combine(ArrayList<ArrayList<String>> combs, ArrayList<String> comb, int start, int n, int k) {
		if(k==0) {
			Collections.sort(comb);
			combs.add(new ArrayList<String>(comb));
			return;
		}
		for(int i=start;i<=n;i++) {
			comb.add(preCoputeDoubleList.get(i-1));
			combine(combs, comb, i+1, n, k-1);
			comb.remove(comb.size()-1);
		}
	}

	static HashMap <String, Double> tempHm;

	public static void emitMinMax(PrimitiveValue[] eachPv) throws InvalidPrimitive{
		tempHm=new HashMap <String, Double>();
		for(String eachColName:preCoputeDoubleList){
			tempHm.put(eachColName, eachPv[colNameToPosition.get(eachColName)].toDouble());
		}

		for(ArrayList<String> eachArrayList: allCombinationsList){
			preComputeMinMax.get(eachArrayList).setMinMax(listOfNums(eachArrayList));
		}
	}

	public static ArrayList<Double> listOfNums(ArrayList<String> eachArrayList){
		ArrayList<Double> returnDouble=new ArrayList<Double>();
		for(String eachColName: eachArrayList){
			returnDouble.add(tempHm.get(eachColName));
		}
		return returnDouble;
	}

	@SuppressWarnings("unchecked")
	public void evaluateTable(String tablename) throws IOException, InvalidPrimitive{
		customMinorcomp = new CustomMinorThan(new DoubleValue(1), new DoubleValue(1));
		customMajorcomp= new CustomGreaterThan(new DoubleValue(1), new DoubleValue(1));
		//ArrayList<PrimitiveValue> tableDataTemp = new ArrayList<PrimitiveValue>();
		//List<ArrayList<PrimitiveValue>> innerTableData = new ArrayList<ArrayList<PrimitiveValue>>();
		overallData=new LinkedHashMap<Long,PrimitiveValue[]>();
		getSchemaDetails = (LinkedHashMap<String, String>) tableDetails.get(tableName);


		Collection<String> c = getSchemaDetails.values();
		Iterator<String> itr = c.iterator();
		Iterator<String> itrCol = tableSchema.keySet().iterator();
		dataTypes = new String[c.size()];
		String[] colName = new String[c.size()];
		ArrayList<String> preCoputeDoubleList=new ArrayList<String>(); 

		colNameToPosition= new HashMap<String,Integer>();

		int i = 0;
		while (itr.hasNext()) {
			dataTypes[i] =  itr.next();
			colName[i]=itrCol.next();
			//!primaryKeys.contains(colName[i]) &&
			if( (dataTypes[i].toLowerCase().equals("int") || dataTypes[i].toLowerCase().equals("decimal"))){
				preCoputeDoubleList.add(colName[i]);
				colNameToPosition.put(colName[i], i);
			}
			i++;
		}
		int linecount=0;

		if(!onDiskPresent){
			Collections.sort(preCoputeDoubleList);
			allCombinationsIntialization(preCoputeDoubleList);
			BufferedReader indexBuilderReader = new BufferedReader(new FileReader("data/"+tableName+".csv"));
			String currentRecord;
			//System.out.println("Inside Index Builder");
			if(primaryKeyIndex != null) primaryKeyIndex.clear();
			if(secondayIndex != null) secondayIndex.clear();
			while((currentRecord = indexBuilderReader.readLine()) != null ){
				preComputeCount++;
				PrimitiveValue[] pvofEachRecord=new PrimitiveValue[dataTypes.length];
				StringTokenizer st = new StringTokenizer(currentRecord, "|");
				for (int j = 0;j<dataTypes.length; j++) {
					String s = st.nextToken();
					PrimitiveValue KeyObject=null;
					switch(dataTypes[j].toLowerCase()){
					case "int": KeyObject = new LongValue(Long.parseLong(s));  break;
					case "decimal": KeyObject = new DoubleValue(Double.parseDouble(s)); break;
					case "date": KeyObject =new DateValue(s);  break;
					case "string": 
					case "varchar": 
					case "char": KeyObject =new StringValue(s); break;	
					}
					pvofEachRecord[j]=KeyObject;
					if(dataTypes[j].toLowerCase().equals("int") || dataTypes[j].toLowerCase().equals("decimal")){
						preComputeSum.put(colName[j], preComputeSum.getOrDefault(colName[j], 0.0)+KeyObject.toDouble());
					}
				}
				emitMinMax(pvofEachRecord);
				if(CreateQuery.indexPresent){
					boolean primaryKeyisOne=false;
					int primaryIndex = linecount;
					if(CreateQuery.primaryKeysNumbers.size()==1){
						primaryKeyisOne=true;
						primaryIndex=CreateQuery.primaryKeysNumbers.get(0);
						overallData.put(pvofEachRecord[primaryIndex].toLong(), pvofEachRecord);
					}
					else{
						overallData.put((long)primaryIndex, pvofEachRecord);
					}
					for(int index:secondayIndexNumber.keySet()){
						ArrayList<indexObject> forEachSec=secondayIndex.getOrDefault(secondayIndexNumber.get(index),new ArrayList<indexObject>());
						if(primaryKeyisOne)
							forEachSec.add(new indexObject(pvofEachRecord[index],pvofEachRecord[primaryIndex]));
						else
							forEachSec.add(new indexObject(pvofEachRecord[index],new LongValue(primaryIndex)));
						secondayIndex.put(secondayIndexNumber.get(index), forEachSec);
					}
				}
				linecount++;
			}


			//			for(ArrayList<String> eachArr: preComputeMinMax.keySet()){
			//				System.out.println(eachArr.toString());
			//				System.out.println("max :"+preComputeMinMax.get(eachArr).maxValue);
			//				System.out.println("min :"+preComputeMinMax.get(eachArr).minValue);
			//				System.out.println();
			//			}


			indexBuilderReader.close();

			//for displaying indexes

			//		System.out.println("Before Sorting");
			//		if(CreateQuery.indexPresent){
			//			if(secondayIndex!=null){
			//				for(String tt: secondayIndex.keySet()){
			//					System.out.println("the secondary index for "+tt);
			//					ArrayList<indexObject> ttt=secondayIndex.get(tt);
			//					for(indexObject tttt: ttt){
			//						System.out.println(tttt.secondaryIndexValue.toString()+" --------> "+tttt.primaryIndexValue.toString());
			//					}
			//					System.out.println("+++++++++++++++++++++++++++++++++++++");
			//				}
			//
			//			}
			//
			//		}

			if(CreateQuery.indexPresent){
				if(secondayIndex!=null){
					for(String eashSecColName: secondayIndex.keySet()){
						ArrayList<indexObject> forEachIndex=secondayIndex.get(eashSecColName);
						Collections.sort(forEachIndex,new indexObjectComparator());
						secondayIndex.put(eashSecColName,forEachIndex);
						ArrayList<Long> primaryIndexList=new ArrayList<Long>(forEachIndex.size());
						for(indexObject eachindexObject:forEachIndex)
							primaryIndexList.add(eachindexObject.primaryIndexValue.toLong());
						forEachIndex.clear();
						indexesToBeUsed.put(eashSecColName, primaryIndexList);
					}
				}
			}
		}


		else{

			if (!Files.isDirectory(Paths.get("data/temporary/"))) {
				Path path = Paths.get("data/temporary/");
				Files.createDirectories(path);
			}
			
			int j=0;
			for(String s:precomputeForOndisk){
				j++;
				if (!Files.isDirectory(Paths.get("data/temporary/"+s+"/"))) {
					Path path = Paths.get("data/temporary/"+s+"/");
					Files.createDirectories(path);
				}
//				if(j==1) continue;
				dumpMemoryWhileOnDisk(s);
			}






		}


	}

	void dumpMemoryWhileOnDisk(String s) throws IOException{
		send=new ArrayList<String>();
		positionOfsend=new ArrayList<Integer>();
		dataOfsend=new ArrayList<String>();

		int part =0;
		if(!s.contains("_")) send.add(s);
		else{
			String[] afterSplit=s.split("_");
			send.add(afterSplit[0]);
			send.add(afterSplit[1]);
		}
		Iterator<String> it=tableSchema.keySet().iterator();
		int k=0;
		while(it.hasNext()){
			String each=it.next();
			if(each.equals(send.get(0))){
				positionOfsend.add(k);
				dataOfsend.add(tableSchema.get(each));
			}
			if(send.size()>1 && each.equals(send.get(1))){
				positionOfsend.add(k);
				dataOfsend.add(tableSchema.get(each));
			}
			k++;
		}

		numberOfOrderBy=send.size();

		BufferedReader readingBuffer= new BufferedReader(new FileReader("data/"+tableName+".csv"));
		ArrayList<PrimitiveValue[]> bufferedPVs=new ArrayList<PrimitiveValue[]>(FileSplitandMerge.recordLimitOnMemory); 

		while((currentRecord = readingBuffer.readLine()) != null){
			tupleFetcher();
			workingRecord = tupleRecord;
			bufferedPVs.add(tupleRecord);
			if(bufferedPVs.size()==FileSplitandMerge.recordLimitOnMemory || !readingBuffer.ready()){
				Collections.sort(bufferedPVs, new recordComparator());
				StringBuilder fileRecord = new StringBuilder();
				Path splitFile = Paths.get("data/temporary/"+s+"/"+"Part"+ part +".csv");
				for(int i = 0; i <bufferedPVs.size(); i++){
					if(bufferedPVs.get(i) == null) continue;
					for(int j=0; j <bufferedPVs.get(i).length; j++){
						fileRecord.append(bufferedPVs.get(i)[j].toRawString());
						if(j!=bufferedPVs.get(i).length-1)fileRecord.append("|"); 
					}
					fileRecord.append("\n");
				}
				Files.write(splitFile,fileRecord.toString().getBytes());
				fileRecord.setLength(0);
				bufferedPVs.clear();
				part++;
			}
		}

		readingBuffer.close();
		TupleFileIdMap TFM;
		PriorityQueue<TupleFileIdMap> indexPriorityQueue=new PriorityQueue<TupleFileIdMap>(new objectComparatorMerge());
		BufferedReader[] secondLevelReader=new BufferedReader[part];
		FileWriter file = new FileWriter("data/temporary/"+s+".csv");
		BufferedWriter bf = new BufferedWriter(file);


		for(int i=0;i<part;i++){
			secondLevelReader[i]=new BufferedReader(new FileReader("data/temporary/"+s+"/"+"Part"+i+".csv"));
		}

		for( int i=0;i<part;i++){
			currentRecord=secondLevelReader[i].readLine();
			tupleFetcher();
			TFM = new TupleFileIdMap(i,tupleRecord);
			indexPriorityQueue.add(TFM);
		}

		int partsDone=0;
		HashMap<Integer,Boolean> intToBool=new HashMap<Integer,Boolean>();
		for(int a=0;a<part;a++){
			intToBool.put(a, true);
		}

		TupleFileIdMap eachTuple;
		StringBuilder st=new StringBuilder();
		List<Integer> fileTraversalOver=new ArrayList<Integer>();
		int i=0;

		while(!indexPriorityQueue.isEmpty()){
			i++;
			eachTuple=indexPriorityQueue.poll();
			st.setLength(0);;
			for(PrimitiveValue pv:eachTuple.tuple){
				st.append(pv.toRawString());
				st.append("|");
			}
			if(s.equals("SHIPDATE") && !compute2.containsKey(eachTuple.tuple[10].toRawString())) compute2.put(eachTuple.tuple[10].toRawString(), i);
			st.append("\n");
			bf.write(st.toString());
			int j=eachTuple.partId;
			if(!fileTraversalOver.contains(j)){
				if(secondLevelReader[j].ready()){
					currentRecord=secondLevelReader[j].readLine();
					tupleFetcher();
					indexPriorityQueue.add(new TupleFileIdMap(j,tupleRecord));
				}else{
					partsDone++;
					fileTraversalOver.add(j);
					secondLevelReader[j].close();
				}
			}

		}


		bf.close();
		file.close();

	}

	static class indexObjectComparator implements Comparator<indexObject>{
		@Override
		public int compare(indexObject s1, indexObject s2) {
			PrimitiveValue s1_sec=s1.secondaryIndexValue;
			PrimitiveValue s2_sec=s2.secondaryIndexValue;
			if(s1_sec instanceof StringValue)
				return s1_sec.toString().compareTo(s2_sec.toString());
			else{
				try {
					if(fileLoader.eval(customMajorcomp.getResult(s1_sec, s2_sec)).toBool())
						return 1;
					else if(fileLoader.eval(customMinorcomp.getResult(s1_sec, s2_sec)).toBool())
						return -1;
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			return 0;
		}
	}



	static class recordComparator implements Comparator<PrimitiveValue[]>{

		@Override
		public int compare(PrimitiveValue[] s1_parsed, PrimitiveValue[] s2_parsed) {

			int traversal=0;
			int num;

			while(traversal < numberOfOrderBy){
				try {
					String dataOfThePV = dataOfsend.get(traversal).toLowerCase();
					if(dataOfThePV.equals("string") || dataOfThePV.equals("char") || dataOfThePV.equals("varchar")){
						num =s1_parsed[positionOfsend.get(traversal)].toString().compareTo(s2_parsed[positionOfsend.get(traversal)].toString());
						if(num==0){
							traversal++;
						}
						else
							return num;
					}
					else if(dataOfThePV.equals("date")){
						if(fileSplitEval.eval(customMajorcomp.getResult(s1_parsed[positionOfsend.get(traversal)],s2_parsed[positionOfsend.get(traversal)])).toBool()){
							return 1;
						}
						else if(fileSplitEval.eval(customMinorcomp.getResult(s1_parsed[positionOfsend.get(traversal)],s2_parsed[positionOfsend.get(traversal)])).toBool()){
							return -1;
						}
						else 
							traversal++;
					}
					else{
						if(s1_parsed[positionOfsend.get(traversal)].toDouble() > s2_parsed[positionOfsend.get(traversal)].toDouble()){
							return 1;
						}
						else if((s1_parsed[positionOfsend.get(traversal)].toDouble() < s2_parsed[positionOfsend.get(traversal)].toDouble())){
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

	static class objectComparatorMerge implements Comparator<TupleFileIdMap>{

		@Override
		public int compare(TupleFileIdMap s1, TupleFileIdMap s2) {
			PrimitiveValue[] s1_parsed=s1.tuple;
			PrimitiveValue[] s2_parsed=s2.tuple;
			int traversal=0;
			int num = -1;

			while(traversal < numberOfOrderBy){
				try {
					String dataOfThePV = dataOfsend.get(traversal).toLowerCase();
					if(dataOfThePV.equals("string") || dataOfThePV.equals("char") || dataOfThePV.equals("varchar")){
						num =s1_parsed[positionOfsend.get(traversal)].toString().compareTo(s2_parsed[positionOfsend.get(traversal)].toString());
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
						if(fileSplitEval.eval(customMajorcomp.getResult(s1_parsed[positionOfsend.get(traversal)],s2_parsed[positionOfsend.get(traversal)])).toBool()){
							return 1;
						}
						else if(fileSplitEval.eval(customMinorcomp.getResult(s1_parsed[positionOfsend.get(traversal)],s2_parsed[positionOfsend.get(traversal)])).toBool()){
							return -1;
						}
						else{
							traversal++;
							if(traversal >= numberOfOrderBy){
	                            if(s1.partId >= s2.partId) return 1;
	                            else return -1;
	                        }
						}

					}
					else{
						if(s1_parsed[positionOfsend.get(traversal)].toDouble() > s2_parsed[positionOfsend.get(traversal)].toDouble()){
							return 1;
						}
						else if((s1_parsed[positionOfsend.get(traversal)].toDouble() < s2_parsed[positionOfsend.get(traversal)].toDouble())){
							return -1;
						}
						else{
							traversal++;
							if(traversal >= numberOfOrderBy){
	                            if(s1.partId >= s2.partId) return 1;
	                            else return -1;
	                        }
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			return 0;
		}

	}



}
