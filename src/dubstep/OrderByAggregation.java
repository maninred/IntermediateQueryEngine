//package dubstep;
//
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.Comparator;
//import java.util.List;
//
//import net.sf.jsqlparser.expression.PrimitiveValue;
//import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
//import net.sf.jsqlparser.statement.select.OrderByElement;
//
//public class OrderByAggregation extends Main{
//	public static ArrayList<String> schemaDetailsOrder;
//	
//	public static ArrayList<Integer> orderby(List<OrderByElement> orderByColList){
//		//System.out.println("INSIDE ORDERBY");
//		schemaDetailsOrder=new ArrayList(Main.getSchemaDetails.keySet());
//		Object[] oderTemp=sortedSecondaryIndexes.get(colNameParser(orderByColList.get(0).getExpression().toString()));
//		boolean ascending=orderByColList.get(0).isAsc();
//		ArrayList<Integer> orderArrayList=new ArrayList<Integer>();
//		for(Object tempObj:oderTemp){
////			ArrayList<Integer> innerOrderArrayList=secondayIndex.get(orderByColList.get(0).getExpression().toString()).get(tempObj);
//			if(orderByColList.size()>1 && innerOrderArrayList.size()>1){
//				innerOrderArrayList=orderByResolver(innerOrderArrayList,tempObj,orderByColList, 1);
//			}
//
//			orderArrayList.addAll(innerOrderArrayList);
//		}
//		if(!ascending){
//			Collections.reverse(orderArrayList);
//		}
//		ArrayList<Integer> OrderByworkingDataIndex=new ArrayList(orderArrayList);
//		//System.out.println("ORDER CLAUSE :"+orderByColList.get(0).getExpression().toString()+"-->"+orderByColList.get(0).isAsc());
//		return OrderByworkingDataIndex;
//	}
//
//	public static ArrayList<Integer> orderByResolver(ArrayList<Integer> innerOrderArrayList,Object tempObj, List<OrderByElement> orderByColList, int traverse){
//
//		ArrayList<Object> oderTemp=findingObject(innerOrderArrayList,orderByColList,traverse);
//		//System.out.println("Before Sorting " + oderTemp.toString());
//		Collections.sort(oderTemp, new MyComparator());
//		//System.out.println("After Sorting " + oderTemp.toString());
//		List<Integer> traversedIndex=new ArrayList<>();
//		boolean ascending=orderByColList.get(traverse).isAsc();
//		ArrayList<Integer> orderArrayList=new ArrayList<Integer>();
//		for(Object tempObject:oderTemp){
//				ArrayList<Integer> innerOrderArrayListResolver=secondayIndex.get(colNameParser(orderByColList.get(traverse).getExpression().toString())).get(tempObject);
//				ArrayList<Integer> filteredOrderArrayListResolver=new ArrayList<Integer>();
//				for(int innerIndex:innerOrderArrayListResolver){
//					if(innerOrderArrayList.contains(innerIndex) && !traversedIndex.contains(innerIndex)){
//						filteredOrderArrayListResolver.add(innerIndex);
//						traversedIndex.add(innerIndex);
//					}
//				}
//				if((traverse<orderByColList.size()-1) && filteredOrderArrayListResolver.size()>1){
//					filteredOrderArrayListResolver=orderByResolver(filteredOrderArrayListResolver,tempObject,orderByColList, traverse+1);
//				}
//				orderArrayList.addAll(filteredOrderArrayListResolver);
//			}
//		if(!ascending){
//			Collections.reverse(orderArrayList);
//		}
//		ArrayList<Integer> OrderByworkingDataIndex = new ArrayList(orderArrayList);
//		return OrderByworkingDataIndex;
//	}
//
//	public static ArrayList<Object> findingObject(ArrayList<Integer> innerOrderArrayList,List<OrderByElement> orderByColList, int traverse){
//
//		ArrayList<Object> returnObj=new ArrayList<Object>(innerOrderArrayList.size());
//		int outerPosition=0;
//		for(int traversingTuple : innerOrderArrayList){
//			ArrayList<PrimitiveValue> tupleOrderBy= workingData.get(traversingTuple);
//			//System.out.println(orderByColList.get(traverse).toString());
//			int pos = schemaDetailsOrder.indexOf(orderByColList.get(traverse).getExpression().toString());
//			//System.out.println("pos "+pos);
//			try {
//				returnObj.add(new ObjectPrimitiveValueMapping().primitiveToObject(tupleOrderBy.get(pos)));
//			} catch (InvalidPrimitive e) {
//				e.printStackTrace();
//			}
//		}
//		
//		return returnObj;
//	}
//	
//	public static String colNameParser(String inputColName){
//		if(inputColName.contains(".")){
//			String[] columnNames = inputColName.split("\\.");
//			inputColName = columnNames[columnNames.length-1].trim();
//		}
//		return inputColName;
//	}
//	
//	
//	static class MyComparator implements Comparator{
//		@Override
//		public int compare(Object obj1, Object obj2){
//			Integer result = 0;
//			
//			if(obj1 instanceof Integer){
//				Integer value1 = (Integer)obj1;
//				Integer value2 = (Integer)obj2;
//				result = value1.compareTo(value2);
//			}
//			
//			else if(obj1 instanceof Double){
//				Double value1 = (Double)obj1;
//				Double value2 = (Double)obj2;
//				result=value2.compareTo( value1);
//			}
//			
//			else if(obj1 instanceof String)
//			{
//				String value1 = (String)obj1;
//				String value2 = (String)obj1;
//				result=value2.compareTo( value1);
//			}
//			return result;
//		}
//	
//	}
//
//}
