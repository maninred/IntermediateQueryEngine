//package dubstep;
//
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.Comparator;
//import java.util.HashMap;
//
//public class IndexBuilder extends Main{
//
//	public static void OrderByPrimarySecondaryIndex(){
//		ArrayList<Object> sortedKeys= new ArrayList(primaryKeyIndex.keySet());  
//		Collections.sort(sortedKeys, new MyComparator());
//		sortedPrimaryKey = new Object[sortedKeys.size()];
//		sortedPrimaryKey = sortedKeys.toArray();
//		//		sortedSecondaryIndexes=new HashMap<String,Object[]>();
//
//		/*
//		System.out.println("Sorted Primary Index:");
//		for(Object value: sortedPrimaryKey){
//			System.out.println(value.toString());
//		}*/
//
//		//		for(String secondaryIndexname: secondayIndex.keySet()){
//		//			ArrayList<Object> sortedSecKeys= new ArrayList(secondayIndex.get(secondaryIndexname).keySet());
//		//			Collections.sort(sortedSecKeys, new MyComparator());
//		//			Object[] sortedSecIndex = new Object[sortedSecKeys.size()];
//		//			sortedSecIndex = sortedSecKeys.toArray();
//		//			sortedSecondaryIndexes.put(secondaryIndexname,sortedSecIndex);
//		//			
//		//		}
//
//		/*
//		for(Object secondaryIndexname: sortedSecondaryIndexes.keySet()){
//			System.out.println("Sorted Secondary Index for "+secondaryIndexname);
//			for(Object value: sortedSecondaryIndexes.get(secondaryIndexname)){
//				System.out.println(value.toString());
//			}
//		}*/
//
//
//	}
//
//
//	public static void OrderByModified(){
//		sortedModifiedIndexes=new HashMap<String,Object[]>();
//
//		for(String modifiedIndexname: modifiedIndexes.keySet()){
//			ArrayList<Object> modifiedSecKeys= new ArrayList(modifiedIndexes.get(modifiedIndexes).keySet());
//			Collections.sort(modifiedSecKeys, new MyComparator());
//			Object[] sortedModifiedIndex = new Object[modifiedSecKeys.size()];
//			sortedModifiedIndex = modifiedSecKeys.toArray();
//			//			sortedSecondaryIndexes.put(modifiedIndexname,sortedModifiedIndex);
//		}
//
//		/*
//		for(Object secondaryIndexname: sortedModifiedIndexes.keySet()){
//			//System.out.println("Sorted Secondary Index for "+secondaryIndexname);
//			for(Object value: sortedModifiedIndexes.get(secondaryIndexname)){
//				System.out.println(value.toString());
//			}
//		}*/
//	}
//
//
//
//
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
//}
