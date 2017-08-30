package dubstep;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class BuildSchemaDetails extends Main{
	public BuildSchemaDetails() {
		// TODO Auto-generated constructor stub
		buildSchemaPlan();
		}
	
	public static List<ArrayList<String>> buildSchemaPlan(){
		boolean breakwithallColumns = false;
		ListIterator<ArrayList<SelectItem>> schemaListIterator = schemaList.listIterator(schemaList.size());
		//System.out.println("Schema List Iterated" + schemaList.toString());
		workingSchemaList = new ArrayList<ArrayList<String>>();
		ArrayList<String> prevSchema = new ArrayList<String>();
		prevSchema = new ArrayList(getSchemaDetails.keySet());
		//System.out.println("Prev Schema Copied " + prevSchema.toString());
		//schemaListMaster:
		while(schemaListIterator.hasPrevious()){ 
			//System.out.println("Checking the Schema Loaded " + it.next().toString());
			ArrayList<SelectItem> es = (ArrayList<SelectItem>) schemaListIterator.previous();
			//System.out.println("Schema Loaded in loop " + es.toString());
			Iterator<SelectItem> columnsNameIterator = es.iterator();
			ArrayList<String> tempColumnPositionInfo = new ArrayList<String>();
			
			while(columnsNameIterator.hasNext()){
				//System.out.println("Here");
				SelectItem selectItem = (SelectItem) columnsNameIterator.next();
				SelectExpressionItem se = null;
				if(selectItem.toString() != "*"){
					//System.out.println("HERE 1");
					se = (SelectExpressionItem) selectItem;
				}
				
				if(selectItem.toString().equals("*")) {
					//System.out.println("Break Happens");
					breakwithallColumns = true;
					break;
				}
				//System.out.println("SelectItem " + columnsNameIterator.next().toString());	
				else if(se.getAlias() != null){
					//System.out.println("Here 2");
					tempColumnPositionInfo.add(se.getAlias());
						//System.out.println(se.getAlias());
				}
				else if(se.toString().contains(".")){
					Main.splitSchema = se.toString().split("\\.");
					tempColumnPositionInfo.add(Main.splitSchema[1]);	
				}
				else{
					tempColumnPositionInfo.add(selectItem.toString());
						//System.out.println("Added " + selectItem.toString());
					}
				}
			if(breakwithallColumns) {
				//Do Nothing - Retain the old schema
				//System.out.println("Break Happens");
				//System.out.println("Working Schema Copied -Break all Columns " + prevSchema);
				workingSchemaList.add(prevSchema);
				breakwithallColumns = false;
				
			}
			else {
				prevSchema = (ArrayList<String>) tempColumnPositionInfo.clone();
				//System.out.println("Working Schema Copied when Column is present " + tempColumnPositionInfo.toString());
				workingSchemaList.add(tempColumnPositionInfo);
			}		
		}
		
		//System.out.println("Working Schema in Build Schema " + workingSchemaList.toString());
		return workingSchemaList;
	}
}
