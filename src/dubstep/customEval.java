package dubstep;
import java.sql.SQLException;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

public class customEval extends Eval {
	int pos;
	String columnValue;
	String dataType;
	@Override
	public PrimitiveValue eval(Column column) throws SQLException {
		
		String columnName = column.toString();
		/*
		if(columnName.contains(".")){
			String[] columnNames = columnName.split("\\.");
			columnName = columnNames[columnNames.length-1];
		}*/
		
//		PrimitiveValue currentColumnValue = null;
		//System.out.println("Column Name Passed " + columnName);
		//System.out.println("Working Data Schema " + Main.workingDataSchema.toString());
//		if(Main.workingDataSchema.contains(columnName)){
//			pos = Main.workingDataSchema.indexOf(columnName);
			//System.out.println("Position of the Schema in Eval " + pos);
			//System.out.println("Print Position of the Schema " + pos);
			//pos = new ArrayList<String>(Main.getSchemaDetails.keySet()).indexOf(column.toString());
			//System.out.println("Current Working Record " + Main.workingRecord.toString());
			//System.out.println("Current Data " + Main.workingRecord.get(pos));
//			currentColumnValue = Main.workingRecord[pos];
//		}
		//System.out.println("Value in Eval " + currentColumnValue.toString());
		return Main.workingRecordMapper.get(columnName);
	}
	
//	public PrimitiveValue eval(Expression e){
//		super(e);
//	}
	public static void setWorkingTuple(PrimitiveValue[] currentTuple) throws SQLException {
		Main.workingRecord = currentTuple;
		for(int i=0; i < Main.workingRecord.length; i++){
			Main.workingRecordMapper.put(Main.workingDataSchema.get(i), Main.workingRecord[i]);
		}
		//System.out.println("Main Working Schema " + Main.workingDataSchema.toString());
		//System.out.println("Working Record Set " + Main.workingRecord);
	}
	
}
