package dubstep;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;

import net.sf.jsqlparser.expression.PrimitiveValue;

public class SelectWhere extends Main {
	public static SelectQuery selectQueryexecute;
	static Iterator outerit;
	SelectWhere() throws SQLException, IOException{
		whereClauseEvaluator();
	}
	public static void whereClauseEvaluator() throws SQLException, IOException{
		PrimitiveValue result;
//		LinkedHashMap<String, String> getSchemaDetails;
//		List<ArrayList<PrimitiveValue>> tempWorkingData;
//		getSchemaDetails = (LinkedHashMap<String, String>) tableDetails.get(tableName);
		//workingTuple = tupleRecord.toArray(new PrimitiveValue[0]);
		//System.out.println("Current Tuple " + tupleRecord.toString());
		result = expressionEvaluator.eval(whereclauseExpression);
		//System.out.println(whereclauseExpression.toString());
		//if(result!=null){
			if(result.toBool() == true){
				bufferFromExternalClass = true;
				avgCount++;
				selectQueryexecute = new SelectQuery();
				//itemIndex++;
			}
			else{
				//Discard the tuple
				//System.out.println("Failure Happens!");
				whereCondition = false;
				if(lastRecord && orderPresent) {
					FileSplitandMerge.lastRecordsortandFlush();
				}
			}
		//}
		//SelectQuery.itemIndex = 0;
		bufferFromExternalClass = false;
	}
}
