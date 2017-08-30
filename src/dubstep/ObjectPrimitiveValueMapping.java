package dubstep;


import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.StringValue;

public class ObjectPrimitiveValueMapping {
	
	public PrimitiveValue objectToPrimitive(Object obj){
		PrimitiveValue primitiveValue = null;
		if(obj instanceof DoubleValue){
			primitiveValue = new DoubleValue(Double.parseDouble(obj.toString())); 
		}
		else if(obj instanceof LongValue){
			primitiveValue = new LongValue(Long.parseLong(obj.toString()));
		}
		else if(obj instanceof DateValue){
			primitiveValue = new DateValue(obj.toString());
		}
		else if(obj instanceof StringValue){
			primitiveValue = new StringValue(obj.toString());
		}	
		return primitiveValue;
	}
	
	public Object primitiveToObject(PrimitiveValue primValue) throws InvalidPrimitive{
		Object obj = null;
		if(primValue instanceof LongValue){
			obj = Integer.parseInt(primValue.toString());
		}
		else if(primValue instanceof DoubleValue){
			obj = Double.parseDouble(primValue.toString());
		}
		else if(primValue instanceof DateValue || primValue instanceof StringValue){
			obj = (primValue.toString());
		}
		return obj;
	}
}
