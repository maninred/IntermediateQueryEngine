package dubstep;

import net.sf.jsqlparser.expression.PrimitiveValue;

public class indexObject {
	public PrimitiveValue secondaryIndexValue;
	public PrimitiveValue primaryIndexValue;
	
	indexObject(PrimitiveValue secondaryIndexValue,PrimitiveValue primaryIndexValue){
		this.secondaryIndexValue=secondaryIndexValue;
		this.primaryIndexValue=primaryIndexValue;
	}
	
	
}
