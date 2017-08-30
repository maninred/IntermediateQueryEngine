package dubstep;

import net.sf.jsqlparser.expression.PrimitiveValue;

public class TupleFileIdMap{
		int partId;
		PrimitiveValue[] tuple;
		
		TupleFileIdMap(int partId,PrimitiveValue[] tuple){
			this.partId=partId;
			this.tuple=tuple;
		}
		
		
	}
