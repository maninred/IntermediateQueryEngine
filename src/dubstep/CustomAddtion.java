package dubstep;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;

public class CustomAddtion extends Addition{
	
	public CustomAddtion(Expression lhs, Expression rhs){
		super(lhs, rhs);
	}
	
	public Expression getResult(Expression lhs, Expression rhs){
		setLeftExpression(lhs);
		setRightExpression(rhs);
		return this;
	}

}
