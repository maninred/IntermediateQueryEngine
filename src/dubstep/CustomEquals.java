package dubstep;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;

public class CustomEquals extends EqualsTo{
	
	public CustomEquals(Expression lhs, Expression rhs){
		super(lhs, rhs);
	}
	
	public Expression getResult(Expression lhs, Expression rhs){
		setLeftExpression(lhs);
		setRightExpression(rhs);
		return this;
	}
}
