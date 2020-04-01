package _2_query;

import java.util.ArrayList;
import java.util.List;

import _1_basic.Catalog;
import _1_basic.Database;
import _1_basic.Field;
import _1_basic.HeapFile;
import _1_basic.RelationalOperator;
import _1_basic.Tuple;
import _1_basic.TupleDesc;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;

public class Query {

	private String q;
	private Relation relation;
	
	public Relation getRelation() {
		return relation;
	}

	public Query(String q) {
		this.q = q;
	}
	
	void initRelation(ArrayList<Tuple> l, TupleDesc td) {
		this.relation = new Relation(l, td);
	}
	
	public void print(Object b) {
		System.out.println(b);
	}
	
	public Relation execute()  {
		Statement statement = null;
		try {
			statement = CCJSqlParserUtil.parse(q);
		} catch (JSQLParserException e) {
			System.out.println("Unable to parse query");
			e.printStackTrace();
		}
		Select selectStatement = (Select) statement;
		PlainSelect sb = (PlainSelect)selectStatement.getSelectBody();
		
		
		//your code here
//		System.out.println(statement);
//		System.out.println(sb);
		
		
		Catalog c = Database.getCatalog();
		String tableName = sb.getFromItem().toString();
		int tableId = c.getTableId(tableName);
		TupleDesc td = c.getTupleDesc(tableId);
		HeapFile hf = c.getDbFile(tableId);
		
		List<SelectItem> selectItems = sb.getSelectItems();
		
		Expression where = sb.getWhere();
		initRelation(hf.getAllTuples(), td);
		if (where != null) {
			WhereExpressionVisitor wev = new WhereExpressionVisitor();
			where.accept(wev);
			int left = td.nameToId(wev.getLeft());
			RelationalOperator op = wev.getOp();
			Field right = wev.getRight();
			this.relation = this.relation.select(left, op, right);
		}
		
		if (selectItems.size() >= td.numFields() || (selectItems.get(0).toString().equals("*"))) {
			List<Expression> GBExpression = sb.getGroupByColumnReferences();
			if (GBExpression != null) {
//				initRelation(hf.getAllTuples(), td);
				String string = selectItems.get(1).toString();
				AggregateOperator optemp = null;
				string = string.replaceAll("\\(", "\\.");
				string = string.replaceAll("\\)", "");
				for (AggregateOperator e: AggregateOperator.values()){
					if(e.toString().equals(string.split("\\.")[0])){
						optemp = e;
		                break;
		            }
				}
				this.relation = this.relation.aggregate(optemp, true);
			}
//			else
//				initRelation(hf.getAllTuples(), td);
		}else {
			ArrayList<String> tdNameList = new ArrayList<String>();
			ArrayList<String> SINameList = new ArrayList<String>();
			ArrayList<Integer> indexToAdd = new ArrayList<Integer>();
			for (int i = 0; i < td.numFields(); i++) {
				tdNameList.add(td.getFieldName(i));
			}
			for (int i = 0; i < selectItems.size(); i++) {
				SINameList.add(selectItems.get(i).toString());
			}
			for (int j = 0; j < SINameList.size(); j++) {
				String string = SINameList.get(j);
				AggregateOperator optemp = null;
				if (!tdNameList.contains(string)) {
//					System.out.println(string);
					string = string.replaceAll("\\(", "\\.");
					string = string.replaceAll("\\)", "");
//					System.out.println(string.split("\\.")[0]);
//					System.out.println(string.split("\\.")[1]);
					for (AggregateOperator e: AggregateOperator.values()){
						if(e.toString().equals(string.split("\\.")[0])){
							optemp = e;
			                break;
			            }
					}
					initRelation(hf.getAllTuples(), td);
					indexToAdd.add(1);
					this.relation = this.relation.project(indexToAdd);
					this.relation = this.relation.aggregate(optemp, false);
				}else {
					
					int index = td.nameToId(string);
					indexToAdd.add(index);
//					initRelation(hf.getAllTuples(), td);
					this.relation = this.relation.project(indexToAdd);
				}
			}
			
		}
		
		
		

		List<Join> join = sb.getJoins();
		if (join != null) {
			WhereExpressionVisitor jv = new WhereExpressionVisitor();
			ArrayList<Relation> joinRelations = new ArrayList<Relation>();
//			joinRelations.add(this.getRelation());
			for (int i = 0; i < join.size(); i++) {
				int index = 0;
				join.get(i).getOnExpression().accept(jv);
				joinRelations.add(new Relation(c.getDbFile(c.getTableId(join.get(i).getRightItem().toString())).getAllTuples(), 
						c.getTupleDesc(c.getTableId(join.get(i).getRightItem().toString()))));
				String otherField = jv.getRight().toString();
				TupleDesc rightDesc = c.getTupleDesc(c.getTableId(join.get(i).getRightItem().toString()));
				if (td.contains(jv.getLeft()) && rightDesc.contains(otherField.split("\\.")[1])) {
					this.relation = this.relation.join(joinRelations.get(i), td.nameToId(jv.getLeft()), 
							rightDesc.nameToId(otherField.split("\\.")[1]));
					td = this.getRelation().getDesc();
				}else if (rightDesc.contains(jv.getLeft()) && td.contains(otherField.split("\\.")[1])) {
					this.relation = this.relation.join(joinRelations.get(i), td.nameToId(otherField.split("\\.")[1]) 
							, rightDesc.nameToId(jv.getLeft()));
					td = this.getRelation().getDesc();
				}
				
			}
		}
		
		
		
		
		return getRelation();
//		return null;
		
	}
}
