package simpledb;

public class QueryPlans {

	public QueryPlans(){
	}

	//SELECT * FROM T1, T2 WHERE T1.column0 = T2.column0;
	public Operator queryOne(DbIterator t1, DbIterator t2) {
		// IMPLEMENT ME
		if (t1 != null && t2 != null)
		{
			return new Join(new JoinPredicate(0,Predicate.Op.EQUALS,0),t1,t2);
		}
		return null;
	}

	//SELECT * FROM T1, T2 WHERE T1. column0 > 1 AND T1.column1 = T2.column1;
	public Operator queryTwo(DbIterator t1, DbIterator t2) {
		// IMPLEMENT ME
		if (t1 != null && t2 != null)
		{
			Operator joined_table = new Join(new JoinPredicate(1,Predicate.Op.EQUALS,1),t1,t2);
			return new Filter(new Predicate(0,Predicate.Op.GREATER_THAN,new IntField(1)),joined_table);
		}
		return null;
	}

	//SELECT column0, MAX(column1) FROM T1 WHERE column2 > 1 GROUP BY column0;
	public Operator queryThree(DbIterator t1) {
		// IMPLEMENT ME
		if (t1 != null)
		{
			Operator filtered_table = new Filter(new Predicate(2,Predicate.Op.GREATER_THAN,new IntField(1)),t1);
			return new Aggregate(filtered_table,1,0,Aggregator.Op.MAX);
		}
		return null;
	}

	// SELECT ​​* FROM T1, T2
	// WHERE T1.column0 < (SELECT COUNT(*​​) FROM T3)
	// AND T2.column0 = (SELECT AVG(column0) FROM T3)
	// AND T1.column1 >= T2. column1
	// ORDER BY T1.column0 DESC;
	public Operator queryFour(DbIterator t1, DbIterator t2, DbIterator t3) throws TransactionAbortedException, DbException {
		// IMPLEMENT ME
		if (t1 != null && t2 != null && t3 != null)
		{
			int t1_ncol = t1.getTupleDesc().numFields();

			Aggregate t3_C = new Aggregate(t3,0,-1,Aggregator.Op.COUNT);
			Aggregate t3_A = new Aggregate(t3,0,-1,Aggregator.Op.AVG);
			Field t3_count = t3_C.fetchNext().getField(0);
			t3_C.rewind();
			Field t3_avg = t3_A.fetchNext().getField(0);

			Operator j_table = new Join(new JoinPredicate(1,Predicate.Op.GREATER_THAN_OR_EQ,1),t1,t2);
			Operator f1_table = new Filter(new Predicate(0,Predicate.Op.LESS_THAN,t3_count),j_table);
			Operator f2_table = new Filter(new Predicate(t1_ncol,Predicate.Op.EQUALS,t3_avg),f1_table);

			return new OrderBy(0,false,f2_table);
		}
		return null;
	}


}