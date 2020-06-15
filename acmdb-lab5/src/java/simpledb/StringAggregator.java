package simpledb;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int groupByField;
    private Type groupByFieldType;
    private int aggregateField;
    private Op op;

    private TupleDesc tupleDesc;

    private HashMap<Field, Integer> cnt;

    private HashMap<Field, Tuple> ans;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.groupByField = gbfield;
        this.groupByFieldType = gbfieldtype;
        this.aggregateField = afield;
        this.op = what;

        cnt = new HashMap<>();

        if (groupByField == NO_GROUPING) {
            tupleDesc = new TupleDesc(new Type[] {Type.INT_TYPE});
        } else {
            tupleDesc = new TupleDesc(new Type[] {gbfieldtype, Type.INT_TYPE});
        }

        ans = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tuple the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tuple) {
        Field key = null;
        if (groupByField != NO_GROUPING) {
            key = tuple.getField(groupByField);
        }
        String value = ((StringField) tuple.getField(aggregateField)).getValue();

        if (!cnt.containsKey(key)) {
            cnt.put(key, 1);
        } else {
            cnt.put(key, cnt.get(key) + 1);
        }

        Tuple resTuple = new Tuple(tupleDesc);
        IntField ansField = null;

        switch (op) {
            case COUNT:
                ansField = new IntField(cnt.get(key));
                break;
            default:
                throw new NotImplementedException();
        }

        if (groupByField == NO_GROUPING) {
            resTuple.setField(0, ansField);
        } else {
            resTuple.setField(0, key);
            resTuple.setField(1, ansField);
        }

        ans.put(key, resTuple);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        return new TupleIterator(tupleDesc, ans.values());
    }

}
