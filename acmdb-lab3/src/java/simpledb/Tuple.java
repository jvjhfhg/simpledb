package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    private TupleDesc tupleDesc;
    private RecordId recordId;
    private List<Field> fields;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        int numFields = td.numFields();
        this.fields = new ArrayList<>(numFields);
        this.tupleDesc = td;
        for (int i = 0; i < numFields; ++i) {
            fields.add(null);
        }
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        fields.set(i, f);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        return fields.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        int numFields = fields.size();
        for (int i = 0; i < numFields; ++i) {
            stringBuilder.append(tupleDesc.getFieldName(i));
            if (i == numFields - 1) {
                stringBuilder.append('\n');
            } else {
                stringBuilder.append('\t');
            }
        }
        for (int i = 0; i < numFields; ++i) {
            stringBuilder.append(fields.get(i).toString());
            if (i == numFields - 1) {
                stringBuilder.append('\n');
            } else {
                stringBuilder.append('\t');
            }
        }
        return stringBuilder.toString();
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields() {
        return fields.iterator();
    }

    /**
     * reset the TupleDesc of thi tuple
     * */
    public void resetTupleDesc(TupleDesc td) {
        tupleDesc = td;
        fields.clear();
        int numFields = td.numFields();
        for (int i = 0; i < numFields; ++i) {
            fields.add(null);
        }
    }

    public boolean equals(Object o) {
        if (!(o instanceof Tuple)) {
            return false;
        }
        if (tupleDesc == null) {
            if (((Tuple) o).tupleDesc != null) {
                return false;
            }
        } else if (!tupleDesc.equals(((Tuple) o).tupleDesc)) {
            return false;
        }
        if (recordId == null) {
            if (((Tuple) o).recordId != null) {
                return false;
            }
        } else if (!recordId.equals(((Tuple) o).recordId)) {
            return false;
        }
        if (fields.size() != ((Tuple) o).fields.size()) {
            return false;
        }
        for (int i = 0; i < fields.size(); ++i) {
            if (!fields.get(i).equals(((Tuple) o).fields.get(i))) {
                return false;
            }
        }
        return true;
    }
}
