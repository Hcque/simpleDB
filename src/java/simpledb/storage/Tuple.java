package simpledb.storage;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.*;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    // add new data fields
    private RecordId recordId;
    private TupleDesc tupleDesc;
    private Field fields_[];
    private int len_;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        tupleDesc = td;
        len_ = getTupleDesc().numFields();
        fields_ = new Field[len_];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
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
        // some code goes here
        fields_[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        return fields_[i];
    }

    public static Tuple concat(Tuple t1, Tuple t2)
    {
        TupleDesc merged_td = TupleDesc.merge(t1.getTupleDesc(), t2.getTupleDesc());
        Tuple ans = new Tuple( merged_td );
        int i = 0;
        for ( i = 0; i < t1.len_; i ++ )
        {
            ans.setField(i, t1.getField(i));
        }
        for (; i < t1.len_ + t2.len_ ; i ++ )
        {
            ans.setField(i, t2.getField(i - t1.len_ ));
        }
        return ans;
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
        // some code goes here
        // throw new UnsupportedOperationException("Implement this");
        String ans = "";
        for (int i = 0; i < getTupleDesc().numFields(); i ++ )
        {
            ans += "" + fields_[i] + "\t";
        }
        return ans;
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return new Iterator<Field>() 
        {
            int index_ = 0;
            @Override 
            public boolean hasNext()
            {
                return index_ < fields_.length; 
            }

            @Override 
            public Field next()
            {
                if (hasNext())
                {
                    Field field = fields_[index_++];
                    return field;
                }
                throw new NoSuchElementException();
            }
            @Override 
            public void remove()
            {
                throw new UnsupportedOperationException();
            }

        };
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here
        tupleDesc = td;
    }
}
