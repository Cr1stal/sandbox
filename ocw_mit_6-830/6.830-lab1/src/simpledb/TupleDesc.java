package simpledb;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc {

    private Type[]      typeAr;
    private String[]    fieldAr;

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields
     * fields, with the first td1.numFields coming from td1 and the remaining
     * from td2.
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc combine(TupleDesc td1, TupleDesc td2) {
        Type[] typeAr = new Type[td1.typeAr.length + td2.typeAr.length];
        String[] fieldAr = null;
        System.arraycopy(td1.typeAr, 0, typeAr, 0, td1.typeAr.length);
        System.arraycopy(td2.typeAr, 0, typeAr, td1.typeAr.length, td2.typeAr.length);
        if (td1.fieldAr != null && td2.fieldAr != null) {
            fieldAr = new String[td1.fieldAr.length + td2.fieldAr.length];
            System.arraycopy(td1.fieldAr, 0, fieldAr, 0, td1.fieldAr.length);
            System.arraycopy(td2.fieldAr, 0, fieldAr, td1.fieldAr.length, td2.fieldAr.length);
        }

        return new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        this.typeAr = new Type[typeAr.length];
        this.typeAr = Arrays.copyOf(typeAr, typeAr.length);
        if (fieldAr != null) {
            this.fieldAr = new String[fieldAr.length];
            this.fieldAr = Arrays.copyOf(fieldAr, fieldAr.length);
        }
    }

    /**
     * Constructor.
     * Create a new tuple desc with typeAr.length fields with fields of the
     * specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        this(typeAr, null);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return typeAr.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (fieldAr != null)
            return fieldAr[i];
        else
            return null;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int nameToId(String name) throws NoSuchElementException {
        if (fieldAr != null) {
            for (int i = 0; i < fieldAr.length; i++) {
                if (fieldAr[i].equals(name))
                    return i;
            }
        }
        
        throw new NoSuchElementException();
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getType(int i) throws NoSuchElementException {
        return typeAr[i];
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int size = 0;
        for (Type t : typeAr)
            size += t.getLen();
        return size;
    }

    /**
     * Compares the specified object with this TupleDesc for equality.
     * Two TupleDescs are considered equal if they are the same size and if the
     * n-th type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if (!(o instanceof TupleDesc))
            return false;

        TupleDesc that = (TupleDesc)o;
        if (this.typeAr.length != that.typeAr.length)
            return false;
        for (int i = 0; i < this.typeAr.length; i++) {
            if (this.typeAr[i] != that.typeAr[i])
                return false;
        }

        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < typeAr.length; i++) {
            sb.append(typeAr[i].toString());
            if (fieldAr != null)
                sb.append('(').append(fieldAr[i]).append(')');
            if (i != (typeAr.length - 1))
                sb.append(',');
        }

        return sb.toString();
    }
}
