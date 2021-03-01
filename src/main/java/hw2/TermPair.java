package hw2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;

/**
 * WritableComparable representing a pair of Strings. The elements in the pair are referred to as
 * the left and right elements. The natural sort order is: first by the left element, and then by
 * the right element.
 */
public class TermPair implements WritableComparable<TermPair> {

    public String left, right;

    /**
     * Creates a pair.
     */
    public TermPair() {
    }

    /**
     * Creates a pair.
     *
     * @param r  the left element
     * @param l the right element
     */
    public TermPair(String l, String r) {
        this.left = l;
        this.right = r;
    }

    /**
     * Deserializes the pair.
     *
     * @param in source for raw byte representation
     */
    public void readFields(DataInput in) throws IOException {
        this.left = Text.readString(in);
        this.right = Text.readString(in);
    }

    /**
     * Serializes this pair.
     *
     * @param out where to write the raw byte representation
     */
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, this.left);
        Text.writeString(out, this.right);
    }

    /**
     * Returns the left element.
     *
     * @return the left element
     */
    public String getLeft() {
        return this.left;
    }

    /**
     * Returns the right element.
     *
     * @return the right element
     */
    public String getRight() {
        return this.right;
    }

    /**
     * Checks two pairs for equality.
     *
     * @param obj object for comparison
     * @return <code>true</code> if <code>obj</code> is equal to this object, <code>false</code>
     * otherwise
     */
    public boolean equals(Object obj) {
        TermPair pair = (TermPair) obj;
        return this.left.equals(pair.left) && this.right.equals(pair.right);
    }

    /**
     * Defines a natural sort order for pairs. Pairs are sorted first by the left element, and then by
     * the right element.
     *
     * @return a value less than zero, a value greater than zero, or zero if this pair should be
     * sorted before, sorted after, or is equal to <code>obj</code>.
     */
    public int compareTo(TermPair pair) {
        String leftyLeft = pair.left;
        String rightyRight = pair.right;

        if (this.left.equals(leftyLeft)) {
            return this.right.compareTo(rightyRight);
        } else {

            return this.left.compareTo(leftyLeft);
        }
    }

    /**
     * Returns a hash code value for the pair.
     *
     * @return hash code for the pair
     */
    public int hashCode() {
        return left.hashCode() + right.hashCode();
    }

    /**
     * Generates human-readable String representation of this pair.
     *
     * @return human-readable String representation of this pair
     */
    public String toString() {
        return "(<" + left + "> <" + right + ">)";
    }

    /**
     * Clones this object.
     *
     * @return clone of this object
     */
    public TermPair clone() {
        return new TermPair(this.left, this.right);
    }

    public void set(String l, String r) {
        this.left = l;
        this.right = r;
    }
}
