package com.yahoo.ycsb;

import java.io.Serializable;

/**
 * Created by IntelliJ IDEA.
 * User: pedrogomes
 * Date: 1/23/12
 * Time: 4:49 PM
 * To change this template use File | Settings | File Templates.
 */
public class Pair<T1, T2> implements Serializable {
    public T1 left;
    public T2 right;

    public Pair(T1 left, T2 right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public final int hashCode() {
        int hashCode = 31 + (left == null ? 0 : left.hashCode());
        return 31 * hashCode + (right == null ? 0 : right.hashCode());
    }

    public T1 getLeft() {
        return left;
    }

    public T2 getRight() {
        return right;
    }

    public void setLeft(T1 value) {
        left = value;
    }

    public void setRight(T2 value) {
        right = value;
    }



    @Override
    public final boolean equals(Object o) {
        if (!(o instanceof Pair))
            return false;
        Pair that = (Pair) o;
        // handles nulls properly
        return false;//Objects.equal(left, that.left) && Objects.equal(right, that.right);
    }

    @Override
    public String toString() {
        return "Pair(" +
                "left=" + left +
                ", right=" + right +
                ')';
    }

}