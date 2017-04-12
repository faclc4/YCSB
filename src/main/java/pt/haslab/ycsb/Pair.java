/*
 * Copyright 2016 by INESC TEC                                               
 * Developed by Fábio Coelho                                                 
 * This work was based on the YCSB Project from Yahoo!                          
 *
 * Licensed under the Apache License, Version 2.0 (the "License");           
 * you may not use this file except in compliance with the License.          
 * You may obtain a copy of the License at                                   
 *
 * http://www.apache.org/licenses/LICENSE-2.0                              
 *
 * Unless required by applicable law or agreed to in writing, software       
 * distributed under the License is distributed on an "AS IS" BASIS,         
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  
 * See the License for the specific language governing permissions and       
 * limitations under the License.                                            
 */
package pt.haslab.ycsb;

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