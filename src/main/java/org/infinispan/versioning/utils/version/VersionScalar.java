/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.infinispan.versioning.utils.version;

/**
 *
 * @author Fábio Coelho
 */
public class VersionScalar extends Version{
    
    Long version;
    
    public VersionScalar(Long version){
        super(version);
        this.version=version;
    }
    
}
