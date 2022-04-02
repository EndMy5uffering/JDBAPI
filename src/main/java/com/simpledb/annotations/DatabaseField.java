package com.simpledb.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Collection;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
public @interface DatabaseField {

    public class util{
        public static boolean inSameGroup(Collection<Integer> groups, int[] fieldGroups) {
            for(int i : fieldGroups) {
                if(groups.contains(i)) return true;
            }
            return false;
        }

    }
    
    public String columnName() default "";
	public int[] groups() default 0;

}
