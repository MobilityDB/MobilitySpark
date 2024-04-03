package utils;

import types.basic.tpoint.TPoint;
import types.temporal.TInstant;

import java.util.Comparator;

public class TInstComparator implements Comparator<TInstant>{
    @Override
    public int compare(TInstant a, TInstant b){
        return a.start_timestamp().compareTo(b.start_timestamp());
    }
}