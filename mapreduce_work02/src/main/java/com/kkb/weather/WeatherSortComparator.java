package com.kkb.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WeatherSortComparator extends WritableComparator {

    public WeatherSortComparator() {
        super(WeatherBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        WeatherBean a1 = (WeatherBean) a;
        WeatherBean b1 = (WeatherBean) b;

        int yearCompare = Integer.compare(a1.getYear(), b1.getYear());
        if (yearCompare == 0) {
            int monthCompare = Integer.compare(a1.getMonth(), b1.getMonth());
            if (monthCompare == 0) {
                return -Integer.compare(a1.getWd(), b1.getWd());
            }
            return monthCompare;
        }
        return yearCompare;
    }
}
