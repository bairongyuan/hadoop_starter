package com.kkb.weather;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WeatherGroupCompearator extends WritableComparator {
    public WeatherGroupCompearator() {
        super(WeatherBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        WeatherBean a1 = (WeatherBean) a;
        WeatherBean b1 = (WeatherBean) b;

        int yearCompare = Integer.compare(a1.getYear(), b1.getYear());
        if (yearCompare == 0) {
            return Integer.compare(a1.getMonth(), b1.getMonth());
        }
        return yearCompare;
    }
}
