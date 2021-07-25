package com.yc.mr10.p3;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class City implements WritableComparable {
    private String city;
    private String country;

    public String getCity() {
        return city;
    }

    public String getCountry() {
        return country;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        //读的顺序要按照写的顺序来
        this.city= in.readLine();
        this.country=in.readLine();

    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBytes(city);
        out.writeBytes(country);

    }



    @Override
    public int compareTo(Object o) {
        return 0;
    }
}
