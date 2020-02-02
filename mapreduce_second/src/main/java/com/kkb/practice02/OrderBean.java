package com.kkb.practice02;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {
    private String userId;
    private String dateTime;
    private String title;
    private double unitPrice;
    private int purchaseNum;
    private String produceId;

    @Override
    public String toString() {
        return "userId='" + userId + '\'' +
                ", dateTime='" + dateTime + '\'' +
                ", title='" + title + '\'' +
                ", unitPrice=" + unitPrice +
                ", purchaseNum=" + purchaseNum +
                ", produceId='" + produceId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getDateTime() {
        return dateTime;
    }

    public void setDateTime(String dateTime) {
        this.dateTime = dateTime;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public double getUnitPrice() {
        return unitPrice;
    }

    public void setUnitPrice(double unitPrice) {
        this.unitPrice = unitPrice;
    }

    public int getPurchaseNum() {
        return purchaseNum;
    }

    public void setPurchaseNum(int purchaseNum) {
        this.purchaseNum = purchaseNum;
    }

    public String getProduceId() {
        return produceId;
    }

    public void setProduceId(String produceId) {
        this.produceId = produceId;
    }

    @Override
    public int compareTo(OrderBean o) {
        int compareUserID = this.userId.compareTo(o.userId);
        if (compareUserID == 0) {
            int compareDateTime = this.dateTime.compareTo(o.dateTime);
            if (compareDateTime == 0) {
                double thisTotlePrice = this.unitPrice * this.purchaseNum;
                double otherTotlePrice = o.unitPrice * o.purchaseNum;
                return -Double.compare(thisTotlePrice, otherTotlePrice);
            }
            return compareDateTime;
        }
        return compareUserID;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(userId);
        out.writeUTF(dateTime);
        out.writeUTF(title);
        out.writeDouble(unitPrice);
        out.writeInt(purchaseNum);
        out.writeUTF(produceId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.userId = in.readUTF();
        this.dateTime = in.readUTF();
        this.title = in.readUTF();
        this.unitPrice = in.readDouble();
        this.purchaseNum = in.readInt();
        this.produceId = in.readUTF();
    }
}
