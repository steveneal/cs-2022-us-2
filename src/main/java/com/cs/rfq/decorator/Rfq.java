package com.cs.rfq.decorator;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.Map;

public class Rfq implements Serializable {
    private String id;
    private String isin;
    private Long traderId;
    private Long entityId;
    private Long quantity;
    private Double price;
    private String side;

    public static Rfq fromJson(String json) {
        //Create a new RFQ data object to return
        Rfq newRfq = new Rfq();

        //Map the Json object to a fields map
        Type t = new TypeToken<Map<String, String>>() {}.getType();
        Map<String, String> fields = new Gson().fromJson(json, t);

        //Get the fields from the fields map and initialize the data in the
        //RFQ object
        newRfq.setId(fields.get("id"));
        newRfq.setTraderId(Long.parseLong(fields.get("traderId")));
        newRfq.setIsin(fields.get("instrumentId"));
        newRfq.setEntityId(Long.parseLong(fields.get("entityId")));
        newRfq.setQuantity(Long.parseLong(fields.get("qty")));
        newRfq.setPrice(Double.parseDouble(fields.get("price")));
        newRfq.setSide(fields.get("side"));

        //Return the RFQ object
        return newRfq;
    }

    @Override
    public String toString() {
        return "Rfq{" +
                "id='" + id + '\'' +
                ", isin='" + isin + '\'' +
                ", traderId=" + traderId +
                ", entityId=" + entityId +
                ", quantity=" + quantity +
                ", price=" + price +
                ", side=" + side +
                '}';
    }

    public boolean isBuySide() {
        return "B".equals(side);
    }

    public boolean isSellSide() {
        return "S".equals(side);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getIsin() {
        return isin;
    }

    public void setIsin(String isin) {
        this.isin = isin;
    }

    public Long getTraderId() {
        return traderId;
    }

    public void setTraderId(Long traderId) {
        this.traderId = traderId;
    }

    public Long getEntityId() {
        return entityId;
    }

    public void setEntityId(Long entityId) {
        this.entityId = entityId;
    }

    public Long getQuantity() {
        return quantity;
    }

    public void setQuantity(Long quantity) {
        this.quantity = quantity;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public String getSide() {
        return side;
    }

    public void setSide(String side) {
        this.side = side;
    }
}
