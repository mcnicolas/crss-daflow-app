package com.pemc.crss.dataflow.app.support;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PageableRequest {

    private int pageNo;

    private int pageSize;

    private List<PageOrder> orderList = new ArrayList<>();

    // use this to set params for filtering
    private Map<String, String> mapParams = new HashMap<>();

    public int getPageNo() {
        return pageNo;
    }

    public void setPageNo(int pageNo) {
        this.pageNo = pageNo;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public List<PageOrder> getOrderList() {
        return orderList;
    }

    public void setOrderList(List<PageOrder> orderList) {
        this.orderList = orderList;
    }

    public Map<String, String> getMapParams() {
        return mapParams;
    }

    public void setMapParams(Map<String, String> mapParams) {
        this.mapParams = mapParams;
    }

    public Pageable getPageable() {
        if (!this.orderList.isEmpty()) {
            List<Sort.Order> orders = orderList.stream().map(PageOrder::getOrder).collect(Collectors.toList());
            return new PageRequest(this.pageNo, this.pageSize, new Sort(orders));
        }

        return new PageRequest(pageNo, pageSize);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("pageNo", pageNo)
                .append("pageSize", pageSize)
                .append("orderList", orderList)
                .append("mapParams", mapParams)
                .toString();
    }
}
