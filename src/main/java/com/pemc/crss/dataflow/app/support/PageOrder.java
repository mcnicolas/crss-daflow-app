package com.pemc.crss.dataflow.app.support;

import org.springframework.data.domain.Sort;

public class PageOrder {

    private String sortColumn;

    private Sort.Direction sortDirection;

    public String getSortColumn() {
        return sortColumn;
    }

    public void setSortColumn(String sortColumn) {
        this.sortColumn = sortColumn;
    }

    public Sort.Direction getSortDirection() {
        return sortDirection;
    }

    public void setSortDirection(Sort.Direction sortDirection) {
        this.sortDirection = sortDirection;
    }

    public Sort.Order getOrder() {
        return new Sort.Order(this.sortDirection, this.sortColumn, Sort.NullHandling.NULLS_LAST);
    }
}
