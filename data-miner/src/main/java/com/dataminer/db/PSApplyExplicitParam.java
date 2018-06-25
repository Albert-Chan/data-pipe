package com.dataminer.db;

import java.sql.SQLException;

@FunctionalInterface
public interface PSApplyExplicitParam<PS, E> {
    PS apply(PS ps, E e) throws SQLException;
}


