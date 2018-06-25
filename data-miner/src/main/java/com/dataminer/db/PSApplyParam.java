package com.dataminer.db;

import java.sql.SQLException;

@FunctionalInterface
public interface PSApplyParam<PS> {
    PS apply(PS ps) throws SQLException;
}


