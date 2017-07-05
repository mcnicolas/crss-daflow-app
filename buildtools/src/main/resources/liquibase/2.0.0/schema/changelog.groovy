databaseChangeLog {
  changeSet(id: '1498377817196-1', author: 'jdimayuga (generated)') {
    createSequence(sequenceName: 'batch_job_skip_logs_seq')
  }

  changeSet(id: '1498377817196-2', author: 'jdimayuga (generated)') {
    createSequence(sequenceName: 'hibernate_sequence')
  }

  changeSet(id: '1498377817196-3', author: 'jdimayuga (generated)') {
    createTable(tableName: 'LATEST_ADJUSTMENT_LOCK') {
      column(name: 'id', type: 'BIGINT', autoIncrement: true) {
        constraints(primaryKey: true, primaryKeyName: 'LATEST_ADJUSTMENT_LOCKPK')
      }
      column(name: 'end_date', type: 'TIMESTAMP WITHOUT TIME ZONE') {
        constraints(nullable: false)
      }
      column(name: 'group_id', type: 'BIGINT') {
        constraints(nullable: false)
      }
      column(name: 'locked', type: 'BOOLEAN')
      column(name: 'parent_job_id', type: 'BIGINT') {
        constraints(nullable: false)
      }
      column(name: 'start_date', type: 'TIMESTAMP WITHOUT TIME ZONE') {
        constraints(nullable: false)
      }
    }
  }

  changeSet(id: '1498377817196-4', author: 'jdimayuga (generated)') {
    createTable(tableName: 'RUNNING_ADJUSTMENT_LOCK') {
      column(name: 'id', type: 'BIGINT', autoIncrement: true) {
        constraints(primaryKey: true, primaryKeyName: 'RUNNING_ADJUSTMENT_LOCKPK')
      }
      column(name: 'end_date', type: 'TIMESTAMP WITHOUT TIME ZONE') {
        constraints(nullable: false)
      }
      column(name: 'group_id', type: 'BIGINT') {
        constraints(nullable: false)
      }
      column(name: 'locked', type: 'BOOLEAN')
      column(name: 'parent_job_id', type: 'BIGINT') {
        constraints(nullable: false)
      }
      column(name: 'start_date', type: 'TIMESTAMP WITHOUT TIME ZONE') {
        constraints(nullable: false)
      }
    }
  }

  changeSet(id: '1498377817196-5', author: 'jdimayuga (generated)') {
    createTable(tableName: 'addtl_comp_params') {
      column(name: 'id', type: 'BIGINT', autoIncrement: true) {
        constraints(primaryKey: true, primaryKeyName: 'addtl_comp_paramsPK')
      }
      column(name: 'approved_rate', type: 'numeric(19, 2)') {
        constraints(nullable: false)
      }
      column(name: 'BILLING_END_DATE', type: 'TIMESTAMP WITHOUT TIME ZONE') {
        constraints(nullable: false)
      }
      column(name: 'billing_id', type: 'VARCHAR(255)') {
        constraints(nullable: false)
      }
      column(name: 'BILLING_START_DATE', type: 'TIMESTAMP WITHOUT TIME ZONE') {
        constraints(nullable: false)
      }
      column(name: 'group_id', type: 'VARCHAR(255)') {
        constraints(nullable: false)
      }
      column(name: 'job_id', type: 'BIGINT')
      column(name: 'mtn', type: 'VARCHAR(255)') {
        constraints(nullable: false)
      }
      column(name: 'pricing_condition', type: 'VARCHAR(255)') {
        constraints(nullable: false)
      }
      column(name: 'status', type: 'VARCHAR(255)')
    }
  }

  changeSet(id: '1498377817196-6', author: 'jdimayuga (generated)') {
    createTable(tableName: 'batch_job_addtl_params') {
      column(name: 'id', type: 'BIGINT', autoIncrement: true) {
        constraints(primaryKey: true, primaryKeyName: 'batch_job_addtl_paramsPK')
      }
      column(name: 'created_datetime', type: 'TIMESTAMP(6) WITHOUT TIME ZONE')
      column(name: 'date_val', type: 'TIMESTAMP WITHOUT TIME ZONE')
      column(name: 'double_val', type: 'FLOAT8')
      column(name: 'key_name', type: 'VARCHAR(255)') {
        constraints(nullable: false)
      }
      column(name: 'long_val', type: 'BIGINT')
      column(name: 'run_id', type: 'BIGINT') {
        constraints(nullable: false)
      }
      column(name: 'string_val', type: 'VARCHAR(255)')
      column(name: 'type_cd', type: 'VARCHAR(255)')
    }
  }

  changeSet(id: '1498377817196-7', author: 'jdimayuga (generated)') {
    createTable(tableName: 'batch_job_adj_run') {
      column(name: 'id', type: 'BIGINT', autoIncrement: true) {
        constraints(primaryKey: true, primaryKeyName: 'batch_job_adj_runPK')
      }
      column(name: 'created_datetime', type: 'TIMESTAMP(6) WITHOUT TIME ZONE')
      column(name: 'ADDTL_COMP', type: 'CHAR(1)') {
        constraints(nullable: false)
      }
      column(name: 'BILLING_PERIOD_END', type: 'TIMESTAMP WITHOUT TIME ZONE') {
        constraints(nullable: false)
      }
      column(name: 'BILLING_PERIOD_START', type: 'TIMESTAMP WITHOUT TIME ZONE') {
        constraints(nullable: false)
      }
      column(name: 'group_id', type: 'VARCHAR(255)') {
        constraints(nullable: false)
      }
      column(name: 'job_id', type: 'VARCHAR(255)') {
        constraints(nullable: false)
      }
      column(name: 'METER_PROCESS_TYPE', type: 'VARCHAR(50)') {
        constraints(nullable: false)
      }
      column(name: 'OUTPUT_READY', type: 'CHAR(1)') {
        constraints(nullable: false)
      }
      column(name: 'OUTPUT_READY_DATETIME', type: 'TIMESTAMP WITHOUT TIME ZONE')
    }
  }

  changeSet(id: '1498377817196-8', author: 'jdimayuga (generated)') {
    createTable(tableName: 'batch_job_retry_attempt') {
      column(name: 'job_execution_id', type: 'BIGINT') {
        constraints(nullable: false)
      }
      column(name: 'retry_attempt', type: 'INT')
    }
  }

  changeSet(id: '1498377817196-9', author: 'jdimayuga (generated)') {
    createTable(tableName: 'batch_job_run_lock') {
      column(name: 'id', type: 'BIGINT', autoIncrement: true) {
        constraints(primaryKey: true, primaryKeyName: 'batch_job_run_lockPK')
      }
      column(name: 'created_datetime', type: 'TIMESTAMP(6) WITHOUT TIME ZONE')
      column(name: 'job_name', type: 'VARCHAR(255)')
      column(name: 'locked', type: 'BOOLEAN')
      column(name: 'locked_date', type: 'TIMESTAMP WITHOUT TIME ZONE')
    }
  }

  changeSet(id: '1498377817196-10', author: 'jdimayuga (generated)') {
    createTable(tableName: 'batch_job_skip_logs') {
      column(name: 'id', type: 'BIGINT') {
        constraints(nullable: false)
      }
      column(name: 'details', type: 'VARCHAR(255)')
      column(name: 'error', type: 'VARCHAR(255)')
      column(name: 'error_code', type: 'INT')
      column(name: 'step_id', type: 'BIGINT')
      column(name: 'step_name', type: 'VARCHAR(255)')
    }
  }

  changeSet(id: '1498377817196-11', author: 'jdimayuga (generated)') {
    addPrimaryKey(columnNames: 'job_execution_id', constraintName: 'batch_job_retry_attemptPK', tableName: 'batch_job_retry_attempt')
  }

  changeSet(id: '1498377817196-12', author: 'jdimayuga (generated)') {
    addPrimaryKey(columnNames: 'id', constraintName: 'batch_job_skip_logsPK', tableName: 'batch_job_skip_logs')
  }

  changeSet(id: '1498377817196-13', author: 'jdimayuga (generated)') {
    addUniqueConstraint(columnNames: 'run_id, key_name', constraintName: 'uk_batch_job_addtl_params', tableName: 'batch_job_addtl_params')
  }

  changeSet(id: '1498377817196-14', author: 'jdimayuga (generated)') {
    addUniqueConstraint(columnNames: 'group_id', constraintName: 'uk_batch_job_adj_run', tableName: 'batch_job_adj_run')
  }

  changeSet(id: '1498464836417-1', author: 'dmendoza (generated)') {
    addColumn(tableName: 'batch_job_skip_logs') {
      column(name: 'parent_step_name', type: 'VARCHAR(255)')
    }
  }

  changeSet(id: '1498464836417-2', author: 'dmendoza (generated)') {
    addColumn(tableName: 'batch_job_skip_logs') {
      column(name: 'job_execution_id', type: 'BIGINT')
    }
  }

  changeSet(id: '1498629417240-1', author: 'dmendoza (generated)') {
    modifyDataType(tableName: 'batch_job_skip_logs', columnName: 'error', newDataType: 'VARCHAR(4000)')
  }

  changeSet(id: '1499162854493-1', author: 'dmendoza (generated)') {
    dropNotNullConstraint(tableName: 'batch_job_adj_run', columnName: 'job_id')
  }


}
