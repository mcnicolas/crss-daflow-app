databaseChangeLog {
  changeSet(id: '1495436959264-1', author: 'mlnicolas (generated)') {
    createSequence(sequenceName: 'batch_job_execution_seq')
  }

  changeSet(id: '1495436959264-2', author: 'mlnicolas (generated)') {
    createSequence(sequenceName: 'batch_job_seq')
  }

  changeSet(id: '1495436959264-4', author: 'mlnicolas (generated)') {
    createSequence(sequenceName: 'batch_step_execution_seq')
  }

  changeSet(id: '1495436959264-5', author: 'mlnicolas (generated)') {
    createSequence(sequenceName: 'hibernate_sequence')
  }

  changeSet(id: '1495436959264-6', author: 'mlnicolas (generated)') {
    createSequence(sequenceName: 'task_seq')
  }

  changeSet(id: '1495436959264-9', author: 'mlnicolas (generated)') {
    createTable(tableName: 'batch_job_execution') {
      column(name: 'job_execution_id', type: 'BIGINT') {
        constraints(nullable: false)
      }
      column(name: 'version', type: 'BIGINT')
      column(name: 'job_instance_id', type: 'BIGINT') {
        constraints(nullable: false)
      }
      column(name: 'create_time', type: 'TIMESTAMP(6) WITHOUT TIME ZONE') {
        constraints(nullable: false)
      }
      column(name: 'start_time', type: 'TIMESTAMP(6) WITHOUT TIME ZONE')
      column(name: 'end_time', type: 'TIMESTAMP(6) WITHOUT TIME ZONE')
      column(name: 'status', type: 'VARCHAR(10)')
      column(name: 'exit_code', type: 'VARCHAR(2500)')
      column(name: 'exit_message', type: 'VARCHAR(2500)')
      column(name: 'last_updated', type: 'TIMESTAMP(6) WITHOUT TIME ZONE')
      column(name: 'job_configuration_location', type: 'VARCHAR(2500)')
    }
  }

  changeSet(id: '1495436959264-10', author: 'mlnicolas (generated)') {
    createTable(tableName: 'batch_job_execution_context') {
      column(name: 'job_execution_id', type: 'BIGINT') {
        constraints(nullable: false)
      }
      column(name: 'short_context', type: 'VARCHAR(2500)') {
        constraints(nullable: false)
      }
      column(name: 'serialized_context', type: 'TEXT')
    }
  }

  changeSet(id: '1495436959264-11', author: 'mlnicolas (generated)') {
    createTable(tableName: 'batch_job_execution_params') {
      column(name: 'job_execution_id', type: 'BIGINT') {
        constraints(nullable: false)
      }
      column(name: 'type_cd', type: 'VARCHAR(6)') {
        constraints(nullable: false)
      }
      column(name: 'key_name', type: 'VARCHAR(100)') {
        constraints(nullable: false)
      }
      column(name: 'string_val', type: 'VARCHAR(250)')
      column(name: 'date_val', type: 'TIMESTAMP(6) WITHOUT TIME ZONE')
      column(name: 'long_val', type: 'BIGINT')
      column(name: 'double_val', type: 'FLOAT8')
      column(name: 'identifying', type: 'CHAR(1)') {
        constraints(nullable: false)
      }
    }
  }

  changeSet(id: '1495436959264-12', author: 'mlnicolas (generated)') {
    createTable(tableName: 'batch_job_instance') {
      column(name: 'job_instance_id', type: 'BIGINT') {
        constraints(nullable: false)
      }
      column(name: 'version', type: 'BIGINT')
      column(name: 'job_name', type: 'VARCHAR(100)') {
        constraints(nullable: false)
      }
      column(name: 'job_key', type: 'VARCHAR(32)') {
        constraints(nullable: false)
      }
    }
  }

  changeSet(id: '1495436959264-16', author: 'mlnicolas (generated)') {
    createTable(tableName: 'batch_step_execution') {
      column(name: 'step_execution_id', type: 'BIGINT') {
        constraints(nullable: false)
      }
      column(name: 'version', type: 'BIGINT') {
        constraints(nullable: false)
      }
      column(name: 'step_name', type: 'VARCHAR(100)') {
        constraints(nullable: false)
      }
      column(name: 'job_execution_id', type: 'BIGINT') {
        constraints(nullable: false)
      }
      column(name: 'start_time', type: 'TIMESTAMP(6) WITHOUT TIME ZONE') {
        constraints(nullable: false)
      }
      column(name: 'end_time', type: 'TIMESTAMP(6) WITHOUT TIME ZONE')
      column(name: 'status', type: 'VARCHAR(10)')
      column(name: 'commit_count', type: 'BIGINT')
      column(name: 'read_count', type: 'BIGINT')
      column(name: 'filter_count', type: 'BIGINT')
      column(name: 'write_count', type: 'BIGINT')
      column(name: 'read_skip_count', type: 'BIGINT')
      column(name: 'write_skip_count', type: 'BIGINT')
      column(name: 'process_skip_count', type: 'BIGINT')
      column(name: 'rollback_count', type: 'BIGINT')
      column(name: 'exit_code', type: 'VARCHAR(2500)')
      column(name: 'exit_message', type: 'VARCHAR(2500)')
      column(name: 'last_updated', type: 'TIMESTAMP(6) WITHOUT TIME ZONE')
    }
  }

  changeSet(id: '1495436959264-17', author: 'mlnicolas (generated)') {
    createTable(tableName: 'batch_step_execution_context') {
      column(name: 'step_execution_id', type: 'BIGINT') {
        constraints(nullable: false)
      }
      column(name: 'short_context', type: 'VARCHAR(2500)') {
        constraints(nullable: false)
      }
      column(name: 'serialized_context', type: 'TEXT')
    }
  }

  changeSet(id: '1495436959264-18', author: 'mlnicolas (generated)') {
    createTable(tableName: 'deployment_ids') {
      column(name: 'deployment_key', type: 'VARCHAR(255)') {
        constraints(nullable: false)
      }
      column(name: 'deployment_id', type: 'VARCHAR(255)') {
        constraints(nullable: false)
      }
    }
  }

  changeSet(id: '1495436959264-21', author: 'mlnicolas (generated)') {
    createTable(tableName: 'stream_definitions') {
      column(name: 'definition_name', type: 'VARCHAR(255)') {
        constraints(nullable: false)
      }
      column(name: 'definition', type: 'TEXT')
    }
  }

  changeSet(id: '1495436959264-22', author: 'mlnicolas (generated)') {
    createTable(tableName: 'task_definitions') {
      column(name: 'definition_name', type: 'VARCHAR(255)') {
        constraints(nullable: false)
      }
      column(name: 'definition', type: 'TEXT')
    }
  }

  changeSet(id: '1495436959264-23', author: 'mlnicolas (generated)') {
    createTable(tableName: 'task_execution') {
      column(name: 'task_execution_id', type: 'BIGINT') {
        constraints(nullable: false)
      }
      column(name: 'start_time', type: 'TIMESTAMP(6) WITHOUT TIME ZONE')
      column(name: 'end_time', type: 'TIMESTAMP(6) WITHOUT TIME ZONE')
      column(name: 'task_name', type: 'VARCHAR(100)')
      column(name: 'exit_code', type: 'INT')
      column(name: 'exit_message', type: 'VARCHAR(2500)')
      column(name: 'error_message', type: 'VARCHAR(2500)')
      column(name: 'last_updated', type: 'TIMESTAMP(6) WITHOUT TIME ZONE')
      column(name: 'external_execution_id', type: 'VARCHAR(255)')
    }
  }

  changeSet(id: '1495436959264-24', author: 'mlnicolas (generated)') {
    createTable(tableName: 'task_execution_params') {
      column(name: 'task_execution_id', type: 'BIGINT') {
        constraints(nullable: false)
      }
      column(name: 'task_param', type: 'VARCHAR(2500)')
    }
  }

  changeSet(id: '1495436959264-25', author: 'mlnicolas (generated)') {
    createTable(tableName: 'task_task_batch') {
      column(name: 'task_execution_id', type: 'BIGINT') {
        constraints(nullable: false)
      }
      column(name: 'job_execution_id', type: 'BIGINT') {
        constraints(nullable: false)
      }
    }
  }

  changeSet(id: '1495436959264-26', author: 'mlnicolas (generated)') {
    createTable(tableName: 'uri_registry') {
      column(name: 'name', type: 'VARCHAR(255)') {
        constraints(nullable: false)
      }
      column(name: 'uri', type: 'VARCHAR(255)') {
        constraints(nullable: false)
      }
    }
  }


  changeSet(id: '1495436959264-28', author: 'mlnicolas (generated)') {
    addPrimaryKey(columnNames: 'job_execution_id', constraintName: 'batch_job_execution_context_pkey', tableName: 'batch_job_execution_context')
  }

  changeSet(id: '1495436959264-29', author: 'mlnicolas (generated)') {
    addPrimaryKey(columnNames: 'job_execution_id', constraintName: 'batch_job_execution_pkey', tableName: 'batch_job_execution')
  }

  changeSet(id: '1495436959264-30', author: 'mlnicolas (generated)') {
    addPrimaryKey(columnNames: 'job_instance_id', constraintName: 'batch_job_instance_pkey', tableName: 'batch_job_instance')
  }

  changeSet(id: '1495436959264-33', author: 'mlnicolas (generated)') {
    addPrimaryKey(columnNames: 'step_execution_id', constraintName: 'batch_step_execution_context_pkey', tableName: 'batch_step_execution_context')
  }

  changeSet(id: '1495436959264-34', author: 'mlnicolas (generated)') {
    addPrimaryKey(columnNames: 'step_execution_id', constraintName: 'batch_step_execution_pkey', tableName: 'batch_step_execution')
  }

  changeSet(id: '1495436959264-35', author: 'mlnicolas (generated)') {
    addPrimaryKey(columnNames: 'deployment_key', constraintName: 'deployment_ids_pkey', tableName: 'deployment_ids')
  }

  changeSet(id: '1495436959264-38', author: 'mlnicolas (generated)') {
    addPrimaryKey(columnNames: 'definition_name', constraintName: 'stream_definitions_pkey', tableName: 'stream_definitions')
  }

  changeSet(id: '1495436959264-39', author: 'mlnicolas (generated)') {
    addPrimaryKey(columnNames: 'definition_name', constraintName: 'task_definitions_pkey', tableName: 'task_definitions')
  }

  changeSet(id: '1495436959264-40', author: 'mlnicolas (generated)') {
    addPrimaryKey(columnNames: 'task_execution_id', constraintName: 'task_execution_pkey', tableName: 'task_execution')
  }

  changeSet(id: '1495436959264-41', author: 'mlnicolas (generated)') {
    addPrimaryKey(columnNames: 'name', constraintName: 'uri_registry_pkey', tableName: 'uri_registry')
  }

  changeSet(id: '1495436959264-42', author: 'mlnicolas (generated)') {
    addUniqueConstraint(columnNames: 'job_name, job_key', constraintName: 'job_inst_un', tableName: 'batch_job_instance')
  }

  changeSet(id: '1495436959264-45', author: 'mlnicolas (generated)') {
    addForeignKeyConstraint(baseColumnNames: 'job_execution_id', baseTableName: 'batch_job_execution_context', constraintName: 'job_exec_ctx_fk', deferrable: false, initiallyDeferred: false, onDelete: 'NO ACTION', onUpdate: 'NO ACTION', referencedColumnNames: 'job_execution_id', referencedTableName: 'batch_job_execution')
  }

  changeSet(id: '1495436959264-46', author: 'mlnicolas (generated)') {
    addForeignKeyConstraint(baseColumnNames: 'job_execution_id', baseTableName: 'batch_job_execution_params', constraintName: 'job_exec_params_fk', deferrable: false, initiallyDeferred: false, onDelete: 'NO ACTION', onUpdate: 'NO ACTION', referencedColumnNames: 'job_execution_id', referencedTableName: 'batch_job_execution')
  }

  changeSet(id: '1495436959264-47', author: 'mlnicolas (generated)') {
    addForeignKeyConstraint(baseColumnNames: 'job_execution_id', baseTableName: 'batch_step_execution', constraintName: 'job_exec_step_fk', deferrable: false, initiallyDeferred: false, onDelete: 'NO ACTION', onUpdate: 'NO ACTION', referencedColumnNames: 'job_execution_id', referencedTableName: 'batch_job_execution')
  }

  changeSet(id: '1495436959264-48', author: 'mlnicolas (generated)') {
    addForeignKeyConstraint(baseColumnNames: 'job_instance_id', baseTableName: 'batch_job_execution', constraintName: 'job_inst_exec_fk', deferrable: false, initiallyDeferred: false, onDelete: 'NO ACTION', onUpdate: 'NO ACTION', referencedColumnNames: 'job_instance_id', referencedTableName: 'batch_job_instance')
  }

  changeSet(id: '1495436959264-49', author: 'mlnicolas (generated)') {
    addForeignKeyConstraint(baseColumnNames: 'step_execution_id', baseTableName: 'batch_step_execution_context', constraintName: 'step_exec_ctx_fk', deferrable: false, initiallyDeferred: false, onDelete: 'NO ACTION', onUpdate: 'NO ACTION', referencedColumnNames: 'step_execution_id', referencedTableName: 'batch_step_execution')
  }

  changeSet(id: '1495436959264-50', author: 'mlnicolas (generated)') {
    addForeignKeyConstraint(baseColumnNames: 'task_execution_id', baseTableName: 'task_task_batch', constraintName: 'task_exec_batch_fk', deferrable: false, initiallyDeferred: false, onDelete: 'NO ACTION', onUpdate: 'NO ACTION', referencedColumnNames: 'task_execution_id', referencedTableName: 'task_execution')
  }

  changeSet(id: '1495436959264-51', author: 'mlnicolas (generated)') {
    addForeignKeyConstraint(baseColumnNames: 'task_execution_id', baseTableName: 'task_execution_params', constraintName: 'task_exec_params_fk', deferrable: false, initiallyDeferred: false, onDelete: 'NO ACTION', onUpdate: 'NO ACTION', referencedColumnNames: 'task_execution_id', referencedTableName: 'task_execution')
  }

}
