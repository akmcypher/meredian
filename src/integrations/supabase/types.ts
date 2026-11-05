
export type Json = string | number | boolean | null | { [key: string]: Json | undefined } | Json[]

export type Database = {
  
  "public": {
          Tables: {
            "access_policies": {
                  Row: {
                    "allowed_roles": (Database["public"]['Enums']["app_role"])[],"classification": string,"column_name": string,"created_at": string,"dataset": string,"id": string,"masking": string,"organization_id": string | null
                  }
                  ComputedFields: never
                  Insert: {
                    "allowed_roles"?: (Database["public"]['Enums']["app_role"])[],"classification"?: string,"column_name": string,"created_at"?: string,"dataset": string,"id"?: string,"masking"?: string,"organization_id"?: string | null
                  }
                  Update: {
                    "allowed_roles"?: (Database["public"]['Enums']["app_role"])[],"classification"?: string,"column_name"?: string,"created_at"?: string,"dataset"?: string,"id"?: string,"masking"?: string,"organization_id"?: string | null
                  }
                  Relationships: [
                    {
      foreignKeyName: "access_policies_organization_id_fkey"
      columns: ["organization_id"]
isOneToOne: false
      referencedRelation: "organizations"
      referencedColumns: ["id"]
    }
                  ]
                },"admin_audit": {
                  Row: {
                    "action": string,"actor": string | null,"created_at": string,"detail": NonNullable<Json>,"hash": string | null,"hash_alg": string,"id": number,"prev_hash": string | null,"target": string | null
                  }
                  ComputedFields: never
                  Insert: {
                    "action": string,"actor"?: string | null,"created_at"?: string,"detail"?: NonNullable<Json>,"hash"?: string | null,"hash_alg"?: string,"id"?: number,"prev_hash"?: string | null,"target"?: string | null
                  }
                  Update: {
                    "action"?: string,"actor"?: string | null,"created_at"?: string,"detail"?: NonNullable<Json>,"hash"?: string | null,"hash_alg"?: string,"id"?: number,"prev_hash"?: string | null,"target"?: string | null
                  }
                  Relationships: [
                    
                  ]
                },"alert_settings": {
                  Row: {
                    "alert_email": string | null,"notify_approvals": boolean,"notify_job_failures": boolean,"updated_at": string,"user_id": string
                  }
                  ComputedFields: never
                  Insert: {
                    "alert_email"?: string | null,"notify_approvals"?: boolean,"notify_job_failures"?: boolean,"updated_at"?: string,"user_id": string
                  }
                  Update: {
                    "alert_email"?: string | null,"notify_approvals"?: boolean,"notify_job_failures"?: boolean,"updated_at"?: string,"user_id"?: string
                  }
                  Relationships: [
                    
                  ]
                },"audit_log": {
                  Row: {
                    "action": string,"actor_email": string | null,"actor_id": string,"created_at": string,"details": NonNullable<Json>,"id": string,"target": string | null
                  }
                  ComputedFields: never
                  Insert: {
                    "action": string,"actor_email"?: string | null,"actor_id": string,"created_at"?: string,"details"?: NonNullable<Json>,"id"?: string,"target"?: string | null
                  }
                  Update: {
                    "action"?: string,"actor_email"?: string | null,"actor_id"?: string,"created_at"?: string,"details"?: NonNullable<Json>,"id"?: string,"target"?: string | null
                  }
                  Relationships: [
                    
                  ]
                },"change_requests": {
                  Row: {
                    "code": string,"created_at": string,"description": string,"id": string,"language": string,"review_note": string | null,"reviewed_at": string | null,"reviewed_by": string | null,"reviewer_email": string | null,"status": string,"submitted_by": string,"submitter_email": string | null,"target": string,"title": string,"validation": NonNullable<Json>
                  }
                  ComputedFields: never
                  Insert: {
                    "code": string,"created_at"?: string,"description"?: string,"id"?: string,"language"?: string,"review_note"?: string | null,"reviewed_at"?: string | null,"reviewed_by"?: string | null,"reviewer_email"?: string | null,"status"?: string,"submitted_by": string,"submitter_email"?: string | null,"target": string,"title": string,"validation"?: NonNullable<Json>
                  }
                  Update: {
                    "code"?: string,"created_at"?: string,"description"?: string,"id"?: string,"language"?: string,"review_note"?: string | null,"reviewed_at"?: string | null,"reviewed_by"?: string | null,"reviewer_email"?: string | null,"status"?: string,"submitted_by"?: string,"submitter_email"?: string | null,"target"?: string,"title"?: string,"validation"?: NonNullable<Json>
                  }
                  Relationships: [
                    
                  ]
                },"connect_attempts": {
                  Row: {
                    "attempt": number,"error_code": string | null,"finished_at": string | null,"id": string,"run_id": string,"started_at": string,"status": string,"token": string
                  }
                  ComputedFields: never
                  Insert: {
                    "attempt": number,"error_code"?: string | null,"finished_at"?: string | null,"id"?: string,"run_id": string,"started_at"?: string,"status"?: string,"token": string
                  }
                  Update: {
                    "attempt"?: number,"error_code"?: string | null,"finished_at"?: string | null,"id"?: string,"run_id"?: string,"started_at"?: string,"status"?: string,"token"?: string
                  }
                  Relationships: [
                    {
      foreignKeyName: "connect_attempts_run_id_fkey"
      columns: ["run_id"]
isOneToOne: false
      referencedRelation: "connect_runs"
      referencedColumns: ["id"]
    }
                  ]
                },"connect_commits": {
                  Row: {
                    "batch_id": string,"byte_count": number,"checkpoint": NonNullable<Json>,"created_at": string,"id": string,"manifest_key": string,"row_count": number,"run_id": string,"sequence": number,"sha256": string,"stream_id": string
                  }
                  ComputedFields: never
                  Insert: {
                    "batch_id": string,"byte_count": number,"checkpoint": NonNullable<Json>,"created_at"?: string,"id"?: string,"manifest_key": string,"row_count": number,"run_id": string,"sequence": number,"sha256": string,"stream_id": string
                  }
                  Update: {
                    "batch_id"?: string,"byte_count"?: number,"checkpoint"?: NonNullable<Json>,"created_at"?: string,"id"?: string,"manifest_key"?: string,"row_count"?: number,"run_id"?: string,"sequence"?: number,"sha256"?: string,"stream_id"?: string
                  }
                  Relationships: [
                    {
      foreignKeyName: "connect_commits_batch_id_fkey"
      columns: ["batch_id"]
isOneToOne: false
      referencedRelation: "ingest_landing_batches"
      referencedColumns: ["id"]
    },{
      foreignKeyName: "connect_commits_run_id_fkey"
      columns: ["run_id"]
isOneToOne: false
      referencedRelation: "connect_runs"
      referencedColumns: ["id"]
    },{
      foreignKeyName: "connect_commits_stream_id_fkey"
      columns: ["stream_id"]
isOneToOne: false
      referencedRelation: "connect_streams"
      referencedColumns: ["id"]
    }
                  ]
                },"connect_connections": {
                  Row: {
                    "created_at": string,"credential_ref": string,"dead_letter_budget": number,"destination_version": string,"discovered_catalog": NonNullable<Json>,"enabled": boolean,"health": string,"id": string,"interval_minutes": number,"name": string,"next_run_at": string | null,"operation_timeout_seconds": number,"organization_id": string | null,"owner_id": string,"source_id": string,"source_version": string
                  }
                  ComputedFields: never
                  Insert: {
                    "created_at"?: string,"credential_ref": string,"dead_letter_budget"?: number,"destination_version"?: string,"discovered_catalog"?: NonNullable<Json>,"enabled"?: boolean,"health"?: string,"id"?: string,"interval_minutes"?: number,"name": string,"next_run_at"?: string | null,"operation_timeout_seconds"?: number,"organization_id"?: string | null,"owner_id": string,"source_id": string,"source_version"?: string
                  }
                  Update: {
                    "created_at"?: string,"credential_ref"?: string,"dead_letter_budget"?: number,"destination_version"?: string,"discovered_catalog"?: NonNullable<Json>,"enabled"?: boolean,"health"?: string,"id"?: string,"interval_minutes"?: number,"name"?: string,"next_run_at"?: string | null,"operation_timeout_seconds"?: number,"organization_id"?: string | null,"owner_id"?: string,"source_id"?: string,"source_version"?: string
                  }
                  Relationships: [
                    {
      foreignKeyName: "connect_connections_organization_id_fkey"
      columns: ["organization_id"]
isOneToOne: false
      referencedRelation: "organizations"
      referencedColumns: ["id"]
    },{
      foreignKeyName: "connect_connections_source_id_fkey"
      columns: ["source_id"]
isOneToOne: true
      referencedRelation: "ingest_sources"
      referencedColumns: ["id"]
    }
                  ]
                },"connect_dead_letters": {
                  Row: {
                    "connection_id": string,"created_at": string,"error_code": string,"id": string,"line": number,"object_key": string,"position": string,"replay_run_id": string | null,"resolved_at": string | null,"run_id": string,"sequence": number,"status": string,"stream_id": string
                  }
                  ComputedFields: never
                  Insert: {
                    "connection_id": string,"created_at"?: string,"error_code": string,"id"?: string,"line": number,"object_key": string,"position": string,"replay_run_id"?: string | null,"resolved_at"?: string | null,"run_id": string,"sequence": number,"status"?: string,"stream_id": string
                  }
                  Update: {
                    "connection_id"?: string,"created_at"?: string,"error_code"?: string,"id"?: string,"line"?: number,"object_key"?: string,"position"?: string,"replay_run_id"?: string | null,"resolved_at"?: string | null,"run_id"?: string,"sequence"?: number,"status"?: string,"stream_id"?: string
                  }
                  Relationships: [
                    {
      foreignKeyName: "connect_dead_letters_connection_id_fkey"
      columns: ["connection_id"]
isOneToOne: false
      referencedRelation: "connect_connections"
      referencedColumns: ["id"]
    },{
      foreignKeyName: "connect_dead_letters_replay_run_id_fkey"
      columns: ["replay_run_id"]
isOneToOne: false
      referencedRelation: "connect_runs"
      referencedColumns: ["id"]
    },{
      foreignKeyName: "connect_dead_letters_run_id_fkey"
      columns: ["run_id"]
isOneToOne: false
      referencedRelation: "connect_runs"
      referencedColumns: ["id"]
    },{
      foreignKeyName: "connect_dead_letters_stream_id_fkey"
      columns: ["stream_id"]
isOneToOne: false
      referencedRelation: "connect_streams"
      referencedColumns: ["id"]
    }
                  ]
                },"connect_events": {
                  Row: {
                    "actor_id": string | null,"code": string,"connection_id": string,"created_at": string,"detail": Json | null,"id": string,"level": string,"run_id": string | null
                  }
                  ComputedFields: never
                  Insert: {
                    "actor_id"?: string | null,"code": string,"connection_id": string,"created_at"?: string,"detail"?: Json | null,"id"?: string,"level"?: string,"run_id"?: string | null
                  }
                  Update: {
                    "actor_id"?: string | null,"code"?: string,"connection_id"?: string,"created_at"?: string,"detail"?: Json | null,"id"?: string,"level"?: string,"run_id"?: string | null
                  }
                  Relationships: [
                    {
      foreignKeyName: "connect_events_connection_id_fkey"
      columns: ["connection_id"]
isOneToOne: false
      referencedRelation: "connect_connections"
      referencedColumns: ["id"]
    },{
      foreignKeyName: "connect_events_run_id_fkey"
      columns: ["run_id"]
isOneToOne: false
      referencedRelation: "connect_runs"
      referencedColumns: ["id"]
    }
                  ]
                },"connect_run_streams": {
                  Row: {
                    "checkpoint": NonNullable<Json>,"completed": boolean,"generation": number,"initial_state": NonNullable<Json>,"replay_deferred": number,"run_id": string,"schema_json": NonNullable<Json>,"sequence": number,"stream_id": string
                  }
                  ComputedFields: never
                  Insert: {
                    "checkpoint"?: NonNullable<Json>,"completed"?: boolean,"generation": number,"initial_state": NonNullable<Json>,"replay_deferred"?: number,"run_id": string,"schema_json": NonNullable<Json>,"sequence"?: number,"stream_id": string
                  }
                  Update: {
                    "checkpoint"?: NonNullable<Json>,"completed"?: boolean,"generation"?: number,"initial_state"?: NonNullable<Json>,"replay_deferred"?: number,"run_id"?: string,"schema_json"?: NonNullable<Json>,"sequence"?: number,"stream_id"?: string
                  }
                  Relationships: [
                    {
      foreignKeyName: "connect_run_streams_run_id_fkey"
      columns: ["run_id"]
isOneToOne: false
      referencedRelation: "connect_runs"
      referencedColumns: ["id"]
    },{
      foreignKeyName: "connect_run_streams_stream_id_fkey"
      columns: ["stream_id"]
isOneToOne: false
      referencedRelation: "connect_streams"
      referencedColumns: ["id"]
    }
                  ]
                },"connect_runs": {
                  Row: {
                    "attempts": number,"bytes_committed": number,"connection_id": string,"created_at": string,"error_code": string | null,"finished_at": string | null,"id": string,"lease_token": string | null,"lease_until": string | null,"max_attempts": number,"next_attempt_at": string,"operation": string,"request_key": string,"rows_committed": number,"status": string
                  }
                  ComputedFields: never
                  Insert: {
                    "attempts"?: number,"bytes_committed"?: number,"connection_id": string,"created_at"?: string,"error_code"?: string | null,"finished_at"?: string | null,"id"?: string,"lease_token"?: string | null,"lease_until"?: string | null,"max_attempts"?: number,"next_attempt_at"?: string,"operation": string,"request_key": string,"rows_committed"?: number,"status"?: string
                  }
                  Update: {
                    "attempts"?: number,"bytes_committed"?: number,"connection_id"?: string,"created_at"?: string,"error_code"?: string | null,"finished_at"?: string | null,"id"?: string,"lease_token"?: string | null,"lease_until"?: string | null,"max_attempts"?: number,"next_attempt_at"?: string,"operation"?: string,"request_key"?: string,"rows_committed"?: number,"status"?: string
                  }
                  Relationships: [
                    {
      foreignKeyName: "connect_runs_connection_id_fkey"
      columns: ["connection_id"]
isOneToOne: false
      referencedRelation: "connect_connections"
      referencedColumns: ["id"]
    }
                  ]
                },"connect_streams": {
                  Row: {
                    "connection_id": string,"cursor_field": string,"destination_mode": string,"enabled": boolean,"id": string,"key_columns": (string)[],"last_success_at": string | null,"name": string,"namespace": string,"primary_key": string,"schema_json": NonNullable<Json>,"schema_policy": string,"state": NonNullable<Json>,"state_generation": number,"sync_mode": string
                  }
                  ComputedFields: never
                  Insert: {
                    "connection_id": string,"cursor_field": string,"destination_mode"?: string,"enabled"?: boolean,"id"?: string,"key_columns": (string)[],"last_success_at"?: string | null,"name": string,"namespace": string,"primary_key": string,"schema_json"?: NonNullable<Json>,"schema_policy"?: string,"state"?: NonNullable<Json>,"state_generation"?: number,"sync_mode"?: string
                  }
                  Update: {
                    "connection_id"?: string,"cursor_field"?: string,"destination_mode"?: string,"enabled"?: boolean,"id"?: string,"key_columns"?: (string)[],"last_success_at"?: string | null,"name"?: string,"namespace"?: string,"primary_key"?: string,"schema_json"?: NonNullable<Json>,"schema_policy"?: string,"state"?: NonNullable<Json>,"state_generation"?: number,"sync_mode"?: string
                  }
                  Relationships: [
                    {
      foreignKeyName: "connect_streams_connection_id_fkey"
      columns: ["connection_id"]
isOneToOne: false
      referencedRelation: "connect_connections"
      referencedColumns: ["id"]
    }
                  ]
                },"connection_secrets": {
                  Row: {
                    "connection_id": string,"secrets": NonNullable<Json>,"updated_at": string
                  }
                  ComputedFields: never
                  Insert: {
                    "connection_id": string,"secrets"?: NonNullable<Json>,"updated_at"?: string
                  }
                  Update: {
                    "connection_id"?: string,"secrets"?: NonNullable<Json>,"updated_at"?: string
                  }
                  Relationships: [
                    {
      foreignKeyName: "connection_secrets_connection_id_fkey"
      columns: ["connection_id"]
isOneToOne: true
      referencedRelation: "connections"
      referencedColumns: ["id"]
    }
                  ]
                },"connections": {
                  Row: {
                    "config": NonNullable<Json>,"created_at": string,"created_by": string,"id": string,"kind": string,"last_tested_at": string | null,"name": string,"organization_id": string | null,"status": string,"status_message": string | null
                  }
                  ComputedFields: never
                  Insert: {
                    "config"?: NonNullable<Json>,"created_at"?: string,"created_by": string,"id"?: string,"kind": string,"last_tested_at"?: string | null,"name": string,"organization_id"?: string | null,"status"?: string,"status_message"?: string | null
                  }
                  Update: {
                    "config"?: NonNullable<Json>,"created_at"?: string,"created_by"?: string,"id"?: string,"kind"?: string,"last_tested_at"?: string | null,"name"?: string,"organization_id"?: string | null,"status"?: string,"status_message"?: string | null
                  }
                  Relationships: [
                    {
      foreignKeyName: "connections_organization_id_fkey"
      columns: ["organization_id"]
isOneToOne: false
      referencedRelation: "organizations"
      referencedColumns: ["id"]
    }
                  ]
                },"entitlements": {
                  Row: {
                    "expires_at": string | null,"feature": string,"granted_at": string,"granted_by": string | null,"licence_id": string,"organization_id": string
                  }
                  ComputedFields: never
                  Insert: {
                    "expires_at"?: string | null,"feature": string,"granted_at"?: string,"granted_by"?: string | null,"licence_id": string,"organization_id": string
                  }
                  Update: {
                    "expires_at"?: string | null,"feature"?: string,"granted_at"?: string,"granted_by"?: string | null,"licence_id"?: string,"organization_id"?: string
                  }
                  Relationships: [
                    {
      foreignKeyName: "entitlements_organization_id_fkey"
      columns: ["organization_id"]
isOneToOne: false
      referencedRelation: "organizations"
      referencedColumns: ["id"]
    }
                  ]
                },"execution_engines": {
                  Row: {
                    "capabilities": NonNullable<Json>,"checked_at": string | null,"config": NonNullable<Json>,"created_at": string,"enabled": boolean,"endpoint": string,"health": string,"id": string,"kind": string,"lifecycle_status": string,"name": string,"organization_id": string | null,"owner_id": string,"ownership_mode": string,"provider": string,"updated_at": string,"version": string | null
                  }
                  ComputedFields: never
                  Insert: {
                    "capabilities"?: NonNullable<Json>,"checked_at"?: string | null,"config"?: NonNullable<Json>,"created_at"?: string,"enabled"?: boolean,"endpoint": string,"health"?: string,"id"?: string,"kind": string,"lifecycle_status"?: string,"name": string,"organization_id"?: string | null,"owner_id": string,"ownership_mode"?: string,"provider"?: string,"updated_at"?: string,"version"?: string | null
                  }
                  Update: {
                    "capabilities"?: NonNullable<Json>,"checked_at"?: string | null,"config"?: NonNullable<Json>,"created_at"?: string,"enabled"?: boolean,"endpoint"?: string,"health"?: string,"id"?: string,"kind"?: string,"lifecycle_status"?: string,"name"?: string,"organization_id"?: string | null,"owner_id"?: string,"ownership_mode"?: string,"provider"?: string,"updated_at"?: string,"version"?: string | null
                  }
                  Relationships: [
                    {
      foreignKeyName: "execution_engines_organization_id_fkey"
      columns: ["organization_id"]
isOneToOne: false
      referencedRelation: "organizations"
      referencedColumns: ["id"]
    }
                  ]
                },"file_tables": {
                  Row: {
                    "columns": (string)[],"id": string,"manifest_prefix": string,"manifest_sha256": string | null,"name": string,"organization_id": string | null,"owner_id": string,"part_count": number,"published_at": string,"row_count": number,"source_sha256": string
                  }
                  ComputedFields: never
                  Insert: {
                    "columns"?: (string)[],"id"?: string,"manifest_prefix": string,"manifest_sha256"?: string | null,"name": string,"organization_id"?: string | null,"owner_id"?: string,"part_count": number,"published_at"?: string,"row_count": number,"source_sha256": string
                  }
                  Update: {
                    "columns"?: (string)[],"id"?: string,"manifest_prefix"?: string,"manifest_sha256"?: string | null,"name"?: string,"organization_id"?: string | null,"owner_id"?: string,"part_count"?: number,"published_at"?: string,"row_count"?: number,"source_sha256"?: string
                  }
                  Relationships: [
                    
                  ]
                },"ingest_dead_letters": {
                  Row: {
                    "batch_id": string,"created_at": string,"error_code": string,"id": string,"line": number,"object_key": string,"offset": string,"organization_id": string | null,"owner_id": string,"partition": number,"payload_sha256": string | null,"source_id": string,"status": string,"topic": string
                  }
                  ComputedFields: never
                  Insert: {
                    "batch_id": string,"created_at"?: string,"error_code": string,"id"?: string,"line": number,"object_key": string,"offset": string,"organization_id"?: string | null,"owner_id": string,"partition": number,"payload_sha256"?: string | null,"source_id": string,"status"?: string,"topic": string
                  }
                  Update: {
                    "batch_id"?: string,"created_at"?: string,"error_code"?: string,"id"?: string,"line"?: number,"object_key"?: string,"offset"?: string,"organization_id"?: string | null,"owner_id"?: string,"partition"?: number,"payload_sha256"?: string | null,"source_id"?: string,"status"?: string,"topic"?: string
                  }
                  Relationships: [
                    {
      foreignKeyName: "ingest_dead_letters_organization_id_fkey"
      columns: ["organization_id"]
isOneToOne: false
      referencedRelation: "organizations"
      referencedColumns: ["id"]
    },{
      foreignKeyName: "ingest_dead_letters_source_id_fkey"
      columns: ["source_id"]
isOneToOne: false
      referencedRelation: "ingest_sources"
      referencedColumns: ["id"]
    }
                  ]
                },"ingest_landing_batches": {
                  Row: {
                    "batch_key": string,"created_at": string,"id": string,"mode": string,"object_key": string,"organization_id": string | null,"owner_id": string,"row_count": number,"sha256": string,"source_id": string
                  }
                  ComputedFields: never
                  Insert: {
                    "batch_key": string,"created_at"?: string,"id"?: string,"mode": string,"object_key": string,"organization_id"?: string | null,"owner_id": string,"row_count"?: number,"sha256": string,"source_id": string
                  }
                  Update: {
                    "batch_key"?: string,"created_at"?: string,"id"?: string,"mode"?: string,"object_key"?: string,"organization_id"?: string | null,"owner_id"?: string,"row_count"?: number,"sha256"?: string,"source_id"?: string
                  }
                  Relationships: [
                    {
      foreignKeyName: "ingest_landing_batches_source_id_fkey"
      columns: ["source_id"]
isOneToOne: false
      referencedRelation: "ingest_sources"
      referencedColumns: ["id"]
    }
                  ]
                },"ingest_partitions": {
                  Row: {
                    "attempts": number,"batches": number,"checkpoint": Json | null,"claim_version": number,"claimed_at": string | null,"error": string | null,"est_rows": number,"finished_at": string | null,"heartbeat_at": string | null,"id": string,"idx": number,"label": string,"lease_token": string | null,"lease_until": string | null,"owner_id": string,"predicate": NonNullable<Json>,"rows_written": number,"run_id": string,"started_at": string | null,"status": string,"updated_at": string,"worker": string | null
                  }
                  ComputedFields: never
                  Insert: {
                    "attempts"?: number,"batches"?: number,"checkpoint"?: Json | null,"claim_version"?: number,"claimed_at"?: string | null,"error"?: string | null,"est_rows"?: number,"finished_at"?: string | null,"heartbeat_at"?: string | null,"id"?: string,"idx": number,"label": string,"lease_token"?: string | null,"lease_until"?: string | null,"owner_id": string,"predicate": NonNullable<Json>,"rows_written"?: number,"run_id": string,"started_at"?: string | null,"status"?: string,"updated_at"?: string,"worker"?: string | null
                  }
                  Update: {
                    "attempts"?: number,"batches"?: number,"checkpoint"?: Json | null,"claim_version"?: number,"claimed_at"?: string | null,"error"?: string | null,"est_rows"?: number,"finished_at"?: string | null,"heartbeat_at"?: string | null,"id"?: string,"idx"?: number,"label"?: string,"lease_token"?: string | null,"lease_until"?: string | null,"owner_id"?: string,"predicate"?: NonNullable<Json>,"rows_written"?: number,"run_id"?: string,"started_at"?: string | null,"status"?: string,"updated_at"?: string,"worker"?: string | null
                  }
                  Relationships: [
                    {
      foreignKeyName: "ingest_partitions_run_id_fkey"
      columns: ["run_id"]
isOneToOne: false
      referencedRelation: "ingest_runs"
      referencedColumns: ["id"]
    }
                  ]
                },"ingest_rows": {
                  Row: {
                    "data": NonNullable<Json>,"partition_idx": number,"pk": string,"run_id": string
                  }
                  ComputedFields: never
                  Insert: {
                    "data": NonNullable<Json>,"partition_idx": number,"pk": string,"run_id": string
                  }
                  Update: {
                    "data"?: NonNullable<Json>,"partition_idx"?: number,"pk"?: string,"run_id"?: string
                  }
                  Relationships: [
                    {
      foreignKeyName: "ingest_rows_run_id_fkey"
      columns: ["run_id"]
isOneToOne: false
      referencedRelation: "ingest_runs"
      referencedColumns: ["id"]
    }
                  ]
                },"ingest_runs": {
                  Row: {
                    "batch_size": number,"concurrency": number,"created_at": string,"expected_rows": number,"finished_at": string | null,"id": string,"organization_id": string | null,"owner_id": string,"pk_columns": (string)[],"pk_encoding": string,"plan": NonNullable<Json>,"published_at": string | null,"snapshot": Json | null,"source_id": string | null,"source_kind": string,"started_at": string | null,"status": string,"target_table": string,"written_rows": number
                  }
                  ComputedFields: never
                  Insert: {
                    "batch_size"?: number,"concurrency"?: number,"created_at"?: string,"expected_rows"?: number,"finished_at"?: string | null,"id"?: string,"organization_id"?: string | null,"owner_id": string,"pk_columns"?: (string)[],"pk_encoding"?: string,"plan"?: NonNullable<Json>,"published_at"?: string | null,"snapshot"?: Json | null,"source_id"?: string | null,"source_kind": string,"started_at"?: string | null,"status"?: string,"target_table": string,"written_rows"?: number
                  }
                  Update: {
                    "batch_size"?: number,"concurrency"?: number,"created_at"?: string,"expected_rows"?: number,"finished_at"?: string | null,"id"?: string,"organization_id"?: string | null,"owner_id"?: string,"pk_columns"?: (string)[],"pk_encoding"?: string,"plan"?: NonNullable<Json>,"published_at"?: string | null,"snapshot"?: Json | null,"source_id"?: string | null,"source_kind"?: string,"started_at"?: string | null,"status"?: string,"target_table"?: string,"written_rows"?: number
                  }
                  Relationships: [
                    {
      foreignKeyName: "ingest_runs_organization_id_fkey"
      columns: ["organization_id"]
isOneToOne: false
      referencedRelation: "organizations"
      referencedColumns: ["id"]
    },{
      foreignKeyName: "ingest_runs_source_fk"
      columns: ["source_id"]
isOneToOne: false
      referencedRelation: "ingest_sources"
      referencedColumns: ["id"]
    }
                  ]
                },"ingest_source_secrets": {
                  Row: {
                    "headers": NonNullable<Json>,"source_id": string,"updated_at": string
                  }
                  ComputedFields: never
                  Insert: {
                    "headers"?: NonNullable<Json>,"source_id": string,"updated_at"?: string
                  }
                  Update: {
                    "headers"?: NonNullable<Json>,"source_id"?: string,"updated_at"?: string
                  }
                  Relationships: [
                    {
      foreignKeyName: "ingest_source_secrets_source_id_fkey"
      columns: ["source_id"]
isOneToOne: true
      referencedRelation: "ingest_sources"
      referencedColumns: ["id"]
    }
                  ]
                },"ingest_sources": {
                  Row: {
                    "adapter_version": number,"config": NonNullable<Json>,"created_at": string,"enabled": boolean,"id": string,"kind": string,"last_rows": number,"last_run_at": string | null,"last_status": string | null,"name": string,"organization_id": string | null,"owner_id": string,"pk_columns": (string)[],"target_table": string,"token_hash": string | null
                  }
                  ComputedFields: never
                  Insert: {
                    "adapter_version"?: number,"config"?: NonNullable<Json>,"created_at"?: string,"enabled"?: boolean,"id"?: string,"kind"?: string,"last_rows"?: number,"last_run_at"?: string | null,"last_status"?: string | null,"name": string,"organization_id"?: string | null,"owner_id": string,"pk_columns"?: (string)[],"target_table": string,"token_hash"?: string | null
                  }
                  Update: {
                    "adapter_version"?: number,"config"?: NonNullable<Json>,"created_at"?: string,"enabled"?: boolean,"id"?: string,"kind"?: string,"last_rows"?: number,"last_run_at"?: string | null,"last_status"?: string | null,"name"?: string,"organization_id"?: string | null,"owner_id"?: string,"pk_columns"?: (string)[],"target_table"?: string,"token_hash"?: string | null
                  }
                  Relationships: [
                    {
      foreignKeyName: "ingest_sources_organization_id_fkey"
      columns: ["organization_id"]
isOneToOne: false
      referencedRelation: "organizations"
      referencedColumns: ["id"]
    }
                  ]
                },"ingest_tables": {
                  Row: {
                    "current_run_id": string | null,"name": string,"organization_id": string | null,"owner_id": string,"published_at": string,"row_count": number
                  }
                  ComputedFields: never
                  Insert: {
                    "current_run_id"?: string | null,"name": string,"organization_id"?: string | null,"owner_id": string,"published_at"?: string,"row_count"?: number
                  }
                  Update: {
                    "current_run_id"?: string | null,"name"?: string,"organization_id"?: string | null,"owner_id"?: string,"published_at"?: string,"row_count"?: number
                  }
                  Relationships: [
                    {
      foreignKeyName: "ingest_tables_current_run_id_fkey"
      columns: ["current_run_id"]
isOneToOne: false
      referencedRelation: "ingest_runs"
      referencedColumns: ["id"]
    },{
      foreignKeyName: "ingest_tables_organization_id_fkey"
      columns: ["organization_id"]
isOneToOne: false
      referencedRelation: "organizations"
      referencedColumns: ["id"]
    }
                  ]
                },"ingest_workers": {
                  Row: {
                    "heartbeat_at": string,"host": string | null,"id": string,"owner_id": string,"registered_at": string,"source_id": string
                  }
                  ComputedFields: never
                  Insert: {
                    "heartbeat_at"?: string,"host"?: string | null,"id": string,"owner_id": string,"registered_at"?: string,"source_id": string
                  }
                  Update: {
                    "heartbeat_at"?: string,"host"?: string | null,"id"?: string,"owner_id"?: string,"registered_at"?: string,"source_id"?: string
                  }
                  Relationships: [
                    {
      foreignKeyName: "ingest_workers_source_id_fkey"
      columns: ["source_id"]
isOneToOne: false
      referencedRelation: "ingest_sources"
      referencedColumns: ["id"]
    }
                  ]
                },"org_members": {
                  Row: {
                    "org_id": string,"user_id": string,"workspace_role": string
                  }
                  ComputedFields: never
                  Insert: {
                    "org_id": string,"user_id": string,"workspace_role"?: string
                  }
                  Update: {
                    "org_id"?: string,"user_id"?: string,"workspace_role"?: string
                  }
                  Relationships: [
                    {
      foreignKeyName: "org_members_org_id_fkey"
      columns: ["org_id"]
isOneToOne: false
      referencedRelation: "organizations"
      referencedColumns: ["id"]
    }
                  ]
                },"organizations": {
                  Row: {
                    "compute_quota_hours": number,"created_at": string,"created_by": string,"description": string,"id": string,"infra": string,"name": string,"onboarding_settings": NonNullable<Json>,"status": string,"storage_quota_mb": number
                  }
                  ComputedFields: never
                  Insert: {
                    "compute_quota_hours"?: number,"created_at"?: string,"created_by": string,"description"?: string,"id"?: string,"infra"?: string,"name": string,"onboarding_settings"?: NonNullable<Json>,"status"?: string,"storage_quota_mb"?: number
                  }
                  Update: {
                    "compute_quota_hours"?: number,"created_at"?: string,"created_by"?: string,"description"?: string,"id"?: string,"infra"?: string,"name"?: string,"onboarding_settings"?: NonNullable<Json>,"status"?: string,"storage_quota_mb"?: number
                  }
                  Relationships: [
                    
                  ]
                },"platform_settings": {
                  Row: {
                    "key": string,"updated_at": string,"updated_by": string | null,"value": NonNullable<Json>
                  }
                  ComputedFields: never
                  Insert: {
                    "key": string,"updated_at"?: string,"updated_by"?: string | null,"value": NonNullable<Json>
                  }
                  Update: {
                    "key"?: string,"updated_at"?: string,"updated_by"?: string | null,"value"?: NonNullable<Json>
                  }
                  Relationships: [
                    
                  ]
                },"profiles": {
                  Row: {
                    "active_organization_id": string | null,"created_at": string,"default_organization_id": string | null,"display_name": string | null,"email": string | null,"id": string,"onboarding_completed_at": string | null,"onboarding_first_source": string | null,"onboarding_step": number
                  }
                  ComputedFields: never
                  Insert: {
                    "active_organization_id"?: string | null,"created_at"?: string,"default_organization_id"?: string | null,"display_name"?: string | null,"email"?: string | null,"id": string,"onboarding_completed_at"?: string | null,"onboarding_first_source"?: string | null,"onboarding_step"?: number
                  }
                  Update: {
                    "active_organization_id"?: string | null,"created_at"?: string,"default_organization_id"?: string | null,"display_name"?: string | null,"email"?: string | null,"id"?: string,"onboarding_completed_at"?: string | null,"onboarding_first_source"?: string | null,"onboarding_step"?: number
                  }
                  Relationships: [
                    {
      foreignKeyName: "profiles_active_organization_id_fkey"
      columns: ["active_organization_id"]
isOneToOne: false
      referencedRelation: "organizations"
      referencedColumns: ["id"]
    },{
      foreignKeyName: "profiles_default_organization_id_fkey"
      columns: ["default_organization_id"]
isOneToOne: false
      referencedRelation: "organizations"
      referencedColumns: ["id"]
    }
                  ]
                },"rate_limit_windows": {
                  Row: {
                    "hits": number,"key": string,"window_start": string
                  }
                  ComputedFields: never
                  Insert: {
                    "hits": number,"key": string,"window_start": string
                  }
                  Update: {
                    "hits"?: number,"key"?: string,"window_start"?: string
                  }
                  Relationships: [
                    
                  ]
                },"row_policies": {
                  Row: {
                    "column_name": string | null,"created_at": string,"created_by": string,"filter_sql": string,"id": string,"op": string,"organization_id": string | null,"role": Database["public"]['Enums']["app_role"],"table_name": string,"value": string | null
                  }
                  ComputedFields: never
                  Insert: {
                    "column_name"?: string | null,"created_at"?: string,"created_by": string,"filter_sql": string,"id"?: string,"op"?: string,"organization_id"?: string | null,"role": Database["public"]['Enums']["app_role"],"table_name": string,"value"?: string | null
                  }
                  Update: {
                    "column_name"?: string | null,"created_at"?: string,"created_by"?: string,"filter_sql"?: string,"id"?: string,"op"?: string,"organization_id"?: string | null,"role"?: Database["public"]['Enums']["app_role"],"table_name"?: string,"value"?: string | null
                  }
                  Relationships: [
                    {
      foreignKeyName: "row_policies_organization_id_fkey"
      columns: ["organization_id"]
isOneToOne: false
      referencedRelation: "organizations"
      referencedColumns: ["id"]
    }
                  ]
                },"schema_subjects": {
                  Row: {
                    "compatibility": string,"created_at": string,"format": string,"id": string,"name": string,"organization_id": string | null,"owner_id": string
                  }
                  ComputedFields: never
                  Insert: {
                    "compatibility"?: string,"created_at"?: string,"format"?: string,"id"?: string,"name": string,"organization_id"?: string | null,"owner_id": string
                  }
                  Update: {
                    "compatibility"?: string,"created_at"?: string,"format"?: string,"id"?: string,"name"?: string,"organization_id"?: string | null,"owner_id"?: string
                  }
                  Relationships: [
                    {
      foreignKeyName: "schema_subjects_organization_id_fkey"
      columns: ["organization_id"]
isOneToOne: false
      referencedRelation: "organizations"
      referencedColumns: ["id"]
    }
                  ]
                },"schema_versions": {
                  Row: {
                    "created_at": string,"created_by": string,"fingerprint": string,"id": string,"schema": NonNullable<Json>,"subject_id": string,"version": number
                  }
                  ComputedFields: never
                  Insert: {
                    "created_at"?: string,"created_by": string,"fingerprint": string,"id"?: string,"schema": NonNullable<Json>,"subject_id": string,"version": number
                  }
                  Update: {
                    "created_at"?: string,"created_by"?: string,"fingerprint"?: string,"id"?: string,"schema"?: NonNullable<Json>,"subject_id"?: string,"version"?: number
                  }
                  Relationships: [
                    {
      foreignKeyName: "schema_versions_subject_id_fkey"
      columns: ["subject_id"]
isOneToOne: false
      referencedRelation: "schema_subjects"
      referencedColumns: ["id"]
    }
                  ]
                },"storage_reconciliation_log": {
                  Row: {
                    "action": string,"bucket": string,"bytes": number | null,"created_at": string,"dry_run": boolean,"id": string,"kind": string,"object_created_at": string | null,"object_key": string,"run_id": string
                  }
                  ComputedFields: never
                  Insert: {
                    "action": string,"bucket": string,"bytes"?: number | null,"created_at"?: string,"dry_run": boolean,"id"?: string,"kind": string,"object_created_at"?: string | null,"object_key": string,"run_id": string
                  }
                  Update: {
                    "action"?: string,"bucket"?: string,"bytes"?: number | null,"created_at"?: string,"dry_run"?: boolean,"id"?: string,"kind"?: string,"object_created_at"?: string | null,"object_key"?: string,"run_id"?: string
                  }
                  Relationships: [
                    
                  ]
                },"table_versions": {
                  Row: {
                    "columns": NonNullable<Json>,"created_at": string,"created_by": string,"id": string,"operation": string,"row_count": number,"rows": NonNullable<Json>,"table_id": string | null,"table_name": string,"version": number
                  }
                  ComputedFields: never
                  Insert: {
                    "columns"?: NonNullable<Json>,"created_at"?: string,"created_by": string,"id"?: string,"operation"?: string,"row_count"?: number,"rows"?: NonNullable<Json>,"table_id"?: string | null,"table_name": string,"version": number
                  }
                  Update: {
                    "columns"?: NonNullable<Json>,"created_at"?: string,"created_by"?: string,"id"?: string,"operation"?: string,"row_count"?: number,"rows"?: NonNullable<Json>,"table_id"?: string | null,"table_name"?: string,"version"?: number
                  }
                  Relationships: [
                    
                  ]
                },"user_roles": {
                  Row: {
                    "id": string,"role": Database["public"]['Enums']["app_role"],"user_id": string
                  }
                  ComputedFields: never
                  Insert: {
                    "id"?: string,"role": Database["public"]['Enums']["app_role"],"user_id": string
                  }
                  Update: {
                    "id"?: string,"role"?: Database["public"]['Enums']["app_role"],"user_id"?: string
                  }
                  Relationships: [
                    
                  ]
                },"user_tables": {
                  Row: {
                    "columns": NonNullable<Json>,"created_at": string,"id": string,"name": string,"organization_id": string | null,"owner_id": string,"row_count": number,"rows": NonNullable<Json>
                  }
                  ComputedFields: never
                  Insert: {
                    "columns"?: NonNullable<Json>,"created_at"?: string,"id"?: string,"name": string,"organization_id"?: string | null,"owner_id": string,"row_count"?: number,"rows"?: NonNullable<Json>
                  }
                  Update: {
                    "columns"?: NonNullable<Json>,"created_at"?: string,"id"?: string,"name"?: string,"organization_id"?: string | null,"owner_id"?: string,"row_count"?: number,"rows"?: NonNullable<Json>
                  }
                  Relationships: [
                    {
      foreignKeyName: "user_tables_organization_id_fkey"
      columns: ["organization_id"]
isOneToOne: false
      referencedRelation: "organizations"
      referencedColumns: ["id"]
    }
                  ]
                }
          }
          Views: {
            [_ in never]: never
          }
          Functions: {
      write_native_table: { Args: { _name: string; _rows: Json; _columns: Json; _operation: string; _table_id?: string }; Returns: number };

            "ensure_workspace":
{ Args: Record<PropertyKey, never>; Returns: Json
                           },
"is_active_user":
{ Args: Record<PropertyKey, never>; Returns: boolean
                           },
"require_active_subject":
{ Args: { "_subject": string }; Returns: undefined
                           },
"provision_workspace_for_user":
{ Args: { "_user": string }; Returns: string
                           },
"select_workspace":
{ Args: { "_workspace": string }; Returns: Json
                           },
"update_account_profile":
{ Args: { "_name": string }; Returns: Json
                           },
"update_onboarding":
{ Args: { "_description"?: string,"_first_source"?: string,"_name"?: string,"_step": number }; Returns: Json
                           },
"apply_file_table_policies":
{ Args: { "_rows": Json,"_table": string }; Returns: Json
                           },
"apply_licence_entitlements":
{ Args: { "_actor": string,"_expires": string,"_features": (string)[],"_licence": string,"_org": string }; Returns: number
                           },
"bootstrap_platform_admin":
{ Args: { "_email": string }; Returns: string
                           },
"can_access":
{ Args: { "_org": string,"_owner": string }; Returns: boolean
                           },
"can_govern":
{ Args: { "_org": string }; Returns: boolean
                           },
"can_read_file_object":
{ Args: { "_name": string }; Returns: boolean
                           },
"can_read_scoped":
{ Args: { "_org": string,"_owner": string }; Returns: boolean
                           },
"can_see_user":
{ Args: { "_user": string }; Returns: boolean
                           },
"cancel_ingest_run":
{ Args: { "_run": string }; Returns: boolean
                           },
"claim_ingest_partition":
{ Args: { "_lease_seconds"?: number,"_run": string,"_worker": string }; Returns: {
              "attempts": number,
"batches": number,
"checkpoint": Json | null,
"claim_version": number,
"claimed_at": string | null,
"error": string | null,
"est_rows": number,
"finished_at": string | null,
"heartbeat_at": string | null,
"id": string,
"idx": number,
"label": string,
"lease_token": string | null,
"lease_until": string | null,
"owner_id": string,
"predicate": NonNullable<Json>,
"rows_written": number,
"run_id": string,
"started_at": string | null,
"status": string,
"updated_at": string,
"worker": string | null
            }[]
                          SetofOptions: {
        from: "*"
        to: "ingest_partitions"
        isOneToOne: false
        isSetofReturn: true
      } },
"claim_specific_partition":
{ Args: { "_idx": number,"_lease_seconds"?: number,"_run": string,"_worker": string }; Returns: {
              "attempts": number,
"batches": number,
"checkpoint": Json | null,
"claim_version": number,
"claimed_at": string | null,
"error": string | null,
"est_rows": number,
"finished_at": string | null,
"heartbeat_at": string | null,
"id": string,
"idx": number,
"label": string,
"lease_token": string | null,
"lease_until": string | null,
"owner_id": string,
"predicate": NonNullable<Json>,
"rows_written": number,
"run_id": string,
"started_at": string | null,
"status": string,
"updated_at": string,
"worker": string | null
            }[]
                          SetofOptions: {
        from: "*"
        to: "ingest_partitions"
        isOneToOne: false
        isSetofReturn: true
      } },
"connect_accept_offset_gap":
{ Args: { "_stream": string }; Returns: boolean
                           },
"connect_cancel":
{ Args: { "_run": string }; Returns: boolean
                           },
"connect_claim":
{ Args: { "_connection": string }; Returns: Json
                           },
"connect_commit":
{ Args: { "_batch": string,"_bytes": number,"_checkpoint": Json,"_completed": boolean,"_manifest": string,"_rows": number,"_run": string,"_sequence": number,"_sha": string,"_stream": string,"_token": string }; Returns: string
                           },
"connect_discard_dead_letters":
{ Args: { "_connection": string,"_dead_letters": (string)[] }; Returns: number
                           },
"connect_enqueue":
{ Args: { "_connection": string,"_operation": string,"_request": string }; Returns: string
                           },
      connect_defer_replay: {
        Args: { _ids: string[]; _run: string; _stream: string; _token: string };
        Returns: number;
      };
"connect_enqueue_replay":
{ Args: { "_connection": string,"_dead_letters": (string)[],"_request": string }; Returns: string
                           },
"connect_finish":
{ Args: { "_catalog"?: Json,"_code": string,"_ok": boolean,"_run": string,"_token": string }; Returns: boolean
                           },
"connect_heartbeat":
{ Args: { "_run": string,"_token": string }; Returns: boolean
                           },
"connect_record_dead_letters":
{ Args: { "_entries": Json,"_object": string,"_run": string,"_sequence": number,"_stream": string,"_token": string }; Returns: number
                           },
"connect_report":
{ Args: { "_code": string,"_detail": Json,"_run": string,"_stream"?: string,"_token": string }; Returns: boolean
                           },
"connect_rewind_stream":
{ Args: { "_state": Json,"_stream": string }; Returns: boolean
                           },
"consume_rate_limit":
{ Args: { "_key": string,"_limit": number,"_window_seconds": number }; Returns: boolean
                           },
"create_demo_ingest_run_verified":
{ Args: { "_actor": string,"_batch": number,"_concurrency": number,"_partitions": Json,"_plan": Json,"_target": string,"_total": number }; Returns: string
                           },
"create_ingest_run_atomic":
{ Args: { "_batch": number,"_concurrency": number,"_partitions": Json,"_pk": (string)[],"_plan": Json,"_source": string,"_target": string,"_total": number }; Returns: string
                           },
"create_ingest_run_verified":
{ Args: { "_actor": string,"_batch": number,"_concurrency": number,"_partitions": Json,"_pk": (string)[],"_plan": Json,"_source": string,"_target": string,"_total": number }; Returns: string
                           },
"create_ingest_source_verified":
{ Args: { "_actor": string,"_config": Json,"_hash": string,"_headers": Json,"_kind": string,"_name": string,"_target": string }; Returns: string
                           },
"dataset_policy_decision":
{ Args: { "_dataset": string,"_subject": string }; Returns: Json
                           },
"fail_ingest_partition_fenced":
{ Args: { "_error": string,"_idx": number,"_lease_token": string,"_run": string,"_worker": string }; Returns: boolean
                           },
"file_object_is_committed":
{ Args: { "_name": string }; Returns: boolean
                           },
"has_entitlement":
{ Args: { "_feature": string,"_org": string }; Returns: boolean
                           },
"has_role":
{ Args: { "_role": Database["public"]['Enums']["app_role"],"_user_id": string }; Returns: boolean
                           },
"heartbeat_ingest_partition":
{ Args: { "_idx": number,"_lease_seconds"?: number,"_run": string,"_worker": string }; Returns: boolean
                           },
"heartbeat_ingest_partition_fenced":
{ Args: { "_idx": number,"_lease_seconds"?: number,"_lease_token": string,"_run": string,"_worker": string }; Returns: boolean
                           },
"is_org_member":
{ Args: { "_org": string }; Returns: boolean
                           },
"is_steward_for":
{ Args: { "_org": string,"_user": string }; Returns: boolean
                           },
"mask_text":
{ Args: { "_mode": string,"_v": string }; Returns: string
                           },
"publish_ingest_run":
{ Args: { "_run": string }; Returns: number
                           },
"read_ingest_table":
{ Args: { "_after"?: string,"_limit"?: number,"_name": string }; Returns: {
              "data": Json,"pk": string
            }[]
                           },
"register_ingest_landing_batch":
{ Args: { "_actor": string,"_batch": string,"_key": string,"_mode": string,"_rows": number,"_sha": string,"_source": string }; Returns: string
                           },
"register_personal_verified_file_table":
{ Args: { "_columns": (string)[],"_manifest_sha": string,"_name": string,"_owner": string,"_parts": number,"_prefix": string,"_rows": number,"_source_sha": string }; Returns: string
                           },
"register_verified_file_table":
{ Args: { "_columns": (string)[],"_manifest_sha": string,"_name": string,"_org": string,"_owner": string,"_parts": number,"_prefix": string,"_rows": number,"_source_sha": string }; Returns: string
                           },
"row_passes":
{ Args: { "_col": string,"_op": string,"_row": Json,"_val": string }; Returns: boolean
                           },
"schema_register_version":
{ Args: { "_actor": string,"_compatibility"?: string,"_expected_latest": number,"_fingerprint": string,"_name": string,"_organization": string,"_schema": Json }; Returns: Json
                           },
"secure_tables":
{ Args: Record<PropertyKey, never>; Returns: {
              "columns": Json,"created_at": string,"filtered": boolean,"id": string,"masked": (string)[],"name": string,"owner_id": string,"row_count": number,"rows": Json
            }[]
                           },
"serve_connect_bronze":
{ Args: { "_after"?: number,"_engine": string,"_run": string,"_stream": string,"_subject": string }; Returns: {
              "bucket": string,"byte_count": number,"created_at": string,"dataset_id": string,"evidence_class": string,"format": string,"layer": string,"name": string,"namespace": string,"object_key": string,"produced_by": Json,"row_count": number,"schema": Json,"sequence": number,"sha256": string,"version_id": string
            }[]
                           },
      "serve_manifest":
{ Args: { "_names": (string)[],"_subject": string }; Returns: {
              "bucket": string,"byte_count": number,"created_at": string,"dataset_id": string,"evidence_class": string,"layer": string,"name": string,"object_key": string,"policy": Json,"produced_by": Json,"row_count": number,"schema": Json,"sha256": string,"version_id": string
            }[]
                           },
"shares_org_with":
{ Args: { "_user": string }; Returns: boolean
                           },
"verify_admin_audit":
{ Args: Record<PropertyKey, never>; Returns: {
              "broken_at": number,"checked": number,"ok": boolean
            }[]
                           },
"write_ingest_batch":
{ Args: { "_checkpoint": Json,"_done": boolean,"_idx": number,"_lease_seconds"?: number,"_rows": Json,"_run": string,"_worker": string }; Returns: number
                           },
"write_ingest_batch_fenced":
{ Args: { "_checkpoint": Json,"_done": boolean,"_idx": number,"_lease_seconds"?: number,"_lease_token": string,"_rows": Json,"_run": string,"_worker": string }; Returns: number
                           },
"yield_ingest_partition_fenced":
{ Args: { "_idx": number,"_lease_token": string,"_run": string,"_worker": string }; Returns: boolean
                           }
          }
          Enums: {
            "app_role": "admin"|"steward"|"engineer"|"analyst"
          }
          CompositeTypes: {
            [_ in never]: never
          }
        },"storage": {
          Tables: {
            "buckets": {
                  Row: {
                    "allowed_mime_types": (string)[] | null,"avif_autodetection": boolean | null,"created_at": string | null,"file_size_limit": number | null,"id": string,"lifecycle_configuration": Json | null,"lifecycle_configuration_generation": string | null,"name": string,"owner": string | null,"owner_id": string | null,"public": boolean | null,"type": Database["storage"]['Enums']["buckettype"],"updated_at": string | null,"versioning_status": string
                  }
                  ComputedFields: never
                  Insert: {
                    "allowed_mime_types"?: (string)[] | null,"avif_autodetection"?: boolean | null,"created_at"?: string | null,"file_size_limit"?: number | null,"id": string,"lifecycle_configuration"?: Json | null,"lifecycle_configuration_generation"?: string | null,"name": string,"owner"?: string | null,"owner_id"?: string | null,"public"?: boolean | null,"type"?: Database["storage"]['Enums']["buckettype"],"updated_at"?: string | null,"versioning_status"?: string
                  }
                  Update: {
                    "allowed_mime_types"?: (string)[] | null,"avif_autodetection"?: boolean | null,"created_at"?: string | null,"file_size_limit"?: number | null,"id"?: string,"lifecycle_configuration"?: Json | null,"lifecycle_configuration_generation"?: string | null,"name"?: string,"owner"?: string | null,"owner_id"?: string | null,"public"?: boolean | null,"type"?: Database["storage"]['Enums']["buckettype"],"updated_at"?: string | null,"versioning_status"?: string
                  }
                  Relationships: [
                    
                  ]
                },"buckets_analytics": {
                  Row: {
                    "created_at": string,"deleted_at": string | null,"format": string,"id": string,"name": string,"type": Database["storage"]['Enums']["buckettype"],"updated_at": string
                  }
                  ComputedFields: never
                  Insert: {
                    "created_at"?: string,"deleted_at"?: string | null,"format"?: string,"id"?: string,"name": string,"type"?: Database["storage"]['Enums']["buckettype"],"updated_at"?: string
                  }
                  Update: {
                    "created_at"?: string,"deleted_at"?: string | null,"format"?: string,"id"?: string,"name"?: string,"type"?: Database["storage"]['Enums']["buckettype"],"updated_at"?: string
                  }
                  Relationships: [
                    
                  ]
                },"buckets_vectors": {
                  Row: {
                    "created_at": string,"id": string,"type": Database["storage"]['Enums']["buckettype"],"updated_at": string
                  }
                  ComputedFields: never
                  Insert: {
                    "created_at"?: string,"id": string,"type"?: Database["storage"]['Enums']["buckettype"],"updated_at"?: string
                  }
                  Update: {
                    "created_at"?: string,"id"?: string,"type"?: Database["storage"]['Enums']["buckettype"],"updated_at"?: string
                  }
                  Relationships: [
                    
                  ]
                },"iceberg_namespaces": {
                  Row: {
                    "bucket_name": string,"catalog_id": string,"created_at": string,"id": string,"metadata": NonNullable<Json>,"name": string,"updated_at": string
                  }
                  ComputedFields: never
                  Insert: {
                    "bucket_name": string,"catalog_id": string,"created_at"?: string,"id"?: string,"metadata"?: NonNullable<Json>,"name": string,"updated_at"?: string
                  }
                  Update: {
                    "bucket_name"?: string,"catalog_id"?: string,"created_at"?: string,"id"?: string,"metadata"?: NonNullable<Json>,"name"?: string,"updated_at"?: string
                  }
                  Relationships: [
                    {
      foreignKeyName: "iceberg_namespaces_catalog_id_fkey"
      columns: ["catalog_id"]
isOneToOne: false
      referencedRelation: "buckets_analytics"
      referencedColumns: ["id"]
    }
                  ]
                },"iceberg_tables": {
                  Row: {
                    "bucket_name": string,"catalog_id": string,"created_at": string,"id": string,"location": string,"name": string,"namespace_id": string,"remote_table_id": string | null,"shard_id": string | null,"shard_key": string | null,"updated_at": string
                  }
                  ComputedFields: never
                  Insert: {
                    "bucket_name": string,"catalog_id": string,"created_at"?: string,"id"?: string,"location": string,"name": string,"namespace_id": string,"remote_table_id"?: string | null,"shard_id"?: string | null,"shard_key"?: string | null,"updated_at"?: string
                  }
                  Update: {
                    "bucket_name"?: string,"catalog_id"?: string,"created_at"?: string,"id"?: string,"location"?: string,"name"?: string,"namespace_id"?: string,"remote_table_id"?: string | null,"shard_id"?: string | null,"shard_key"?: string | null,"updated_at"?: string
                  }
                  Relationships: [
                    {
      foreignKeyName: "iceberg_tables_catalog_id_fkey"
      columns: ["catalog_id"]
isOneToOne: false
      referencedRelation: "buckets_analytics"
      referencedColumns: ["id"]
    },{
      foreignKeyName: "iceberg_tables_namespace_id_fkey"
      columns: ["namespace_id"]
isOneToOne: false
      referencedRelation: "iceberg_namespaces"
      referencedColumns: ["id"]
    }
                  ]
                },"migrations": {
                  Row: {
                    "executed_at": string | null,"hash": string,"id": number,"name": string
                  }
                  ComputedFields: never
                  Insert: {
                    "executed_at"?: string | null,"hash": string,"id": number,"name": string
                  }
                  Update: {
                    "executed_at"?: string | null,"hash"?: string,"id"?: number,"name"?: string
                  }
                  Relationships: [
                    
                  ]
                },"objects": {
                  Row: {
                    "archived_at": string | null,"bucket_id": string | null,"created_at": string | null,"id": string,"is_delete_marker": boolean,"is_versioned": boolean,"last_accessed_at": string | null,"metadata": Json | null,"name": string | null,"owner": string | null,"owner_id": string | null,"path_tokens": (string)[] | null,"updated_at": string | null,"user_metadata": Json | null,"version": string | null
                  }
                  ComputedFields: never
                  Insert: {
                    "archived_at"?: string | null,"bucket_id"?: string | null,"created_at"?: string | null,"id"?: string,"is_delete_marker"?: boolean,"is_versioned"?: boolean,"last_accessed_at"?: string | null,"metadata"?: Json | null,"name"?: string | null,"owner"?: string | null,"owner_id"?: string | null,"path_tokens"?: never,"updated_at"?: string | null,"user_metadata"?: Json | null,"version"?: string | null
                  }
                  Update: {
                    "archived_at"?: string | null,"bucket_id"?: string | null,"created_at"?: string | null,"id"?: string,"is_delete_marker"?: boolean,"is_versioned"?: boolean,"last_accessed_at"?: string | null,"metadata"?: Json | null,"name"?: string | null,"owner"?: string | null,"owner_id"?: string | null,"path_tokens"?: never,"updated_at"?: string | null,"user_metadata"?: Json | null,"version"?: string | null
                  }
                  Relationships: [
                    {
      foreignKeyName: "objects_bucketId_fkey"
      columns: ["bucket_id"]
isOneToOne: false
      referencedRelation: "buckets"
      referencedColumns: ["id"]
    }
                  ]
                },"s3_multipart_uploads": {
                  Row: {
                    "bucket_id": string,"created_at": string,"id": string,"in_progress_size": number,"key": string,"metadata": Json | null,"owner_id": string | null,"upload_signature": string,"user_metadata": Json | null,"version": string
                  }
                  ComputedFields: never
                  Insert: {
                    "bucket_id": string,"created_at"?: string,"id": string,"in_progress_size"?: number,"key": string,"metadata"?: Json | null,"owner_id"?: string | null,"upload_signature": string,"user_metadata"?: Json | null,"version": string
                  }
                  Update: {
                    "bucket_id"?: string,"created_at"?: string,"id"?: string,"in_progress_size"?: number,"key"?: string,"metadata"?: Json | null,"owner_id"?: string | null,"upload_signature"?: string,"user_metadata"?: Json | null,"version"?: string
                  }
                  Relationships: [
                    {
      foreignKeyName: "s3_multipart_uploads_bucket_id_fkey"
      columns: ["bucket_id"]
isOneToOne: false
      referencedRelation: "buckets"
      referencedColumns: ["id"]
    }
                  ]
                },"s3_multipart_uploads_parts": {
                  Row: {
                    "bucket_id": string,"created_at": string,"etag": string,"id": string,"key": string,"owner_id": string | null,"part_number": number,"size": number,"upload_id": string,"version": string
                  }
                  ComputedFields: never
                  Insert: {
                    "bucket_id": string,"created_at"?: string,"etag": string,"id"?: string,"key": string,"owner_id"?: string | null,"part_number": number,"size"?: number,"upload_id": string,"version": string
                  }
                  Update: {
                    "bucket_id"?: string,"created_at"?: string,"etag"?: string,"id"?: string,"key"?: string,"owner_id"?: string | null,"part_number"?: number,"size"?: number,"upload_id"?: string,"version"?: string
                  }
                  Relationships: [
                    {
      foreignKeyName: "s3_multipart_uploads_parts_bucket_id_fkey"
      columns: ["bucket_id"]
isOneToOne: false
      referencedRelation: "buckets"
      referencedColumns: ["id"]
    },{
      foreignKeyName: "s3_multipart_uploads_parts_upload_id_fkey"
      columns: ["upload_id"]
isOneToOne: false
      referencedRelation: "s3_multipart_uploads"
      referencedColumns: ["id"]
    }
                  ]
                },"vector_indexes": {
                  Row: {
                    "bucket_id": string,"created_at": string,"data_type": string,"dimension": number,"distance_metric": string,"id": string,"metadata_configuration": Json | null,"name": string,"updated_at": string
                  }
                  ComputedFields: never
                  Insert: {
                    "bucket_id": string,"created_at"?: string,"data_type": string,"dimension": number,"distance_metric": string,"id"?: string,"metadata_configuration"?: Json | null,"name": string,"updated_at"?: string
                  }
                  Update: {
                    "bucket_id"?: string,"created_at"?: string,"data_type"?: string,"dimension"?: number,"distance_metric"?: string,"id"?: string,"metadata_configuration"?: Json | null,"name"?: string,"updated_at"?: string
                  }
                  Relationships: [
                    {
      foreignKeyName: "vector_indexes_bucket_id_fkey"
      columns: ["bucket_id"]
isOneToOne: false
      referencedRelation: "buckets_vectors"
      referencedColumns: ["id"]
    }
                  ]
                }
          }
          Views: {
            [_ in never]: never
          }
          Functions: {
            "allow_any_operation":
{ Args: { "expected_operations": (string)[] }; Returns: boolean
                           },
"allow_only_operation":
{ Args: { "expected_operation": string }; Returns: boolean
                           },
"can_insert_object":
{ Args: { "bucketid": string,"metadata": Json,"name": string,"owner": string }; Returns: undefined
                           },
"extension":
{ Args: { "name": string }; Returns: string
                           },
"filename":
{ Args: { "name": string }; Returns: string
                           },
"foldername":
{ Args: { "name": string }; Returns: (string)[]
                           },
"get_common_prefix":
{ Args: { "p_delimiter": string,"p_key": string,"p_prefix": string }; Returns: string
                           },
"get_size_by_bucket":
{ Args: { "delete_markers"?: string,"noncurrent_versions"?: string }; Returns: {
              "bucket_id": string,"size": number
            }[]
                           },
"list_multipart_uploads_with_delimiter":
{ Args: { "bucket_id": string,"delimiter_param": string,"max_keys"?: number,"next_key_token"?: string,"next_upload_token"?: string,"prefix_param": string,"raw_prefix_param"?: string }; Returns: {
              "created_at": string,"id": string,"key": string
            }[]
                           },
"list_objects_with_delimiter":
{ Args: { "_bucket_id": string,"delete_markers"?: string,"delimiter_param": string,"max_keys"?: number,"next_token"?: string,"next_token_archived_at"?: string,"next_token_version"?: string,"noncurrent_versions"?: string,"prefix_param": string,"sort_order"?: string,"start_after"?: string }; Returns: {
              "archived_at": string,"created_at": string,"id": string,"is_delete_marker": boolean,"is_versioned": boolean,"last_accessed_at": string,"metadata": Json,"name": string,"updated_at": string,"version": string
            }[]
                           },
"operation":
{ Args: Record<PropertyKey, never>; Returns: string
                           },
"search":
{ Args: { "bucketname": string,"delete_markers"?: string,"levels"?: number,"limits"?: number,"noncurrent_versions"?: string,"offsets"?: number,"prefix": string,"search"?: string,"sortcolumn"?: string,"sortorder"?: string }; Returns: {
              "archived_at": string,"created_at": string,"id": string,"is_delete_marker": boolean,"is_versioned": boolean,"last_accessed_at": string,"metadata": Json,"name": string,"updated_at": string,"version": string
            }[]
                           },
"search_by_timestamp":
{ Args: { "delete_markers"?: string,"noncurrent_versions"?: string,"p_bucket_id": string,"p_level": number,"p_limit": number,"p_prefix": string,"p_sort_column": string,"p_sort_column_after": string,"p_sort_order": string,"p_start_after": string,"p_start_after_version"?: string }; Returns: {
              "archived_at": string,"created_at": string,"id": string,"is_delete_marker": boolean,"is_versioned": boolean,"key": string,"last_accessed_at": string,"metadata": Json,"name": string,"updated_at": string,"version": string
            }[]
                           },
"search_v2":
{ Args: { "bucket_name": string,"delete_markers"?: string,"levels"?: number,"limits"?: number,"noncurrent_versions"?: string,"prefix": string,"sort_column"?: string,"sort_column_after"?: string,"sort_order"?: string,"start_after"?: string,"start_after_archived_at"?: string,"start_after_is_continuation"?: boolean,"start_after_version"?: string }; Returns: {
              "archived_at": string,"created_at": string,"id": string,"is_delete_marker": boolean,"is_versioned": boolean,"key": string,"last_accessed_at": string,"metadata": Json,"name": string,"updated_at": string,"version": string
            }[]
                           }
          }
          Enums: {
            "buckettype": "STANDARD"|"ANALYTICS"|"VECTOR"
          }
          CompositeTypes: {
            [_ in never]: never
          }
        }
}

type DatabaseWithoutInternals = Omit<Database, '__InternalSupabase'>

type DefaultSchema = DatabaseWithoutInternals[Extract<keyof Database, "public">]

export type Tables<
  DefaultSchemaTableNameOrOptions extends
    | keyof (DefaultSchema["Tables"] & DefaultSchema["Views"])
    | { schema: keyof DatabaseWithoutInternals },
  TableName extends DefaultSchemaTableNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof (DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"] &
        DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Views"])
    : never = never
> = DefaultSchemaTableNameOrOptions extends { schema: keyof DatabaseWithoutInternals }
  ? (DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"] &
      DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Views"])[TableName] extends {
      Row: infer R
    }
    ? R
    : never
  : DefaultSchemaTableNameOrOptions extends keyof (DefaultSchema["Tables"] & DefaultSchema["Views"])
  ? (DefaultSchema["Tables"] & DefaultSchema["Views"])[DefaultSchemaTableNameOrOptions] extends {
      Row: infer R
    }
    ? R
    : never
  : never

export type TablesInsert<
  DefaultSchemaTableNameOrOptions extends
    | keyof DefaultSchema["Tables"]
    | { schema: keyof DatabaseWithoutInternals },
  TableName extends DefaultSchemaTableNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"]
    : never = never
> = DefaultSchemaTableNameOrOptions extends { schema: keyof DatabaseWithoutInternals }
  ? DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"][TableName] extends {
      Insert: infer I
    }
    ? I
    : never
  : DefaultSchemaTableNameOrOptions extends keyof DefaultSchema["Tables"]
  ? DefaultSchema["Tables"][DefaultSchemaTableNameOrOptions] extends {
      Insert: infer I
    }
    ? I
    : never
  : never

export type TablesUpdate<
  DefaultSchemaTableNameOrOptions extends
    | keyof DefaultSchema["Tables"]
    | { schema: keyof DatabaseWithoutInternals },
  TableName extends DefaultSchemaTableNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"]
    : never = never
> = DefaultSchemaTableNameOrOptions extends { schema: keyof DatabaseWithoutInternals }
  ? DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"][TableName] extends {
      Update: infer U
    }
    ? U
    : never
  : DefaultSchemaTableNameOrOptions extends keyof DefaultSchema["Tables"]
  ? DefaultSchema["Tables"][DefaultSchemaTableNameOrOptions] extends {
      Update: infer U
    }
    ? U
    : never
  : never

export type Enums<
  DefaultSchemaEnumNameOrOptions extends
    | keyof DefaultSchema["Enums"]
    | { schema: keyof DatabaseWithoutInternals },
  EnumName extends DefaultSchemaEnumNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[DefaultSchemaEnumNameOrOptions["schema"]]["Enums"]
    : never = never
> = DefaultSchemaEnumNameOrOptions extends { schema: keyof DatabaseWithoutInternals }
  ? DatabaseWithoutInternals[DefaultSchemaEnumNameOrOptions["schema"]]["Enums"][EnumName]
  : DefaultSchemaEnumNameOrOptions extends keyof DefaultSchema["Enums"]
  ? DefaultSchema["Enums"][DefaultSchemaEnumNameOrOptions]
  : never

export type CompositeTypes<
  PublicCompositeTypeNameOrOptions extends
    | keyof DefaultSchema["CompositeTypes"]
    | { schema: keyof DatabaseWithoutInternals },
  CompositeTypeName extends PublicCompositeTypeNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[PublicCompositeTypeNameOrOptions["schema"]]["CompositeTypes"]
    : never = never
> = PublicCompositeTypeNameOrOptions extends { schema: keyof DatabaseWithoutInternals }
  ? DatabaseWithoutInternals[PublicCompositeTypeNameOrOptions["schema"]]["CompositeTypes"][CompositeTypeName]
  : PublicCompositeTypeNameOrOptions extends keyof DefaultSchema["CompositeTypes"]
  ? DefaultSchema["CompositeTypes"][PublicCompositeTypeNameOrOptions]
  : never

export const Constants = {
  "public": {
          Enums: {
            "app_role": ["admin", "steward", "engineer", "analyst"]
          }
        },"storage": {
          Enums: {
            "buckettype": ["STANDARD", "ANALYTICS", "VECTOR"]
          }
        }
} as const
