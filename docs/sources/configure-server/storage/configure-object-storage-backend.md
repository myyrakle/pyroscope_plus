---
title: "Configure object storage backend"
menuTitle: "Configure object storage"
description: "Learn how to configure Pyroscope to use different object storage backend implementations."
aliases:
  - /docs/phlare/latest/configure-server/configure-object-storage-backend/
  - ../configure-object-storage-backend/ # https://grafana.com/docs/pyroscope/latest/configure-server/configure-object-storage-backend/
---

# Configure object storage backend

Grafana Pyroscope can use different object storage services to persist blocks containing the profiles data.
Blocks are flushed by ingesters [on disk](https://grafana.com/docs/pyroscope/<PYROSCOPE_VERSION>/configure-server/storage/configure-disk-storage/) first then are uploaded to the object store.

The supported backends are:

- [Amazon S3](https://aws.amazon.com/s3/) and compatible implementations like [MinIO](https://min.io/)
- [Google Cloud Storage](https://cloud.google.com/storage)
- [Azure Blob Storage](https://azure.microsoft.com/es-es/services/storage/blobs/)
- [Swift (OpenStack Object Storage)](https://wiki.openstack.org/wiki/Swift)
- Self-managed ClickHouse

> For S3, GCS, Azure, and Swift, Pyroscope uses [Thanos' object store client], so their stated limitations apply.

[Thanos' object store client]: https://github.com/thanos-io/objstore#supported-providers-clients

## ClickHouse

The ClickHouse backend stores object manifests and fixed-size binary chunks in
ClickHouse. The current schema uses node-local `MergeTree` tables and requires
exactly one stable native-protocol endpoint. It does not provide ClickHouse
replication or endpoint failover, so use S3, GCS, or Azure when the object store
must remain available after losing a ClickHouse node.

```yaml
storage:
  backend: clickhouse
  clickhouse:
    addresses: clickhouse-storage:9000
    database: pyroscope
    objects_table: pyroscope_objects
    chunks_table: pyroscope_object_chunks
    auto_create_tables: true
    cleanup:
      enabled: false # Enable on one designated process only.
```

The backend creates the manifest table, chunk table, latest-state aggregate
table, and latest-state materialized view when `auto_create_tables` is enabled.
For production, bootstrap and verify the schema first, then disable automatic
creation during normal service startup. `partition_count` is fixed at 64; changing
it or upgrading a legacy ClickHouse object-store schema requires recreating all
four objects.

Latest-object reads use `argMaxMerge` over append-only aggregate states, so they
do not depend on background `MergeTree` merge completion and do not require
`FINAL`. Reader prefetch is bounded by both `read_prefetch_chunks` and
`max_read_prefetch_bytes`.

Cleanup is disabled by default. It uses synchronous, partition-scoped mutations
and limits total deletions per pass. In a distributed Pyroscope deployment,
enable cleanup on one designated process only and leave it disabled on other
processes that open the same tables. Back up the ClickHouse database before
schema changes, and monitor the `pyroscope_objstore_clickhouse_*` metrics for
operation latency, failures, commit retries, chunk query volume, and cleanup
mutation duration.

## Amazon S3

To use an AWS S3 or S3-compatible bucket for long term storage, you can find Pyroscope's configuration parameters [in the reference config][aws_ref]. Apart from those parameters, it is also possible to supply configuration  parameters using [the well-known environment variables][aws_enf] of the AWS SDK.

At a minimum, you will need to provide values for the `bucket_name`, `endpoint`, `access_key_id`, and `secret_access_key` keys.

[aws_ref]: ../../reference-configuration-parameters/#s3_storage_backend
[aws_enf]: https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html

### Example using an AWS Bucket

This how one would configure a bucket in the AWS region `eu-west-2`:

```yaml
storage:
  backend: s3
  s3:
    bucket_name: #REPLACE_WITH_BUCKET_NAME
    region: eu-west-2
    endpoint: s3.eu-west-2.amazonaws.com
    access_key_id: #REPLACE_WITH_ACCESS_KEY
    secret_access_key: #REPLACE_WITH_SECRET_KEY
```

### Example using a S3 compatible Bucket

This how one would configure a bucket on a locally running instance of [MinIO]:

```yaml
storage:
  backend: s3
  s3:
    bucket_name: grafana-pyroscope-data
    endpoint: localhost:9000
    insecure: true
    access_key_id: grafana-pyroscope-data
    secret_access_key: grafana-pyroscope-data
```

[MinIO]: https://min.io/docs/minio/container/index.html

### Using AWS SDK auth

Set `native_aws_auth_enabled: true` to use the [AWS SDK default credential chain](https://docs.aws.amazon.com/sdkref/latest/guide/standardized-credentials.html).

```yaml
storage:
  backend: s3
  s3:
    bucket_name: your-bucket
    region: eu-west-2
    endpoint: s3.eu-west-2.amazonaws.com
    native_aws_auth_enabled: true
```

## Google Cloud Storage

To use a Google Cloud Storage (GCS) bucket for long term storage, you can find Pyroscope's configuration parameters [in the reference config][gcs_ref].

[gcs_ref]: ../../reference-configuration-parameters/#gcs_storage_backend

At a minimum, you will need to provide a values for the `bucket_name` and a service account. To supply the service account there are two ways:

* Use the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to locate your [application credentials](https://cloud.google.com/docs/authentication/production).
* Provide the content the service account key within the `service_account` parameter.

### Example using a Google Cloud Storage bucket

This how one would configure a GCS bucket using the `service_account` parameter:

```yaml
storage:
  backend: gcs
  gcs:
    bucket_name: grafana-pyroscope-data
    service_account: |
      {
        "type": "service_account",
        "project_id": "PROJECT_ID",
        "private_key_id": "KEY_ID",
        "private_key": "-----BEGIN PRIVATE KEY-----\nPRIVATE_KEY\n-----END PRIVATE KEY-----\n",
        "client_email": "SERVICE_ACCOUNT_EMAIL",
        "client_id": "CLIENT_ID",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://accounts.google.com/o/oauth2/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/SERVICE_ACCOUNT_EMAIL"
      }
```

## Azure Blob Storage

To use an Azure Blob Storage bucket for long term storage, you can find Pyroscope's configuration parameters [in the reference config][azure_ref].

[azure_ref]: ../../reference-configuration-parameters/#azure_storage_backend

If `user_assigned_id` is used, authentication is done via user-assigned managed identity.

[//TODO]: <> (Provide example with and without user-assigned managed identity)

## Swift (OpenStack Object Storage)

To use a Swift (OpenStack Object Storage) bucket for long term storage, you can find Pyroscope's configuration parameters [in the reference config][swift_ref].

[swift_ref]: ../../reference-configuration-parameters/#swift_storage_backend

>If the `name` of a user, project or tenant is used one must also specify its domain by ID or name. Various examples for OpenStack authentication can be found in the [official documentation](https://developer.openstack.org/api-ref/identity/v3/index.html?expanded=password-authentication-with-scoped-authorization-detail#password-authentication-with-unscoped-authorization).

[//TODO]: <> (Provide example)
