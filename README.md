# omnata-sync

## What is this package?

This dbt package contains pre-built models and macros for using [Omnata Sync](https://omnata.com) in a dbt project.

Omnata Sync is a Snowflake native application that provides direct data syncs between your Snowflake account and your SaaS apps.

## How do I get started?

### Omnata installation

First, install Omnata Sync from the Snowflake Marketplace.

After creating a sync and choosing dbt as the scheduler, you will be provided with a dbt model definition.

When branching, by default your dbt target name will be used to match the Omnata branch name. If you wish to specify your branch,
provide this variable:

```

dbt run  --vars '{"omnata_branch":"my_branch"}'


```

### dbt project initial setup

1) Add the omnata-sync package as a dependancy in your `packages.yml`:

```

packages:
  - git: "https://github.com/omnata-labs/dbt-omnata-sync.git"
    revision: main

```

2) run `dbt deps`.

### What else can Omnata do?

Omnata is changing the way that data integration works, by removing complex middleware and providing native capabilities to your existing apps and data warehouses.

To find out more or to contact us, visit our [website](http://omnata.com).

### Alternate Omnata installation location

If you install the Omnata Sync Engine under a different application name than the default OMNATA_SYNC_ENGINE, you can configure that in dbt_project.yml like so:

```

vars:
  omnata_application_name: "OMNATA_APP_JAMES"

```
