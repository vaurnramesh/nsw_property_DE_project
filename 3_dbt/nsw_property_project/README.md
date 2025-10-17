# NSW Property dbt Project

## Quick Start

### Essential Commands
```bash
# Install dbt dependencies
dbt deps

# Load seed data (postcode to GCCSA mappings)
dbt seed --select postcode_to_gccsa

# Run all models
dbt run

# Run specific layers
dbt run --select staging.*
dbt run --select core.*
dbt run --select marts.*

# Test models
dbt test
dbt test --select core.dim_*
dbt test --select fact_sales

# Full build (run + test)
dbt build

# Compile SQL without running
dbt compile

# Debug connection issues
dbt debug

# Clean compiled files
dbt clean
```

---

## Configuration

- **dbt profiles location**: `~/.dbt/profiles.yml`
- **Project name**: `nsw_property_project`
- **Default target**: `dev`
- **BigQuery location**: `australia-southeast1`

---

## Resources

- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices