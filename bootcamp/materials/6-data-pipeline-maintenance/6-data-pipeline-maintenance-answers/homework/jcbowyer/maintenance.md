# Week 5 Data Pipeline Maintenance

## Team Responsibilities

### Business Areas and Pipelines
You are part of a team of 4 data engineers managing 5 pipelines that cover the following business areas:

1. **Profit**
   - **Unit-level profit needed for experiments**
   - **Aggregate profit reported to investors**
2. **Growth**
   - **Aggregate growth reported to investors**
   - **Daily growth needed for experiments**
3. **Engagement**
   - **Aggregate engagement reported to investors**

---

## Ownership Plan

### Primary and Secondary Owners

| Pipeline                                | Primary Owner | Secondary Owner |
|-----------------------------------------|---------------|-----------------|
| Unit-level profit needed for experiments | Engineer 1    | Engineer 2      |
| Aggregate profit reported to investors   | Engineer 2    | Engineer 1      |
| Aggregate growth reported to investors   | Engineer 3    | Engineer 4      |
| Daily growth needed for experiments      | Engineer 4    | Engineer 3      |
| Aggregate engagement reported to investors | Engineer 1    | Engineer 4      |

---

## On-Call Schedule

### Fair Rotation
- On-call duty rotates weekly among the four engineers.
- Each engineer is on-call for 1 week per month.
- To account for holidays:
  - Engineers swap on-call weeks in advance if a holiday falls on their assigned week.
  - A backup on-call engineer (secondary owner) is designated for critical support during holidays.

| Week         | Primary On-Call | Backup On-Call |
|--------------|------------------|----------------|
| Week 1       | Engineer 1       | Engineer 2     |
| Week 2       | Engineer 2       | Engineer 3     |
| Week 3       | Engineer 3       | Engineer 4     |
| Week 4       | Engineer 4       | Engineer 1     |

---

## Run Books for Pipelines Reporting Metrics to Investors

### Run Book Template
Each run book includes:
- **Pipeline Description**
- **Critical Metrics**
- **Potential Failure Scenarios**
- **Monitoring Tools**
- **Escalation Contacts**

### Run Book: Aggregate Profit Reported to Investors
- **Pipeline Description**: Processes and aggregates unit-level profit data for monthly investor reports.
- **Critical Metrics**: Data freshness, accuracy of profit calculations, timely delivery.
- **Potential Failure Scenarios**:
  - **Data Load Issues**: Missing data from upstream systems.
  - **Schema Changes**: Upstream source schema mismatch.
  - **Performance Bottlenecks**: Slow aggregations due to increased data volume.
  - **Delivery Failures**: Reports not sent to the designated system or email.
- **Monitoring Tools**: Data pipeline logs, data quality checks, SLA monitoring.
- **Escalation Contacts**: Engineer 2 (Primary), Engineer 1 (Backup).

### Run Book: Aggregate Growth Reported to Investors
- **Pipeline Description**: Aggregates daily growth data into monthly reports for investors.
- **Critical Metrics**: Timely delivery, accurate growth trends, reconciliation with historical data.
- **Potential Failure Scenarios**:
  - **Data Inconsistencies**: Mismatched growth trends.
  - **ETL Failures**: Job failures during transformations.
  - **Data Latency**: Delays in receiving upstream data.
- **Monitoring Tools**: Alerts for job failures, data reconciliation scripts, SLA dashboards.
- **Escalation Contacts**: Engineer 3 (Primary), Engineer 4 (Backup).

### Run Book: Aggregate Engagement Reported to Investors
- **Pipeline Description**: Compiles user engagement metrics into monthly aggregate reports.
- **Critical Metrics**: Accurate engagement rates, timely data delivery, and SLA compliance.
- **Potential Failure Scenarios**:
  - **Data Quality Issues**: Inaccurate user activity metrics.
  - **Pipeline Breaks**: Errors in processing raw engagement data.
  - **Delivery Failures**: Missed deadlines for monthly reports.
- **Monitoring Tools**: Pipeline monitoring, anomaly detection, SLA tracking.
- **Escalation Contacts**: Engineer 1 (Primary), Engineer 4 (Backup).

---

## Summary
This plan ensures clear ownership, a fair on-call schedule, and actionable run books for critical pipelines reporting metrics to investors. The structured approach minimizes risks and facilitates efficient issue resolution.
