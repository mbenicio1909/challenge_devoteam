# challenge_devoteam

Challenge:

1. Your solution needs to minimum detect one Atomic type (either Integer, string, timestamp or any other atomic type). - Done
2. Your solution should detect both repeated RECORDs and non-repeated RECORDs (i.e. json objects as value and arrays of objects as value). - Done
3. Your detected schema must be inline with BigQuery schema specification notation and not the industry standard json schema. - Done
4. You must at least create one table in BigQuery using your detected schema to demonstrate BigQuery accepting your generated schema. - Done
5. Your code must be ready to be deployed on the cloud - Done

Optionally you can extend your solution to do the following:
1. Update the schema of an existing BigQuery Table. - Partially done
2. Extend your solution to support all the possible atomic types. - Partially done
3. Extend your solution to load the data to the table after the table’s schema
is updated. - Done
4. Implement the solution using Apache Beam - Not done

## Solution:
- Implemented this python via Cloud Function
- Authentication via Service Account
- Trigger is set to be a new upload in Cloud Bucket

## Pending List:
- Validation for cases hein the JSON changes (merge schemas)
- Create the cloud functions via Terraform (IAAS)
- Improve Functioning for large files 
- Implement microbatching for case of a big name of files and call via pub / sub
- Implement moving to tmp and archive + timestamp in the archive file / check for files in tmp before restarting

