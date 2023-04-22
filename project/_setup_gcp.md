# GCP

## Create a GCP account

Google provides 300 USD free credit available for 90 days to new accounts.

## Create a new project in GCP

When creating a [Google Cloud Platform project](https://console.cloud.google.com/cloud-resource-manager), use a project id that is unique.

## Create a service account

Note: Service accounts authorize applications to perform authorized API calls.
Go to IAM -> Service accounts
- Add one service account
  - Fill the details
  - Add Viewer role (plain viewer role)
  - No need to grant access to multiple users
- Create keys in the service account
  - Actions icon -> Manage Keys -> AddKey -> Create new key -> Create
  - Save the json in a safe directory in your local computer e.g. to `~/.gc/<credentials>`

## Set up permissions to the service account for GCS and BigQuery

- Go to IAM & Admin -> IAM
- Edit the service account icon -> Edit principal
- Add the following roles:
  - Storage Admin
  - Storage Object Admin
  - BigQuery Admin
  - Viewer (just 'Viewer')
  
## Google DSK

- Install the [Google Cloud SDK](https://cloud.google.com/sdk/docs/install-sdk)
- Let the [environment variable point to your GCP key](https://cloud.google.com/docs/authentication/application-default-credentials#GAC), authenticate it and refresh the session token

## Enable the rest of the APIs 
Be sure to have enabled the following APIs for your project in the GCP account.
- https://console.cloud.google.com/apis/library
  - Compute Engine
  - Cloud Storage
  - BigQuery  
  