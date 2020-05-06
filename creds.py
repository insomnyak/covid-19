from google.cloud import storage

def storageClient():
    bucket_name = "light-yagami-dtxyz"
    archive = "archive/"
    target = "target/"
    staging = "staging/"
    log = "log/"
    try:
        client = storage.Client.from_service_account_json('/Users/kamil/Documents/insomnyak-llc/development/creds/gcp-credentials/data-science-covid-19-a32ae9152d9b.json')
        print("using json creds")
    except:
        client = storage.Client()
        print("using auto creds")
    return client, bucket_name, staging, target, archive, log
