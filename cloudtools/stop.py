import googleapiclient.discovery
from subprocess import call, check_output
import time
import sys
from googleapiclient.errors import HttpError

def init_parser(parser):
    parser.add_argument('name', type=str, help='Cluster name.')
    # gcloud overrides
    default_project = check_output(['gcloud', 'config', 'get-value', 'core/project']).rstrip()
    parser.add_argument('--project', default=default_project, help='Google project to override the default specified by gcloud.')

    default_region = check_output(['gcloud', 'config', 'get-value', 'compute/region']).rstrip()
    parser.add_argument('--region', default=default_region, help='Google region to override the default specified by gcloud.')

def main(args):
    cmd = ['gcloud', 'dataproc', 'clusters', 'delete', '--quiet', args.name]
    
    print('gcloud command:')
    print(' '.join(cmd))
    
    dataproc = googleapiclient.discovery.build('dataproc', 'v1')
    
    cluster_url = "https://console.cloud.google.com/dataproc/clusters/{}?project={}&region={})".format(args.name, args.project, args.region)

    print("Stopping cluster '{}'...".format(args.name))

    try:
        result = dataproc.projects().regions().clusters().delete(
            projectId=args.project,
            region=args.region,
            clusterName=args.name).execute()
    except HttpError as e:
        # if the error is not a 404, then we want to see that error, otherwise it means the 
        # cluster is already deleted and the finally below will handle that
        if e.resp.status not in [404]:
            raise
    finally:
        while True:
            result = dataproc.projects().regions().clusters().list(
                projectId=args.project,
                region=args.region).execute()
            
            if 'clusters' in result:
                cluster_list = result['clusters']
                cluster = [c for c in cluster_list if c['clusterName'] == args.name][0]
        
                try:
                    if cluster['status']['state'] == 'ERROR':
                        raise Exception(cluster['status'])
                    elif cluster['status']['state'] == 'DELETING':
                        time.sleep(5)
                        sys.stdout.write('...')
                        sys.stdout.flush()
                except Exception as e:
                    print "Unable to delete cluster.  See {} for more details.".format(cluster_url) 
                    raise 
            else:
               print("\nCluster deleted.")
               break 