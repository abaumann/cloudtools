from subprocess import check_output, call
import googleapiclient.discovery
from google.cloud import storage
import os

def init_parser(parser):
    parser.add_argument('name', type=str, help='Cluster name.')
    parser.add_argument('script', type=str)
    parser.add_argument('--hash', default='latest', type=str,
                        help='Hail build to use for notebook initialization (default: %(default)s).')
    parser.add_argument('--spark', default='2.0.2', type=str, choices=['2.0.2', '2.1.0'],
                        help='Spark version used to build Hail (default: %(default)s).')
    parser.add_argument('--version', default='0.1', type=str, choices=['0.1', 'devel'],
                        help='Hail version to use (default: %(default)s).')
    parser.add_argument('--jar', required=False, type=str, help='Custom Hail jar to use.')
    parser.add_argument('--zip', required=False, type=str, help='Custom Hail zip to use.')
    parser.add_argument('--files', required=False, type=str, help='Comma-separated list of files to add to the working directory of the Hail application.')
    parser.add_argument('--properties', '-p', required=False, type=str, help='Extra Spark properties to set.')
    parser.add_argument('--args', type=str, help='Quoted string of arguments to pass to the Hail script being submitted.')

    # gcloud overrides
    default_project = check_output(['gcloud', 'config', 'get-value', 'core/project']).rstrip()
    parser.add_argument('--project', default=default_project, help='Google project to override the default specified by gcloud.')

    default_region = check_output(['gcloud', 'config', 'get-value', 'compute/region']).rstrip()
    parser.add_argument('--region', default=default_region, help='Google region to override the default specified by gcloud.')

def main(args):
    print("Submitting to cluster '{}'...".format(args.name))

    dataproc = googleapiclient.discovery.build('dataproc', 'v1')   
    cluster = dataproc.projects().regions().clusters().get(
            projectId=args.project,
            region=args.region,
            clusterName=args.name).execute()
            
    cluster_staging_bucket = cluster["config"]["configBucket"]
    
    # get Hail hash using either most recent, or an older version if specified
    if args.hash != 'latest':
        hash_name = args.hash
    else:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket("hail-common")
        blob = bucket.blob( "latest-hash.txt" )
        # get the hash from this text file, removing any trailing newline
        hash_name = blob.download_as_string().rstrip()
        
    # Hail jar
    if args.jar:
        hail_jar = args.jar.rsplit('/')[-1]
        jar_path = args.jar
        # upload this jar to the staging bucket
        upload_blob(cluster_staging_bucket, jar_path, os.path.basename(jar_path))
    else:
        hail_jar = 'hail-{0}-{1}-Spark-{2}.jar'.format(args.version, hash_name, args.spark)
        jar_path = 'gs://hail-common/builds/{0}/jars/{1}'.format(args.version, hail_jar)

    # Hail zip
    if args.zip:
        zip_path = args.zip
        # upload this zip to the staging bucket
        upload_blob(cluster_staging_bucket, zip_path, os.path.basename(zip_path))
    else:
        hail_zip = 'hail-{0}-{1}.zip'.format(args.version, hash_name)
        zip_path = 'gs://hail-common/builds/{0}/python/{1}'.format(args.version, hail_zip)

    # create files argument
    files = jar_path
    uploaded_file_paths = []
    if args.files:
        files += ',' + args.files
        # upload each of these files to the staging bucket
        for f in args.files.split(","):
            uploaded_file_paths.append(upload_blob(cluster_staging_bucket, f, os.path.basename(f)))

    # create properties argument
    properties = 'spark.driver.extraClassPath=./{0},spark.executor.extraClassPath=./{0}'.format(hail_jar)
    if args.properties:
        properties = properties + ',' + args.properties

    # pyspark submit command
    cmd = [
        'gcloud',
        'dataproc',
        'jobs',
        'submit',
        'pyspark',
        args.script,
        '--cluster={}'.format(args.name),
        '--files={}'.format(files),
        '--py-files={}'.format(zip_path),
        '--properties={}'.format(properties)
    ]

    arg_array = []
    # append arguments to pass to the Hail script
    if args.args:
        arg_array = args.args.split()
        cmd.append('--')
        for x in arg_array:
            cmd.append(x)

    # print underlying gcloud command
    print('gcloud command:')
    print(' '.join(cmd[:6]) + ' \\\n    ' + ' \\\n    '.join(cmd[6:]))

    # submit job
    #call(cmd)
        
    # upload the hail script to this dataproc staging bucket
    script_name = os.path.basename(args.script)            
    upload_blob(cluster_staging_bucket, args.script, script_name)
                           
    job_details = {
        'projectId': args.project,
        'job': {
            'placement': {
                'clusterName': args.name
            },
            # https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.jobs#pysparkjob
            'pysparkJob': {
                "mainPythonFileUri": "gs://{}/{}".format(cluster_staging_bucket, script_name),
                "args": arg_array,
                "pythonFileUris": [
				    "gs://hail-common/pyhail-hail-is-master-{}.zip".format(hash_name),
				    "gs://{}/{}".format(cluster_staging_bucket, script_name)
                ],
                "jarFileUris": [
				    jar_path
                ],
                "fileUris": [
 				    uploaded_file_paths
                 ],
                 "archiveUris": [
				    ["gs://{}/{}".format(cluster_staging_bucket, zip_path)]
                ],
                "properties": {
				    "spark.driver.extraClassPath":"./{}".format(hail_jar),
				    "spark.executor.extraClassPath":"./{}".format(hail_jar)
                },
                # "loggingConfig": {
# 				    object(LoggingConfig)
#                 },
            } 
        }
    }
    
    if args.properties:
        # convert the properties format to a dict that can be pushed into the job json
        properties_json = job_details['job']['pysparkJob']['properties']
        # note we join the properties and then split because properties before this point is a mix
        # of a list of individual properties and the comma separated string that comes in from args
        for k,v in dict(s.split('=') for s in args.properties.split(",")).iteritems():
            properties_json[k] = v
    
    
    result = dataproc.projects().regions().jobs().submit(
        projectId=args.project,
        region=args.region,
        body=job_details).execute()
    job_id = result['reference']['jobId']
    
    job_url = "https://console.cloud.google.com/dataproc/jobs/{}?project={}&region={}".format(job_id, args.project, args.region)
    
    print('Submitted job ID {}.  See {} for more details.'.format(job_id, job_url))

def wait_for_job(dataproc, project, region, job_id):
    print('Waiting for job to finish...')
    while True:
        result = dataproc.projects().regions().jobs().get(
            projectId=project,
            region=region,
            jobId=job_id).execute()
        # Handle exceptions
        if result['status']['state'] == 'ERROR':
            raise Exception(result['status']['details'])
        elif result['status']['state'] == 'DONE':
            print('Job finished.')
            return result

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print('File {} uploaded to {}.'.format(
        source_file_name,
        destination_blob_name)) 
    
    return "g://{}/{}".format(bucket_name, destination_blob_name)
