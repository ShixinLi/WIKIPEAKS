import json
from datetime import datetime, timedelta as td
from subprocess import call, check_output

emr_start_str = 'aws emr create-cluster --release-label emr-5.0.0 ' \
                '--instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m3.xlarge ' \
                '--service-role EMR_DefaultRole --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole ' \
                '--name "DailyViews Cluster" --enable-debugging --log-uri s3://wikipedia-pg-logs/ --auto-terminate'

cluster_response = check_output(emr_start_str, shell=True)
cluster_response_json = json.loads(cluster_response)

try:
    cluster_id = cluster_response_json['ClusterId']
    cluster_id = cluster_id.encode('utf8')

    # Create a data range
    list_dates = []
    d1 = datetime(2015, 12, 23)
    d2 = datetime(2015, 12, 25)

    delta = d2 - d1

    for i in range(delta.days + 1):
        list_dates.append(d1 + td(days=i))

    for current_date in list_dates:
        current_date_string = current_date.strftime('%Y-%m-%d')

        emr_add_step_command = 'aws emr add-steps ' \
                               '--cluster-id %s ' \
                               '--steps file://emr_step_config.json' % cluster_id

        # Creates the Step Configuration
        step_config = '[{ "Name": "Daily_Views_Step_%s", ' \
                      '"Type": "STREAMING",' \
                      '"ActionOnFailure": "CONTINUE",  ' \
                      '"Args": ' \
                      '["-files","s3://wikipedia-scripts/revised_map.py,' \
                      's3://wikipedia-scripts/revised_reduce.py",' \
                      '"-mapper","revised_map.py",' \
                      '"-reducer","revised_reduce.py",' \
                      '"-input","s3://wikipedia-pageviews/%s",' \
                      '"-output","s3://wikipedia-dly-pageviews/%s"]}]' % (current_date_string,
                                                                          current_date_string,
                                                                          current_date_string)

        #Transforms the step_config string to JSON
        step_config_json = json.loads(step_config)

        # Save the File as JSON
        with open("emr_step_config.json", 'w') as out_json:
            json.dump(step_config_json, out_json)

        cluster_response = check_output(emr_add_step_command, shell=True)

except KeyError:
    print "ERROR. EMR Cluster command could not init"
