'''
Created on Dec 31, 2015

@author: michael
'''
from datetime import date
from datetime import datetime
import httplib2
import json
import logging
import os
import sys
import traceback

def create_log(log_dir, log_name):
    try:
        # creates the logs for the run
        logger = logging.getLogger(log_name)
        logger.setLevel(logging.DEBUG)
        log = logging.FileHandler(log_dir + log_name + '.txt', 'w')
        log.setLevel(logging.DEBUG)
        slog = logging.StreamHandler()
        slog.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')
        log.setFormatter(formatter)
        slog.setFormatter(formatter)
        logger.addHandler(log)
        logger.addHandler(slog)
        return log_name
    except:
        print 'ERROR: couldn\'t create log %s in %s' % (log_name, log_dir)

def save2file(config, log, info, name):
    outdir = config['outdir']
    log.info('\tsaving %s into %s' % (name, outdir))
    with open(outdir + name + '.txt', 'w') as outfile:
        json.dump(info, outfile, indent = 2)
    log.info('\tfinished saving %s into %s' % (name, outdir))

def requestURLContent(url, log):
    h = httplib2.Http()
    log.info('\trequesting %s content' % (url))
    _, content = h.request(url)
    log.info('\treceived %s content' % (url))
    return json.loads(content)

def get_endpoints_methods(website, endpoint, version, config, log):
    return requestURLContent(config['ENDPOINT_URL'] % (website, endpoint, version), log)

def get_endpoints_list(website, config, log):
    return requestURLContent(config['ENDPOINT_LIST_URL'] % (website), log)

def main(configFileName, website):
    try:
        with open(configFileName) as configFile:
            config = json.load(configFile)
        log_dir = str(date.today()).replace('-', '_') + '_' + config['log_dir_tag'] + '/'
        log_name = create_log(log_dir, 'discovery')
        log = logging.getLogger(log_name)
    except Exception as e:
        print 'problem in creating the log for validation'
        traceback.print_exc(limit = 5)
        raise e
    log.info('%s verify API against current for %s' % (datetime.now(), website))

    # make sure output directory exists
    if not os.path.isdir(config['outdir']):
        os.makedirs(config['outdir'])
    # Discory the endpoints and get information on each one
    api_list = get_endpoints_list(website, config, log)
    save2file(config, log, api_list, 'discovery')
    report = config['endpoint_report']
    endpoint_keys = report['endpoint_def']['columns']
    with open(config['outdir'] + report['filename'], 'w') as reportfile:
        reportfile.write(report['header'] % (website) + '\n')
        for item in api_list['items']:
            endpoint = item['name']
            version = item['version']
            endpoint_info = get_endpoints_methods(website, endpoint, version, config, log)
            save2file(config, log, endpoint_info, endpoint + '.' + version)
    
            # get the top-level information on the endpoint from the discovery document
            endpoint_body = '\t'
            for key in endpoint_keys:
                endpoint_body += item[key] + '\t'
            endpoint_body += '\n'
            # and the more fine grained detailed information form the endpoint specific document
            top_resources = endpoint_info['resources']
            for named_endpoints in top_resources.itervalues():
                if 'resources' not in named_endpoints:
                    continue
                resources = named_endpoints['resources']
                for key, endpoint in resources.iteritems():
                    endpoint_body += '\t\t' + key + '\n'
                    for key, method in endpoint['methods'].iteritems():
                        endpoint_body += '\t\t\t' + key + '\n\t\t\t\t' + method['path'] + '\t' + method['id'] + '\t' + method['httpMethod'] + '\t' + ','.join(method['scopes']) + '\n'
                        if 'parameters' not in method:
                            continue
                        endpoint_body += '\t\t\t\tparameters:\n'
                        parameters = method['parameters']
                        if 'parameterOrder' in method:
                            parameterOrder = method['parameterOrder']
                            for paramname in parameterOrder:
                                parameter = parameters.pop(paramname)
                                endpoint_body += '\t\t\t\t\t' + paramname + '\t' + parameter['type'] + '\trequired: ' + ('true' if 'required' in parameter and parameter['required'] else 'false') + '\n'
                        for paramname in parameters:
                            parameter = parameters[paramname]
                            endpoint_body += '\t\t\t\t\t' + paramname + '\t' + parameter['type'] + '\trequired: ' + ('true' if 'required' in parameter and parameter['required'] else 'false') + '\n'
            reportfile.write(endpoint_body + '\n')

#       "delete": {
#        "id": "cohort_api.cohort_endpoints.cohort.delete",
#        "path": "delete_cohort",
#        "httpMethod": "POST",
#        "parameters": {
#         "cohort_id": {
#          "type": "string",
#          "required": true,
#          "format": "int64",
#          "location": "query"
#         },
#         "token": {
#          "type": "string",
#          "location": "query"
#         }
#        },
#        "parameterOrder": [
#         "cohort_id"
#        ],
#        "response": {
#         "$ref": "ApiCohortReturnJSON"
#        },
#        "scopes": [
#         "https://www.googleapis.com/auth/userinfo.email"
#        ]
#       },
    log.info(str(datetime.now()) + ' finished verifying API against current for %s' % (website))

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
