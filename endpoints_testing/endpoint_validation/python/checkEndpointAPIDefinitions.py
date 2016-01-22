'''
Created on Dec 31, 2015

Copyright 2015, Institute for Systems Biology.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

@author: michael
'''
import copy
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
        if not os.path.isdir(log_dir):
            os.makedirs(log_dir)
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
    log.info('\tfinished saving %s into %s\n' % (name, outdir))

def requestURLContent(url, log):
    h = httplib2.Http()
    log.info('\trequesting %s content' % (url))
    _, content = h.request(url)
    log.info('\treceived %s content\n' % (url))
    return json.loads(content)

def addtoerrors(curerrors, curerror, args, diffcount, log):
    curerrors += curerror % args
    diffcount -= 1
    if 0 == diffcount:
        log.error('exceeded max error count\n:%s' % (curerrors))
    return curerrors, diffcount

def checkListTags(config, key, curitem, previtem, diffcount, diffreport, log):
    diffreport, diffcount = checkListStringTags(key, curitem, previtem, diffcount, diffreport, log)
    if 0 >= diffcount:
        return diffreport, diffcount
    diffreport, diffcount = checkListMapTags(config, key, curitem, previtem, diffcount, diffreport, log)
    if 0 >= diffcount:
        return diffreport, diffcount

    # at some point a list will not contain a nested list
    curlist = []
    for tag in curitem:
        if isinstance(tag, list):
            curlist += [tag]
    prevlist = []
    for tag in previtem:
        if isinstance(tag, list):
            prevlist += [tag]
    if 0 < len(curlist):
        diffreport, diffcount = checkListTags(config, key, curlist, prevlist, diffcount, diffreport, log)
        if 0 >= diffcount:
            return diffreport, diffcount
    elif 0 < len(prevlist):
        diffreport, diffcount = addtoerrors(diffreport, u'\t\t\tprevious list for \'%s\' had these additional keys: %s\n', (key, u', '.join(prevlist)), diffcount, log) 

    return diffreport, diffcount

def checkListMapTags(config, prevkey, curlistmap, prevlistmap, diffcount, diffreport, log):
    # find a common key in all the maps
    maps = []
    for item in curlistmap:
        if isinstance(item, dict):
            maps += [item]
    if 0 == len(maps):
        return diffreport, diffcount 
    shared_keys = set(maps[0].keys())
    # get candidates that are in all the current maps
    for nextmap in maps[1:]:
        shared_keys &= set(nextmap.keys())
    
    if 0 == len(shared_keys):
        diffreport, diffcount = addtoerrors(diffreport, u'\t\t\tdidn\'t find shared element to use as key for \'%s\' in checkListMapTags()\n', prevkey, diffcount, log)
        if not diffcount:
            return diffreport, diffcount
    
    # find one from the candidates that has unique values for all the maps
    for shared_key in shared_keys:
        unique = set()
        for nextmap in maps:
            if not isinstance(nextmap[shared_key], list):
                unique.add(nextmap[shared_key])
        if len(unique) == len(maps):
            break
    
    curmap = {}
    for item in curlistmap:
        if isinstance(item, dict):
            curmap[item[shared_key]] = item
    
    prevmap = {}
    for item in prevlistmap:
        if isinstance(item, dict):
            prevmap[item[shared_key]] = item
    
    for key, curitem in curmap.iteritems():
        curkey = prevkey + ':' + key if 0 < len(prevkey) else key
        log.info('\t\tlooking at current map with key %s' % (curkey))
        try:
            previtem = prevmap.pop(key)
        except:
            diffreport, diffcount = addtoerrors(diffreport, u'\t\t\tdidn\'t discover key \'%s\' in previous map\n', curkey, diffcount, log)
            if not diffcount:
                return diffreport, diffcount
            continue
        stringtags, maptags, listtags = classifyMapTag(curitem)
        diffreport, diffcount = checkMapStringTags(key, stringtags, curitem, previtem, diffcount, diffreport, log)
        if 0 >= diffcount:
            return diffreport, diffcount
        diffreport, diffcount = checkMapMapTags(config, key, maptags, curitem, previtem, diffcount, diffreport, log)
        if 0 >= diffcount:
            return diffreport, diffcount
        diffreport, diffcount = checkMapListTags(config, key, listtags, curitem, previtem, diffcount, diffreport, log)
        if 0 >= diffcount:
            return diffreport, diffcount
        
        if 0 < len(previtem):
            diffreport, diffcount = addtoerrors(diffreport, u'\t\t\tprevious version for \'%s\' had these additional fields: %s\n', 
                (key, u', '.join(('\'%s\' of type %s' % (str(item), str(type(previtem[item])).split(' ')[1][:-1])) for item in previtem)), diffcount, log)
            if not diffcount:
                return diffreport, diffcount
    
    if 0 < len(prevmap):
        diffreport, diffcount = addtoerrors(diffreport, u'\t\t\tprevious map for \'%s\' had these additional keys: %s\n', (curkey, u', '.join(prevmap.keys())), diffcount, log) 

    return diffreport, diffcount

def checkListStringTags(key, curitem, previtem, diffcount, diffreport, log):
    curstrings = []
    for tag in curitem:
        if isinstance(tag, basestring) or isinstance(tag, bool):
            curstrings += [tag]
    prevstrings = []
    for tag in previtem:
        if isinstance(tag, basestring) or isinstance(tag, bool):
            prevstrings += [tag]
    
    for tag in curstrings:
        try:
            if tag not in prevstrings:
                diffreport, diffcount = addtoerrors(diffreport, u'\t\t\tfound \'%s\' in current version when not in previous for %s\n', (tag, key), diffcount, log)
            else:
                prevstrings.pop(tag)
        except Exception as e:
            diffreport, diffcount = addtoerrors(diffreport, u'\t\t\terror processing tag \'%s\' for endpoint \'%s\':\n\t\t\t\t\'%s\'\n', (tag, key, e), diffcount, log)
            if not diffcount:
                return diffreport, diffcount
    
    if 0 < len(prevstrings):
        diffreport, diffcount = addtoerrors(diffreport, u'\t\t\tprevious string list for \'%s\' had these additional values: %s\n', (key, u', '.join(prevstrings)), diffcount, log) 
        if not diffcount:
            return diffreport, diffcount

    return diffreport, diffcount

def classifyMapTag(curitem):
    stringtags = []
    maptags = []
    listtags = []
    for tag in curitem:
        if isinstance(curitem[tag], basestring) or isinstance(curitem[tag], bool):
            stringtags += [tag]
        elif isinstance(curitem[tag], dict):
            maptags += [tag]
        elif isinstance(curitem[tag], list):
            listtags += [tag]
        else:
            raise ValueError('%s was an unexpected type: %s' % (tag, type(curitem[tag])))
    
    return stringtags, maptags, listtags

def checkMapListTags(config, key, listtags, curlistmap, prevlistmap, diffcount, diffreport, log):
    for tag in listtags:
        curkey = key + ':' + tag if 0 < len(key) else tag
        try:
            if tag not in prevlistmap:
                diffreport, diffcount = addtoerrors(diffreport, u'\t\t\tfound \'%s\' list in current version when not in previous for %s\n', (tag, curkey), diffcount, log)
            else:
                previtemlist = prevlistmap.pop(tag)
                diffreport, diffcount = checkListTags(config, key + ':' + tag, curlistmap[tag], previtemlist, diffcount, diffreport, log)
        except Exception as e:
            diffreport, diffcount = addtoerrors(diffreport, u'\t\t\tdidn\'t discover tag \'%s\' for endpoint \'%s\' in map \'%s\' in previous version:\n\t\t\t\t\'%s\'\n', (curlistmap[tag], str(tag), curkey, e), diffcount, log)
            if not diffcount:
                return diffreport, diffcount
        if 0 < len(prevlistmap):
            diffreport, diffcount = addtoerrors(diffreport, u'\t\t\tprevious version for \'%s\' map \'%s\' had these additional fields: %s\n', 
                (curkey, tag, u', '.join(('\'%s\' of type %s' % (previtem, str(type(previtem)).split(' ')[1][:-1])) for previtem in prevlistmap)), diffcount, log)
            if not diffcount:
                return False

    return diffreport, diffcount

def checkMapMapTags(config, key, maptags, curitem, previtem, diffcount, diffreport, log):
    for tag in maptags:
        previtemmap = {}
        try:
            if tag not in previtem:
                diffreport, diffcount = addtoerrors(diffreport, u'\t\t\tfound \'%s\' map in current version when not in previous for %s\n', (tag, key), diffcount, log)
            else:
                previtemmap = previtem.pop(tag)
                stringtags, maptags, _ = classifyMapTag(curitem[tag])
                diffreport, diffcount = checkMapStringTags(key + ':' + tag, stringtags, curitem[tag], previtemmap, diffcount, diffreport, log)
                diffreport, diffcount = checkMapMapTags(config, key + ':' + tag, maptags, curitem[tag], previtemmap, diffcount, diffreport, log)
                if isinstance(curitem[tag], list):
                    diffreport, diffcount = checkListTags(config, key + ':' + tag, curitem[tag], previtemmap[tag], diffcount, diffreport, log)
        except Exception as e:
            diffreport, diffcount = addtoerrors(diffreport, u'\t\t\tdidn\'t discover tag \'%s\' for endpoint \'%s\' in map \'%s\' in previous version:\n\t\t\t\t\'%s\'\n', (curitem[tag], str(tag), key, e), diffcount, log)
            if 0 >= diffcount:
                return diffreport, diffcount
        if tag in previtem:
            _, maptags, _ = classifyMapTag(previtem[tag])
            if 0 < len(maptags):
                diffreport, diffcount = addtoerrors(diffreport, u'\t\t\tprevious version for \'%s\' map \'%s\' had these additional fields: %s\n', 
                    (key, tag, u', '.join(('\'%s\' of type %s' % (previtem, str(type(previtemmap[previtem])).split(' ')[1][:-1])) for previtem in previtemmap)), diffcount, log)
                if not diffcount:
                    return diffreport, diffcount

    return diffreport, diffcount

def checkMapStringTags(key, stringtags, curitem, previtem, diffcount, diffreport, log):
    for tag in stringtags:
        try:
            if tag not in previtem:
                diffreport, diffcount = addtoerrors(diffreport, u'\t\t\tfound \'%s\' in current version when not in previous for %s\n', (tag, key), diffcount, log)
            else:
                prevtag = previtem.pop(tag)
                if curitem[tag] != prevtag:
                    diffreport, diffcount = addtoerrors(diffreport, u'\t\t\ttag \'%s\' for endpoint \'%s\' not equal between version: \'%s\' != \'%s\'\n', (tag, key, curitem[tag], prevtag), diffcount, log)
                    if not diffcount:
                        return diffreport, diffcount
        except Exception as e:
            diffreport, diffcount = addtoerrors(diffreport, u'\t\t\terror processing tag \'%s\' for endpoint \'%s\':\n\t\t\t\t\'%s\'\n', (tag, key, e), diffcount, log)
            if not diffcount:
                return diffreport, diffcount
    
    return diffreport, diffcount

def validate_endpoint_methods(filename, current, config, log):
    if not os.path.isfile(config['outdir'] + filename + '.txt'):
        log.info('did not find %s to compare, saving current version' % (filename + '.txt'))
        return True
    with open(config['outdir'] + filename + '.txt') as prevfile:
        prev = json.loads(''.join(prevfile.readlines()))
    
    log.info('validating endpoint: %s' % (filename[:filename.index('.')]))
    diffreport, _ = checkListTags(config, '', [current], [prev], 50, '', log)
    if 0 < len(diffreport):
        log.error('\t\tmismatches between previous endpoint api version and current version found:\n%s' % (diffreport))
    return False

def processMethod(key, method, endpoint_body):
    endpoint_body += '\t\t\t' + key + '\n\t\t\t\t' + method['path'] + '\t' + method['id'] + '\t' + method['httpMethod'] + '\t' + ','.join(method['scopes']) + '\n'
    if 'parameters' not in method:
        return endpoint_body
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
    
    return endpoint_body

def get_endpoints_methods(website, endpoint, version, config, log):
    return requestURLContent(config['ENDPOINT_URL'] % (website, endpoint, version), log)

def processEndpoint(endpoint_discovery, website, config, report, reportfile, log):
    endpoint = endpoint_discovery['name']
    version = endpoint_discovery['version']
    endpoint_info = get_endpoints_methods(website, endpoint, version, config, log)
    if validate_endpoint_methods(endpoint + '.' + version, endpoint_info, config, log):
        save2file(config, log, endpoint_info, endpoint + '.' + version)
# get the top-level information on the endpoint from the discovery document
    endpoint_keys = report['endpoint_def']['columns']
    endpoint_body = '\t'
    for key in endpoint_keys:
        endpoint_body += endpoint_discovery[key] + '\t'
    
    endpoint_body += '\n'
# and the more fine grained detailed information form the endpoint specific document
    top_resources = endpoint_info['resources']
    for named_endpoints in top_resources.itervalues():
        if 'resources' not in named_endpoints:
            continue
        resources = named_endpoints['resources']
        for key, endpoint in resources.iteritems():
            endpoint_body += '\t\t' + key + '\n'
            if 'methods' not in endpoint:
                continue
            for key, method in endpoint['methods'].iteritems():
                endpoint_body = processMethod(key, method, endpoint_body)
    
    reportfile.write(endpoint_body + '\n')

def processEndpoints(api_list, website, config, log):
    report = config['endpoint_report']
    with open(config['outdir'] + report['filename'], 'w') as reportfile:
        reportfile.write(report['header'] % (website) + '\n')
        for item in api_list['items']:
            processEndpoint(item, website, config, report, reportfile, log)

def validate_discovery_list(current, config, log):
    if not os.path.isfile(config['outdir'] + config['discovery_filename'] + '.txt'):
        log.info('did not find %s to compare, saving current version' % (config['discovery_filename'] + '.txt'))
        return True
    log.info('\tstart validating the discovery endpoint list')
    with open(config['outdir'] + config['discovery_filename'] + '.txt') as prevfile:
        prev = json.loads(''.join(prevfile.readlines()))
    prevcopy = copy.deepcopy(prev)
    
    diffreport, _ = checkListMapTags(config, u'', current['items'], prev['items'], 50, u'', log)
    if 0 < len(diffreport):
        log.error('\t\tmismatches between previous endpoint discovery version and current version found:\n%s' % (diffreport))
    
    log.info('checking from root\n\n')
    diffreport, _ = checkListTags(config, '', [current], [prevcopy], 50, '', log)
    if 0 < len(diffreport):
        log.error('\t\tmismatches between previous endpoint discovery version and current version found:\n%s' % (diffreport))
    
    log.info('\tfinished validating the discovery endpoint list')
    return False

def get_endpoints_list(website, config, log):
    return requestURLContent(config['ENDPOINT_LIST_URL'] % (website), log)

def processDiscovery(website, config, log):
# make sure output directory exists
    if not os.path.isdir(config['outdir']):
        os.makedirs(config['outdir'])
# Discovery the endpoints and get information on each one
    api_list = get_endpoints_list(website, config, log)
    log.info('discovery api json:\n%s' % (json.dump(api_list, sys.stderr, indent=2)))
    if validate_discovery_list(api_list, config, log):
        save2file(config, log, api_list, config['discovery_filename'])
    return api_list

def openLog(config):
    try:
        log_dir = str(date.today()).replace('-', '_') + '_' + config['log_dir_tag'] + '/'
        log_name = create_log(log_dir, 'discovery')
        log = logging.getLogger(log_name)
    except Exception as e:
        print 'problem in creating the log for validation'
        traceback.print_exc(limit=5)
        raise e
    return log

def openConfig(configFileName):
    try:
        with open(configFileName) as configFile:
            config = json.load(configFile)
    except Exception as e:
        print 'problem opening config file'
        raise e
    return config

def main(configFileName, website):
    config = openConfig(configFileName)
    log = openLog(config)
    log.info('%s verify API against current for %s' % (datetime.now(), website))

    api_list = processDiscovery(website, config, log)
    processEndpoints(api_list, website, config, log)

    log.info(str(datetime.now()) + ' finished verifying API against current for %s' % (website))

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
