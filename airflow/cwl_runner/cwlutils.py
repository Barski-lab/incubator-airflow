# CWL utils
# File with help functions.
import urlparse
import os
import glob
from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException

def shortname(n):
    # return n.split("#")[-1].split("/")[-1]  # What if we have two outputs from the different tools with the same name?
    return n.split("#")[-1]                   # It looks like we don't need to cut by "/"

def flatten(input_list):
    result = []
    for i in input_list:
        if isinstance(i,list): result.extend(flatten(i))
        else: result.append(i)
    return result

def url_shortname(inputid):
    d = urlparse.urlparse(inputid)
    if d.fragment:
        return d.fragment.split(u"/")[-1]
    else:
        return d.path.split(u"/")[-1]

def conf_get_default (section, key, default):
    try:
        return conf.get(section, key)
    except AirflowConfigException:
        return default


def get_only_file (folder):
    return [filename for filename in glob.iglob(folder+"/*") if os.path.isfile(filename)]