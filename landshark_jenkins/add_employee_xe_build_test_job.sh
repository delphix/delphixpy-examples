#!/bin/bash
CONTENTDIR=`pwd`
JOB_XML=Jenkins_Employee_XE_Build_Test.xml
JOB_NAME="Employee+XE+Build+Test"
VERSION="2.0.0.001"

cat ${CONTENTDIR}/${JOB_XML} | curl -X POST "http://linuxtarget:5777/createItem?name=${JOB_NAME}" --header "Content-Type: application/xml" -d @-