#!/bin/bash
CONTENTDIR=`pwd`
JOB_XML=Jenkins_Bookmark_Container.xml
JOB_NAME="Bookmark+Container"
VERSION="2.0.0.001"

cat ${CONTENTDIR}/${JOB_XML} | curl -X POST "http://linuxtarget:5777/createItem?name=${JOB_NAME}" --header "Content-Type: application/xml" -d @-