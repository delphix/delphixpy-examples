#!/bin/bash
CONTENTDIR=`pwd`
VIEW_XML=Jenkins_CI_Pipeline.xml
VIEW_NAME="CI+Pipeline"
VERSION="2.0.0.001"

cat ${CONTENTDIR}/${VIEW_XML} | curl -X POST "http://linuxtarget:5777/createView?name=${VIEW_NAME}" --header "Content-Type: application/xml" -d @-