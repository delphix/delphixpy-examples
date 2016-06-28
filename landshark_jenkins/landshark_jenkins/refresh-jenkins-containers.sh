TEMPLATE_NAME="Masked SugarCRM Application"
CONTAINER_NAME="Sugar Automated Testing Container"

TEMPLATE_NAME_URL==${TEMPLATE_NAME// /%20}
CONTAINER_NAME_URL==${CONTAINER_NAME// /%20}

curl -sS "http://linuxtarget:5777/job/Refresh%20Container/buildWithParameters?TEMPLATE_NAME=${TEMPLATE_NAME_URL}&CONTAINER_NAME=${CONTAINER_NAME_URL}"


until [ "`curl -sS http://linuxtarget:5777/job/Refresh%20Container/api/json| jq '(.lastBuild.number == .lastCompletedBuild.number) and (.inQueue == false)'`" == "true" ]; do 
	echo "Waiting for ${CONTAINER_NAME} to finish refresh."
	sleep 5
done

echo "${CONTAINER_NAME} refreshed"
