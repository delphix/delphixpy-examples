echo "Deleting VDB's"
curl -sS 'http://linuxtarget:5777/job/Delete%20VDB/buildWithParameters?VDB_GROUP=Jenkins%20Test%20VBDs&VDB_NAME='

until [ "`curl -sS http://linuxtarget:5777/job/Delete%20VDB/api/json| jq '(.lastBuild.number == .lastCompletedBuild.number) and (.inQueue == false)'`" == "true" ]; do 
	echo "Waiting for VDB's to be deleted."
	sleep 1
done

echo "vdbs deleted"