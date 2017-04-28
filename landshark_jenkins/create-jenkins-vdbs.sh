TARGET_GROUP="Jenkins Test VBDs"

TARGET_GROUP_URL==${TARGET_GROUP// /%20}

echo "Creating some vdbs"
VDBS=(a b c d)
#VDBS=(a)
for each in ${VDBS[*]}; do
curl -sS "http://linuxtarget:5777/job/Create%20Oracle%20VDB/buildWithParameters?DB_NAME=auto${each}&TARGET_GROUP=TARGET_GROUP_URL"
done

until [ "`curl -sS http://linuxtarget:5777/job/Create%20Oracle%20VDB/api/json| jq '(.lastBuild.number == .lastCompletedBuild.number) and (.inQueue == false)'`" == "true" ]; do 
	echo "Waiting for VDB's to be created."
	sleep 5
done

echo "VDB's created"
