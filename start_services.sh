if [ $1 == "controller" ]
then
python ./data_pipeline/work_queue_service_cli.py "${@:2}"
elif [ $1 == "server"]
then
python ./data_pipeline/work_queue_web_api.py "${@:2}"
else
echo "First argument must be `controller` or `server`"
fi
