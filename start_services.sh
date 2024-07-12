cd data_pipeline

if [ $1 == "controller" ]; then
    echo "Runner controller cli with args ${@:2}"
    python work_queue_service_cli.py "${@:2}"
elif [ $1 == "server" ]; then
    echo "Running web api server"
    uvicorn work_queue_web_api:app --reload --port=80 --host 0.0.0.0
elif [ $1 == "version" ]; then
    full_version=$(pip freeze | grep data-pipeline)
    version=${full_version##*==}
    echo $version 
else
    echo "First argument must be 'controller', 'server', or 'version'"
fi
