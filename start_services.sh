if [ $1 == "controller" ]; then
    echo "Runner controller cli with args ${@:2}"
    python ./data_pipeline/work_queue_service_cli.py "${@:2}"
elif [ $1 == "server" ]; then
    echo "Running web api server with args: ${@:2}"
    python ./data_pipeline/work_queue_web_api.py "${@:2}"
else
    echo "First argument must be 'controller' or 'server'"
fi
