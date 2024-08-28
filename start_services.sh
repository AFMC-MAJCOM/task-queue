if [ $1 == "controller" ]; then
    echo "Runner controller cli with args ${@:2}"
    python task_queue/cli/work_queue_service_cli.py "${@:2}"
elif [ $1 == "server" ]; then
    echo "Running web api server"
    python3 -m uvicorn task_queue.api.work_queue_web_api:app --port=8001 --host 0.0.0.0
else
    echo "First argument must be 'controller' or 'server'"
fi
