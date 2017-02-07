echo 'Deploying: '$1
echo curl -vX POST http://pemc-dcos.southeastasia.cloudapp.azure.com/marathon/v2/apps?force=true -d @$1 --header "Content-Type: application/json"
curl -vX POST http://pemc-dcos.southeastasia.cloudapp.azure.com/marathon/v2/apps?force=true -d @$1 --header "Content-Type: application/json"