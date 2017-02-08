echo 'Deploying: '$1' '$2
echo curl -vX POST $1?force=true -d @$2 --header "Content-Type: application/json"
curl -vX POST $1?force=true -d @$2 --header "Content-Type: application/json"