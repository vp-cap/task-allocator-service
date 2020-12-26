echo $1
if [ "$1" = "bo" ]
then
    echo ${GIT_USER}
    echo ${GIT_PASS}
    docker build --build-arg GIT_USER=${GIT_USER} --build-arg GIT_PASS=${GIT_PASS} -t task-allocator-service .
elif [ "$1" = "br" ]
then
    docker build --build-arg GIT_USER=${GIT_USER} --build-arg GIT_PASS=${GIT_PASS} -t task-allocator-service .
    docker stop task-allocator-service && docker rm task-allocator-service
    docker run --network=common --name task-allocator-service -p 50052:50052 task-allocator-service
else
    docker stop task-allocator-service && docker rm task-allocator-service
    docker run --network=common --name task-allocator-service -p 50052:50052 task-allocator-service
fi
