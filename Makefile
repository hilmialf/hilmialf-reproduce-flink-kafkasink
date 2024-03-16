include properties

image: gradle-build docker-build push

gradle-build:
	./gradlew clean build

docker-build:
	docker build --platform linux/amd64 -t "${IMAGE_NAME}" . \
    --network host \
    --build-arg JAVA_VERSION=${JAVA_VERSION}

# Push the docker image
push:
	docker push ${IMAGE_NAME}