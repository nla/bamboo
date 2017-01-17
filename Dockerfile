FROM maven:3-jdk-8-alpine

# Add the source:
ADD . /usr/src/app
WORKDIR /usr/src/app

# To ensure the dependencies are packaged up we need to use /usr/share/maven/ref/settings-docker.xml
# As per https://github.com/carlossg/docker-maven#packaging-a-local-repository-with-the-image
RUN mvn -B -s /usr/share/maven/ref/settings-docker.xml dependency:resolve package

# Pre-build the classpath file:
RUN mvn -pl ui dependency:build-classpath -q -s /usr/share/maven/ref/settings-docker.xml -Dmdep.outputFile=target/classpath

# Run the server
CMD ./bin/bamboo server

