FROM debian:sid-slim

RUN set -ex; \
    mkdir -p /usr/share/man/man1/; \
    apt-get update; \
    apt-get install --yes --no-install-recommends openjdk-11-jdk git wget unzip; \
    rm -rf /var/lib/apt/lists/*; 

ENV ANDROID_SDK_ROOT="/app/sdk" \
    ANDROID_HOME="/app/sdk" \
    JAVA_TOOL_OPTIONS="-Dfile.encoding=UTF8"

RUN set -ex; \
    mkdir -p "/app/sdk/licenses" "/app/sdk/ndk" "/app/simplebitcoinwallet/"; \
    printf "\n24333f8a63b6825ea9c5514f83c2829b004d1fee" > "/app/sdk/licenses/android-sdk-license"; \
    cd /app/sdk/;
   
WORKDIR /app/simplebitcoinwallet/wallet/

CMD ./gradlew assembleRelease
