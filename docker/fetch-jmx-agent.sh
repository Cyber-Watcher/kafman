#!/usr/bin/env bash
#
# Downloads the Prometheus JMX exporter Java agent used by the test cluster.
# The jar is not committed to the repository, so this runs before the stack starts.

set -euo pipefail

cd "$(dirname "$0")"

VERSION="1.0.1"
JAR="lib/jmx_prometheus_javaagent.jar"
URL="https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${VERSION}/jmx_prometheus_javaagent-${VERSION}.jar"

if [[ -s "$JAR" ]]; then
	echo "jmx agent already present at docker/$JAR — nothing to do"
	exit 0
fi

mkdir -p lib

echo "==> downloading jmx_prometheus_javaagent ${VERSION}"
if command -v curl >/dev/null 2>&1; then
	curl -fSL --retry 3 -o "$JAR.tmp" "$URL"
elif command -v wget >/dev/null 2>&1; then
	wget -q -O "$JAR.tmp" "$URL"
else
	echo "neither curl nor wget is available" >&2
	exit 1
fi

# A truncated download would fail at container start with a confusing error.
if [[ ! -s "$JAR.tmp" ]]; then
	rm -f "$JAR.tmp"
	echo "download produced an empty file" >&2
	exit 1
fi

mv "$JAR.tmp" "$JAR"
chmod 644 "$JAR"
echo "written to docker/$JAR ($(du -h "$JAR" | cut -f1))"
