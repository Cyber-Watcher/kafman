#!/usr/bin/env bash
#
# Populates the test cluster with topics, SCRAM users, ACLs, messages and
# consumer groups — including groups that are deliberately lagging, so kafman's
# monitoring screens have something to show.
#
# Idempotent: safe to run repeatedly.
#
# Usage:  ./docker/init.sh          (the stack must already be up)

set -euo pipefail

cd "$(dirname "$0")"

COMPOSE="docker compose -f compose.yml"
# Admin commands run inside the network over the INTERNAL listener, because the
# external listeners advertise localhost, which is not routable between containers.
BOOTSTRAP="kafka-1:19092"

# KAFKA_OPTS is cleared: the CLI tools inherit the server's options and would try
# to bind the JMX exporter port a second time.
kafka() {
	local script="$1"
	shift
	$COMPOSE exec -T -e KAFKA_OPTS= kafka-1 "/opt/kafka/bin/$script" "$@"
}

log() { printf '\n\033[36m==> %s\033[0m\n' "$*"; }

# ---------------------------------------------------------------- readiness ---

log "waiting for the cluster"
for _ in $(seq 1 60); do
	if kafka kafka-broker-api-versions.sh --bootstrap-server "$BOOTSTRAP" >/dev/null 2>&1; then
		break
	fi
	sleep 2
done
kafka kafka-broker-api-versions.sh --bootstrap-server "$BOOTSTRAP" >/dev/null 2>&1 || {
	echo "cluster did not become ready" >&2
	exit 1
}

# ------------------------------------------------------------------- topics ---

# name : partitions : replication factor : extra configs
TOPICS=(
	"orders.events:12:3:cleanup.policy=delete,retention.ms=604800000"
	"payments.tx:6:3:cleanup.policy=delete,min.insync.replicas=2"
	"user.profile:8:3:cleanup.policy=compact"
	"audit.log:24:3:cleanup.policy=delete,retention.ms=86400000"
	"user.clicks:12:3:cleanup.policy=delete"
	"notif.outbox:4:3:cleanup.policy=compact,delete"
	"dlq.orders:4:3:cleanup.policy=delete"
	"search.index:6:2:cleanup.policy=delete"
	# Single replica on purpose: goes under-replicated the moment its broker stops,
	# which is how the Overview health panel gets exercised.
	"metrics.raw:3:1:cleanup.policy=delete"
	# Read and written continuously by the "live" profile (make stack-live). Kept
	# apart from the topics above on purpose: those carry exact record counts the
	# tests assert on, and a running producer would move them.
	"live.orders:6:3:cleanup.policy=delete,retention.ms=3600000"
)

log "creating topics"
existing=$(kafka kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list 2>/dev/null | tr -d '\r')
for entry in "${TOPICS[@]}"; do
	IFS=: read -r name parts rf configs <<<"$entry"
	if grep -qx "$name" <<<"$existing"; then
		echo "  = $name (exists)"
		continue
	fi
	args=(--bootstrap-server "$BOOTSTRAP" --create --topic "$name"
		--partitions "$parts" --replication-factor "$rf")
	IFS=, read -ra cfgs <<<"$configs"
	for cfg in "${cfgs[@]}"; do
		# "cleanup.policy=compact,delete" is a single config with a comma in its
		# value; stitch it back together.
		if [[ $cfg == delete && ${args[-1]} == cleanup.policy=compact ]]; then
			args[-1]="cleanup.policy=compact,delete"
			continue
		fi
		args+=(--config "$cfg")
	done
	kafka kafka-topics.sh "${args[@]}" >/dev/null
	echo "  + $name ($parts partitions, RF $rf)"
done

# -------------------------------------------------------------- SCRAM users ---

# username : mechanism : password
USERS=(
	"admin:SCRAM-SHA-512:admin-secret"
	"kafman:SCRAM-SHA-512:kafman-secret"
	"kafman:SCRAM-SHA-256:kafman-secret"
	"billing-worker:SCRAM-SHA-512:billing-secret"
	"readonly-audit:SCRAM-SHA-256:readonly-secret"
)

log "creating SCRAM users"
for entry in "${USERS[@]}"; do
	IFS=: read -r user mech pass <<<"$entry"
	kafka kafka-configs.sh --bootstrap-server "$BOOTSTRAP" \
		--alter --entity-type users --entity-name "$user" \
		--add-config "$mech=[password=$pass]" >/dev/null
	echo "  + $user ($mech)"
done

# --------------------------------------------------------------------- ACLs ---

log "creating ACLs"
acl() {
	kafka kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --add "$@" >/dev/null 2>&1 ||
		echo "  ! failed: $*"
}

acl --allow-principal User:billing-worker --operation Read --operation Describe --topic orders.events
acl --allow-principal User:billing-worker --operation Read --group "billing-" --resource-pattern-type prefixed
acl --allow-principal User:readonly-audit --operation Describe --topic '*'
acl --allow-principal User:kafman --operation All --topic '*'
acl --allow-principal User:kafman --operation All --group '*'
# Cluster-level Describe/DescribeConfigs: broker roles (process.roles) on the
# Overview tab come from each broker's configuration.
acl --allow-principal User:kafman --operation Describe --operation DescribeConfigs --cluster
# A Deny entry so the ACL screen has a red row to render.
acl --deny-principal User:readonly-audit --operation Write --topic payments.tx
echo "  ACL entries: $(kafka kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --list 2>/dev/null | grep -c 'permissionType' || true)"

# ----------------------------------------------------------------- messages ---

# Produces JSON records so the record viewer and the future query language have
# structured data to work with.
produce() {
	local topic="$1" count="$2" template="$3"
	local payload=""
	for i in $(seq 1 "$count"); do
		payload+="$(printf "$template" "$i" "$i")"$'\n'
	done
	printf '%s' "$payload" | $COMPOSE exec -T -e KAFKA_OPTS= kafka-1 \
		/opt/kafka/bin/kafka-console-producer.sh \
		--bootstrap-server "$BOOTSTRAP" --topic "$topic" \
		--property "parse.key=true" --property "key.separator=|" >/dev/null 2>&1
}

log "producing messages"
produce orders.events 500 'ord-%04d|{"id":"ord-%04d","status":"NEW","amount":149.5,"items":[{"sku":"A-1002","qty":2}]}'
echo "  + orders.events: 500"
produce payments.tx 300 'pay-%04d|{"txId":"pay-%04d","state":"SETTLED","currency":"RUB","amount":2400}'
echo "  + payments.tx: 300"
produce audit.log 800 'evt-%04d|{"seq":%d,"actor":"system","action":"LOGIN","result":"OK"}'
echo "  + audit.log: 800"
produce user.clicks 400 'usr-%04d|{"user":"u-%04d","page":"/checkout","ms":128}'
echo "  + user.clicks: 400"
produce dlq.orders 12 'ord-fail-%04d|{"id":"ord-fail-%04d","status":"FAILED","error":"payment declined"}'
echo "  + dlq.orders: 12"
produce metrics.raw 200 'm-%04d|{"metric":"cpu","value":%d}'
echo "  + metrics.raw: 200"

# ---------------------------------------------------------- consumer groups ---

# Consuming only part of a topic leaves a measurable lag behind.
consume_partially() {
	local group="$1" topic="$2" max="$3"
	kafka kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" \
		--topic "$topic" --group "$group" --from-beginning \
		--max-messages "$max" --timeout-ms 20000 >/dev/null 2>&1 || true
}

log "creating consumer groups"
consume_partially billing-worker orders.events 120
consume_partially billing-worker payments.tx 80
echo "  + billing-worker (lagging on orders.events and payments.tx)"

consume_partially search-indexer user.clicks 350
echo "  + search-indexer (small lag on user.clicks)"

consume_partially audit-archiver audit.log 800
echo "  + audit-archiver (caught up)"

consume_partially old-consumer dlq.orders 12
echo "  + old-consumer (idle, empty state)"

# The groups above are Empty: their consumers have exited and only their
# committed offsets remain. Live members — with hosts, assignments and Stable or
# Rebalancing states — come from the "live" profile.
# Five more containers is a lot on a small CI runner; KAFMAN_SKIP_LIVE=1 leaves
# them out, and the tests that need them skip themselves.
log "starting the live consumer groups"
if [ "${KAFMAN_SKIP_LIVE:-}" = 1 ]; then
	echo "  = skipped (KAFMAN_SKIP_LIVE=1)"
elif $COMPOSE --profile live up -d >/dev/null 2>&1; then
	echo "  + live-producer      → live.orders, ~20 records/s"
	echo "  + live-orders-fast   3 members, keeps up"
	echo "  + live-orders-slow   1 member, falls behind on purpose"
else
	echo "  ! could not start the live profile; run make stack-live by hand"
fi

# ------------------------------------------------------------------ summary ---

log "summary"
echo "topics:"
kafka kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list 2>/dev/null | sed 's/^/  /'
echo
echo "consumer groups and lag:"
kafka kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP" --all-groups --describe 2>/dev/null |
	awk 'NR==1 || $6 ~ /^[0-9]+$/' | head -20 | sed 's/^/  /'

cat <<'EOF'

test cluster is ready.

  PLAINTEXT       localhost:9092,localhost:9192,localhost:9292
  SASL_PLAINTEXT  localhost:9093,localhost:9193,localhost:9293   PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512
  SASL_SSL        localhost:9094,localhost:9194,localhost:9294   CA: docker/certs/ca.crt
  JMX metrics     http://localhost:9404/metrics (and :9405, :9406)

  users: admin/admin-secret · kafman/kafman-secret · readonly-audit/readonly-secret

  live groups: live-orders-fast (3 members, caught up) and live-orders-slow (1 member, lagging)
               on live.orders; stop them with make stack-live-down

Run kafman against it:  ./kafman --config docker/kafman.dev.yaml
EOF
