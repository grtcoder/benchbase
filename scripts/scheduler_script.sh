#!/bin/bash

# Get current time
current_minute=$(date +%M)
current_hour=$(date +%H)
current_day=$(date +%d)
current_month=$(date +%m)
current_weekday=$(date +%u)

# Calculate 1 minute later
next_minute=$((10#${current_minute} + 1))
next_hour=$current_hour
next_day=$current_day
next_month=$current_month
next_weekday=$current_weekday

if [ "$next_minute" -ge 60 ]; then
    next_minute=0
    next_hour=$((10#${next_hour} + 1))

    if [ "$next_hour" -ge 24 ]; then
        next_hour=0
        next_day=$((10#${next_day} + 1))
        # NOTE: Not handling month rollover in cron (edge case)
    fi
fi

# Format time for cron
cron_time="$(printf '%02d' $next_minute) $(printf '%02d' $next_hour) * * *"

# Create script directory and files
SCRIPT_DIR="$HOME/go_service_scripts"
START_SCRIPT="$SCRIPT_DIR/start_services.sh"
LOG_FILE="$SCRIPT_DIR/log.txt"

mkdir -p "$SCRIPT_DIR"

# Create the actual script to run
cat > "$START_SCRIPT" <<EOF
#!/bin/bash

echo "Starting Broker..." >> "$LOG_FILE"
go run \$HOME/services/broker/main.go -directoryIP=localhost -directoryPort 8080 -brokerIP=localhost -brokerPort 8081 >> "$LOG_FILE" 2>&1 &

sleep 2

echo "Starting Server..." >> "$LOG_FILE"
go run \$HOME/services/server/main.go -directoryIP=localhost -directoryPort 8080 -serverIP=localhost -serverPort 8082 >> "$LOG_FILE" 2>&1 &
EOF

chmod +x "$START_SCRIPT"

# Add the cron job (without duplication)
(crontab -l 2>/dev/null | grep -Fv "$START_SCRIPT"; echo "$cron_time $START_SCRIPT") | crontab -

echo "✅ Cron job scheduled to run services at $(printf '%02d' $next_hour):$(printf '%02d' $next_minute)"
echo "📄 Script path: $START_SCRIPT"
echo "📜 Output log: $LOG_FILE"
