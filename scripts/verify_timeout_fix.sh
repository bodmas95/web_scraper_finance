#!/bin/bash
# =============================================================================
# Verification Script for Timeout Fix
# =============================================================================
# This script verifies that all timeout fixes are properly deployed
# Run this after deploy_websocket_fix.sh to confirm everything is configured

echo "=========================================="
echo "  Timeout Fix Verification"
echo "  $(date)"
echo "=========================================="
echo ""

PASS=0
FAIL=0

# Function to check and report
check() {
    local test_name="$1"
    local expected="$2"
    local actual="$3"
    
    if [[ "$actual" == *"$expected"* ]]; then
        echo "  ✓ $test_name"
        ((PASS++))
    else
        echo "  ✗ $test_name"
        echo "    Expected: $expected"
        echo "    Got: $actual"
        ((FAIL++))
    fi
}

# =============================================================================
# 1. Check Nginx Timeouts
# =============================================================================
echo "1. Nginx Configuration:"
NGINX_TIMEOUT=$(sudo grep "proxy_read_timeout" /etc/nginx/conf.d/streamlit.conf | head -1 | grep -oP '\d+')
check "Nginx proxy_read_timeout" "3600" "$NGINX_TIMEOUT"

NGINX_SEND=$(sudo grep "proxy_send_timeout" /etc/nginx/conf.d/streamlit.conf | head -1 | grep -oP '\d+')
check "Nginx proxy_send_timeout" "3600" "$NGINX_SEND"

echo ""

# =============================================================================
# 2. Check Systemd Memory Limits
# =============================================================================
echo "2. Systemd Memory Limits:"
for PORT in 8515 8516 8517 8518; do
    MEM_MAX=$(sudo systemctl show streamlit@$PORT.service | grep "^MemoryMax=" | cut -d= -f2)
    if [ "$MEM_MAX" = "8589934592" ] || [ "$MEM_MAX" = "8G" ]; then
        echo "  ✓ Port $PORT: MemoryMax = 8G"
        ((PASS++))
    else
        echo "  ✗ Port $PORT: MemoryMax = $MEM_MAX (expected 8G)"
        ((FAIL++))
    fi
done

echo ""

# =============================================================================
# 3. Check Systemd Timeouts
# =============================================================================
echo "3. Systemd Timeouts:"
TIMEOUT_START=$(sudo systemctl show streamlit@8516.service | grep "^TimeoutStartUSec=" | grep -oP '\d+')
TIMEOUT_START_SEC=$((TIMEOUT_START / 1000000))
check "TimeoutStartSec" "3600" "$TIMEOUT_START_SEC"

echo ""

# =============================================================================
# 4. Check config.toml Settings
# =============================================================================
echo "4. Streamlit config.toml:"
if [ -f ~/.streamlit/config.toml ]; then
    XSRF=$(grep "enableXsrfProtection" ~/.streamlit/config.toml | grep -v "^#" | grep -oP "(true|false)")
    check "XSRF Protection Disabled" "false" "$XSRF"
    
    COMPRESSION=$(grep "enableWebsocketCompression" ~/.streamlit/config.toml | grep -v "^#" | grep -oP "(true|false)")
    check "WebSocket Compression Enabled" "true" "$COMPRESSION"
    
    MAX_MSG=$(grep "maxMessageSize" ~/.streamlit/config.toml | grep -v "^#" | grep -oP '\d+')
    if [ "$MAX_MSG" -ge 200 ]; then
        echo "  ✓ maxMessageSize = $MAX_MSG (>= 200)"
        ((PASS++))
    else
        echo "  ✗ maxMessageSize = $MAX_MSG (expected >= 200)"
        ((FAIL++))
    fi
else
    echo "  ✗ config.toml not found at ~/.streamlit/config.toml"
    ((FAIL++))
fi

echo ""

# =============================================================================
# 5. Check TCP Keepalive Settings
# =============================================================================
echo "5. TCP Keepalive Settings:"
TCP_TIME=$(sysctl net.ipv4.tcp_keepalive_time | grep -oP '\d+')
check "tcp_keepalive_time" "30" "$TCP_TIME"

TCP_INTVL=$(sysctl net.ipv4.tcp_keepalive_intvl | grep -oP '\d+')
check "tcp_keepalive_intvl" "10" "$TCP_INTVL"

TCP_PROBES=$(sysctl net.ipv4.tcp_keepalive_probes | grep -oP '\d+')
check "tcp_keepalive_probes" "6" "$TCP_PROBES"

echo ""

# =============================================================================
# 6. Check Services Are Running
# =============================================================================
echo "6. Service Status:"
for PORT in 8515 8516 8517 8518; do
    STATUS=$(sudo systemctl is-active streamlit@$PORT.service 2>/dev/null)
    if [ "$STATUS" = "active" ]; then
        echo "  ✓ Port $PORT: active"
        ((PASS++))
    else
        echo "  ✗ Port $PORT: $STATUS"
        ((FAIL++))
    fi
done

echo ""

# =============================================================================
# 7. Check Memory Usage
# =============================================================================
echo "7. Current Memory Usage:"
for PORT in 8515 8516 8517 8518; do
    MEM=$(sudo systemctl status streamlit@$PORT.service --no-pager | grep "Memory:" | awk '{print $2}')
    if [ -n "$MEM" ]; then
        echo "  Port $PORT: $MEM"
    fi
done

echo ""

# =============================================================================
# Summary
# =============================================================================
echo "=========================================="
echo "  Verification Summary"
echo "=========================================="
echo "  Passed: $PASS"
echo "  Failed: $FAIL"
echo ""

if [ $FAIL -eq 0 ]; then
    echo "✓ All checks passed! Your system is properly configured."
    echo ""
    echo "You can now test Balance Sheet mapping without timeouts."
    exit 0
else
    echo "✗ Some checks failed. Please review the errors above."
    echo ""
    echo "To fix issues, run:"
    echo "  bash scripts/deploy_websocket_fix.sh"
    exit 1
fi
