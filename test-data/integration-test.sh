#!/bin/bash

set -e

echo "=== PagoPA eCommerce CDC Service Integration Test ==="
echo "Testing CDC service integration with ecommerce-local environment"
echo "This test creates transactions via Transactions Service API and validates CDC processing"

# configuration
TRANSACTIONS_SERVICE_URL="http://localhost:8080"
CDC_SERVICE_URL="http://localhost:8099"
JUNIT_OUTPUT_FILE="integration-test-results.xml"

# test variables
TEST_TRANSACTION_ID=""
TEST_START_TIME=$(date +%s)
TEST_PASSED=0
TEST_FAILED=0

# colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # no color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_debug() {
    echo -e "${BLUE}[DEBUG]${NC} $1"
}

# wait for service health
wait_for_service() {
    local service_name=$1
    local health_url=$2
    local max_attempts=60
    local attempt=1
    
    log_info "Waiting for $service_name to be healthy..."
    
    while [ $attempt -le $max_attempts ]; do
        if curl -s -f "$health_url" > /dev/null 2>&1; then
            log_info "$service_name is healthy"
            return 0
        fi
        
        log_debug "Attempt $attempt/$max_attempts - $service_name not ready yet"
        sleep 5
        attempt=$((attempt + 1))
    done
    
    log_error "$service_name failed to become healthy within $((max_attempts * 5)) seconds"
    return 1
}

# check if CDC service container is running
check_cdc_service_running() {
    log_info "Checking if CDC service container is running..."
    
    local cdc_status=$(docker inspect pagopa-ecommerce-cdc-service --format='{{.State.Status}}' 2>/dev/null || echo "not_found")
    
    log_debug "CDC container status: $cdc_status"
    
    if [ "$cdc_status" = "running" ]; then
        log_info "CDC service container is running"
        
        # Get full logs for debugging
        local cdc_logs=$(docker logs pagopa-ecommerce-cdc-service 2>&1)
        
        log_debug "=== CDC Service Full Logs ==="
        echo "$cdc_logs"
        log_debug "=== End CDC Service Logs ==="
        
        # Check for the expected startup message
        if echo "$cdc_logs" | grep -q "Starting transaction change stream consumer"; then
            log_info "CDC service has started successfully"
            return 0
        else
            log_error "CDC service container is running but service hasn't started properly"
            log_error "Expected log pattern 'Starting transaction change stream consumer' not found"
            
            # Additional debug info
            log_debug "=== CDC Container Inspection ==="
            docker inspect pagopa-ecommerce-cdc-service --format='{{json .State}}' | jq '.' 2>/dev/null || echo "Container state inspection failed"
            
            log_debug "=== CDC Environment Variables ==="
            docker exec pagopa-ecommerce-cdc-service env | grep -E "(MONGO|CDC|SPRING)" 2>/dev/null || echo "Failed to get environment variables"
            
            return 1
        fi
    else
        log_error "CDC service container is not running (status: $cdc_status)"
        
        if [ "$cdc_status" != "not_found" ]; then
            log_debug "=== CDC Container Exit Info ==="
            docker inspect pagopa-ecommerce-cdc-service --format='{{json .State}}' | jq '.' 2>/dev/null || echo "Container state inspection failed"
            
            log_debug "=== CDC Container Logs ==="
            docker logs pagopa-ecommerce-cdc-service 2>&1 || echo "Failed to get container logs"
        fi
        
        return 1
    fi
}

# check mongo replica set
check_mongodb_replica_set() {
    log_info "Checking CDC MongoDB replica set status..."
    
    local mongo_status=$(docker exec pagopa-ecommerce-mongo mongosh --eval "rs.status().ok" --quiet 2>/dev/null || echo "0")
    
    if [ "$mongo_status" = "1" ]; then
        log_info "CDC MongoDB replica set is ready for Change Streams"
        return 0
    else
        log_error "CDC MongoDB replica set is not properly configured"
        return 1
    fi
}

# create a test transaction via Transactions Service API
create_test_transaction_via_api() {
    log_info "Creating test transaction via Transactions Service API..."
    
    local correlation_id=$(uuidgen)
    local order_id="cdc-integration-test-order-$(date +%s)"
    local cart_id="cdc-integration-test-cart-$(date +%s)"
    
    local request_body='{
        "paymentNotices": [
            {
                "rptId": "77777777777302012387654312384",
                "amount": 12000
            }
        ],
        "emailToken": "28a7c9d3-6e03-4107-8894-9cd7abb9eff7",
        "orderId": "'$order_id'",
        "idCart": "'$cart_id'"
    }'
    
    log_debug "Making API call to create transaction with correlation ID: $correlation_id"
    
    # POST /transactions
    local response=$(curl -s -w "\n%{http_code}" \
        -X POST "$TRANSACTIONS_SERVICE_URL/v2.1/transactions" \
        -H "Content-Type: application/json" \
        -H "X-Client-Id: CHECKOUT" \
        -H "x-api-key: primary-key" \
        -H "x-correlation-id: $correlation_id" \
        -d "$request_body")
    
    # extract http status code and response body
    local http_code=$(echo "$response" | tail -n1)
    local response_body=$(echo "$response" | head -n -1)
    
    if [ "$http_code" = "200" ]; then
        # extract transactionId from response
        TEST_TRANSACTION_ID=$(echo "$response_body" | jq -r '.transactionId')
        
        if [ "$TEST_TRANSACTION_ID" != "null" ] && [ -n "$TEST_TRANSACTION_ID" ]; then
            log_info "✓ Test transaction created successfully via API: $TEST_TRANSACTION_ID"
            log_debug "API Response: $response_body"
            return 0
        else
            log_error "Failed to extract transactionId from API response"
            log_error "Response: $response_body"
            return 1
        fi
    else
        log_error "Failed to create transaction via API (HTTP $http_code)"
        log_error "Response: $response_body"
        return 1
    fi
}

# wait for CDC processing
wait_for_cdc_processing() {
    log_info "Waiting for CDC service to process transaction events..."
    
    local max_attempts=30
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        # check CDC service logs for our transaction
        local cdc_logs=$(docker logs pagopa-ecommerce-cdc-service --since=60s 2>&1 | grep "$TEST_TRANSACTION_ID" || true)
        
        if [ -n "$cdc_logs" ]; then
            log_info "✓ CDC service processed transaction: $TEST_TRANSACTION_ID"
            log_debug "CDC logs found: $cdc_logs"
            return 0
        fi
        
        log_debug "Attempt $attempt/$max_attempts - CDC processing not detected yet"
        sleep 3
        attempt=$((attempt + 1))
    done
    
    log_error "CDC service did not process transaction within $((max_attempts * 3)) seconds"
    return 1
}

# validate CDC logs
validate_cdc_logs() {
    log_info "Validating CDC service logs..."
    
    local cdc_logs=$(docker logs pagopa-ecommerce-cdc-service --since=120s 2>&1)
    
    # check for essential log patterns
    local required_patterns=(
        "Starting transaction change stream consumer"
        "Connecting to MongoDB Change Stream"
        "transactionId.*$TEST_TRANSACTION_ID"
        "TRANSACTION_ACTIVATED_EVENT"
        "Successfully processed eventstore event"
    )
    
    local found_patterns=0
    
    for pattern in "${required_patterns[@]}"; do
        if echo "$cdc_logs" | grep -q "$pattern"; then
            log_info "✓ Found required pattern: $pattern"
            found_patterns=$((found_patterns + 1))
        else
            log_warn "✗ Missing pattern: $pattern"
        fi
    done
    
    # check for error patterns that should NOT be present
    local error_patterns=(
        "Connection refused"
        "Failed to connect to DB"
        "Change streams are only supported on replica sets"
        "MongoDB connection failed"
    )
    
    local error_found=false
    for error_pattern in "${error_patterns[@]}"; do
        if echo "$cdc_logs" | grep -q "$error_pattern"; then
            log_error "✗ Found error pattern: $error_pattern"
            error_found=true
        fi
    done
    
    if [ "$error_found" = true ]; then
        log_error "CDC service logs contain error patterns"
        return 1
    fi
    
    # require at least 4 out of 5 patterns for success
    local min_required=4
    if [ $found_patterns -ge $min_required ]; then
        log_info "✓ CDC log validation passed ($found_patterns/${#required_patterns[@]} patterns found)"
        return 0
    else
        log_error "CDC log validation failed ($found_patterns/${#required_patterns[@]} patterns found, minimum $min_required required)"
        return 1
    fi
}

# to validate mongodb events
validate_mongodb_events() {
    log_info "Validating CDC MongoDB eventstore collection..."
    
    local event_count=$(docker exec pagopa-ecommerce-mongo mongosh ecommerce --eval "
        db.eventstore.countDocuments({transactionId: '$TEST_TRANSACTION_ID'})
    " --quiet 2>/dev/null)
    
    if [ "$event_count" -gt 0 ]; then
        log_info "✓ Found $event_count event(s) in CDC MongoDB eventstore for transaction $TEST_TRANSACTION_ID"
        return 0
    else
        log_error "No events found in CDC MongoDB eventstore for transaction $TEST_TRANSACTION_ID"
        return 1
    fi
}

# generate JUnit XML report
generate_junit_xml() {
    local test_end_time=$(date +%s)
    local test_duration=$((test_end_time - TEST_START_TIME))
    local total_tests=$((TEST_PASSED + TEST_FAILED))
    
    log_info "Generating JUnit XML report..."
    
    cat > "$JUNIT_OUTPUT_FILE" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<testsuites name="CDC Integration Tests" tests="$total_tests" failures="$TEST_FAILED" errors="0" time="$test_duration">
  <testsuite name="CDC Integration Test Suite" tests="$total_tests" failures="$TEST_FAILED" errors="0" time="$test_duration">
    <testcase classname="CDCIntegrationTest" name="service_health_check" time="5">
EOF

    if [ $TEST_FAILED -gt 0 ]; then
        echo "      <failure message=\"One or more CDC integration test steps failed\">Integration test failed</failure>" >> "$JUNIT_OUTPUT_FILE"
    fi

    cat >> "$JUNIT_OUTPUT_FILE" << EOF
    </testcase>
    <testcase classname="CDCIntegrationTest" name="transaction_creation" time="3">
    </testcase>
    <testcase classname="CDCIntegrationTest" name="cdc_event_processing" time="10">
    </testcase>
    <testcase classname="CDCIntegrationTest" name="log_validation" time="5">
    </testcase>
  </testsuite>
</testsuites>
EOF

    log_info "JUnit XML report generated: $JUNIT_OUTPUT_FILE"
}

# run complete integration test
run_integration_test() {
    log_info "=== Starting CDC Integration Test ==="
    
    # health checks
    log_info "Step 1: Service health checks"
    if check_cdc_service_running && \
       wait_for_service "Transactions Service" "$TRANSACTIONS_SERVICE_URL/actuator/health/liveness" && \
       check_mongodb_replica_set; then
        log_info "✓ All services are healthy"
        TEST_PASSED=$((TEST_PASSED + 1))
    else
        log_error "✗ Service health check failed"
        TEST_FAILED=$((TEST_FAILED + 1))
        return 1
    fi
    
    # create test transaction
    log_info "Step 2: Transaction creation"
    if create_test_transaction_via_api; then
        log_info "✓ Transaction creation successful"
        TEST_PASSED=$((TEST_PASSED + 1))
    else
        log_error "✗ Transaction creation failed"
        TEST_FAILED=$((TEST_FAILED + 1))
        return 1
    fi
    
    # wait for CDC processing
    log_info "Step 3: CDC event processing"
    if wait_for_cdc_processing; then
        log_info "✓ CDC event processing successful"
        TEST_PASSED=$((TEST_PASSED + 1))
    else
        log_error "✗ CDC event processing failed"
        TEST_FAILED=$((TEST_FAILED + 1))
        return 1
    fi
    
    # validate CDC logs
    log_info "Step 4: CDC log validation"
    if validate_cdc_logs; then
        log_info "✓ CDC log validation successful"
        TEST_PASSED=$((TEST_PASSED + 1))
    else
        log_error "✗ CDC log validation failed"
        TEST_FAILED=$((TEST_FAILED + 1))
        return 1
    fi
    
    # validate mongo events
    log_info "Step 5: MongoDB event validation"
    if validate_mongodb_events; then
        log_info "✓ MongoDB event validation successful"
        TEST_PASSED=$((TEST_PASSED + 1))
    else
        log_error "✗ MongoDB event validation failed"
        TEST_FAILED=$((TEST_FAILED + 1))
        return 1
    fi
    
    return 0
}

# main execution
main() {
    log_info "PagoPA eCommerce CDC Service Integration Test"
    log_info "=============================================="
    
    # run the integration test
    if run_integration_test; then
        log_info "🎉 ALL TESTS PASSED! 🎉"
        log_info "✓ CDC service successfully integrated with ecommerce-local"
        log_info "✓ Transaction created via Transactions Service API"
        log_info "✓ Transaction events captured and processed correctly"
        log_info "✓ Change Stream processing working as expected"
        log_info "✓ MongoDB replica set configuration correct"
        log_info "✓ End-to-end transaction flow validation successful"
        log_info "✓ All validation checks passed"
        
        generate_junit_xml
        return 0
    else
        log_error "❌ INTEGRATION TEST FAILED ❌"
        log_error "One or more test steps failed. Check the logs above for details."
        
        # print CDC service logs for debugging
        log_error "=== CDC Service Logs (last 50 lines) ==="
        docker logs pagopa-ecommerce-cdc-service --tail=50 2>&1 || true
        
        generate_junit_xml
        return 1
    fi
}

# check if required commands are available
for cmd in curl jq uuidgen docker; do
    if ! command -v $cmd &> /dev/null; then
        log_error "Required command '$cmd' not found. Please install it."
        exit 1
    fi
done

# run the main function
main "$@"