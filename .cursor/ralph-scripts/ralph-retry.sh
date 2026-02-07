#!/bin/bash
# Ralph Wiggum: Retry Logic with Exponential Backoff
#
# Provides retry utilities for handling transient failures:
# - Exponential backoff with configurable jitter
# - Retryable error detection
# - Command execution with automatic retries
#
# Usage:
#   source scripts/ralph-retry.sh
#   with_retry 3 1 "curl https://api.example.com/data"

# =============================================================================
# CONFIGURATION (can be overridden before sourcing)
# =============================================================================

# Default retry settings
DEFAULT_MAX_RETRIES="${DEFAULT_MAX_RETRIES:-3}"
DEFAULT_BASE_DELAY="${DEFAULT_BASE_DELAY:-1}"
DEFAULT_MAX_DELAY="${DEFAULT_MAX_DELAY:-60}"
DEFAULT_USE_JITTER="${DEFAULT_USE_JITTER:-true}"

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

# Sleep for a specified number of milliseconds
# Args: milliseconds
sleep_ms() {
  local ms="$1"
  
  # Convert milliseconds to seconds (with decimal precision)
  # Use awk or bc for floating point division if available
  if command -v awk &> /dev/null; then
    local seconds
    seconds=$(awk "BEGIN {printf \"%.3f\", $ms / 1000}")
    sleep "$seconds"
  elif command -v bc &> /dev/null; then
    local seconds
    seconds=$(echo "scale=3; $ms / 1000" | bc)
    sleep "$seconds"
  else
    # Fallback: round to nearest second (minimum 1 second)
    local seconds=$(( (ms + 500) / 1000 ))
    [[ $seconds -lt 1 ]] && seconds=1
    sleep "$seconds"
  fi
}

# Calculate exponential backoff delay with optional jitter
# Args: attempt, base_delay_seconds, max_delay_seconds, use_jitter(true/false)
# Returns: delay in milliseconds (as integer)
# Formula: base_delay * 2^(attempt-1), capped at max_delay
# Jitter: adds 0-25% random to prevent thundering herd
calculate_backoff_delay() {
  local attempt="$1"
  local base_delay_seconds="$2"
  local max_delay_seconds="$3"
  local use_jitter="${4:-true}"
  
  # Convert base delay to milliseconds for precision
  local base_delay_ms=$((base_delay_seconds * 1000))
  local max_delay_ms=$((max_delay_seconds * 1000))
  
  # Calculate exponential backoff: base_delay * 2^(attempt-1)
  # For attempt=1: 2^0 = 1, so delay = base_delay
  # For attempt=2: 2^1 = 2, so delay = base_delay * 2
  # For attempt=3: 2^2 = 4, so delay = base_delay * 4
  local delay_ms=$((base_delay_ms * (1 << (attempt - 1))))
  
  # Cap at max delay
  if [[ $delay_ms -gt $max_delay_ms ]]; then
    delay_ms=$max_delay_ms
  fi
  
  # Add jitter: 0-25% random addition
  if [[ "$use_jitter" == "true" ]] || [[ "$use_jitter" == "1" ]]; then
    # Generate random number 0-250 (representing 0-25% of delay_ms)
    # Using $RANDOM which gives 0-32767, scale to 0-250
    local jitter_percent=$((RANDOM % 251))
    local jitter_ms=$((delay_ms * jitter_percent / 1000))
    delay_ms=$((delay_ms + jitter_ms))
  fi
  
  echo "$delay_ms"
}

# Check if an error message indicates a retryable error
# Args: error_message (from stderr or exit code context)
# Returns: 0 if retryable, 1 if not retryable
# Patterns: rate limit, quota, too many requests, 429, timeout, network, connection errors
is_retryable_error() {
  local error_msg="$1"
  
  # Convert to lowercase for case-insensitive matching
  local lower_msg
  lower_msg=$(echo "$error_msg" | tr '[:upper:]' '[:lower:]')
  
  # Check for retryable error patterns
  if [[ "$lower_msg" =~ (rate[[:space:]]*limit|rate[[:space:]]*limiting) ]] || \
     [[ "$lower_msg" =~ (quota[[:space:]]*exceeded|quota[[:space:]]*limit) ]] || \
     [[ "$lower_msg" =~ (too[[:space:]]*many[[:space:]]*requests) ]] || \
     [[ "$lower_msg" =~ (429|http[[:space:]]*429) ]] || \
     [[ "$lower_msg" =~ (timeout|timed[[:space:]]*out|connection[[:space:]]*timeout) ]] || \
     [[ "$lower_msg" =~ (network[[:space:]]*error|network[[:space:]]*unavailable) ]] || \
     [[ "$lower_msg" =~ (connection[[:space:]]*refused|connection[[:space:]]*reset) ]] || \
     [[ "$lower_msg" =~ (connection[[:space:]]*closed|connection[[:space:]]*failed) ]] || \
     [[ "$lower_msg" =~ (temporary[[:space:]]*failure|temporary[[:space:]]*error) ]] || \
     [[ "$lower_msg" =~ (service[[:space:]]*unavailable|503) ]] || \
     [[ "$lower_msg" =~ (bad[[:space:]]*gateway|502) ]] || \
     [[ "$lower_msg" =~ (gateway[[:space:]]*timeout|504) ]]; then
    return 0  # Retryable
  fi
  
  return 1  # Not retryable
}

# Execute a command with retry logic and exponential backoff
# Args: max_retries, base_delay_seconds, max_delay_seconds, use_jitter, command...
#   OR: max_retries, base_delay_seconds, command... (uses defaults for max_delay and jitter)
# Returns: exit code of the command (0 on success, non-zero on failure after all retries)
# Logs retry attempts to stderr
with_retry() {
  local max_retries="$1"
  local base_delay="$2"
  local max_delay="${DEFAULT_MAX_DELAY}"
  local use_jitter="${DEFAULT_USE_JITTER}"
  local command_start=3
  
  # Check if 3rd arg is numeric (max_delay) or a command
  if [[ $# -ge 4 ]] && [[ "$3" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
    max_delay="$3"
    command_start=4
    
    # Check if 4th arg is jitter flag
    if [[ $# -ge 5 ]] && [[ "$4" == "true" ]] || [[ "$4" == "false" ]] || [[ "$4" == "1" ]] || [[ "$4" == "0" ]]; then
      use_jitter="$4"
      command_start=5
    fi
  elif [[ $# -ge 4 ]] && [[ "$3" == "true" ]] || [[ "$3" == "false" ]] || [[ "$3" == "1" ]] || [[ "$3" == "0" ]]; then
    # 3rd arg is jitter flag, use default max_delay
    use_jitter="$3"
    command_start=4
  fi
  
  # Extract the command (everything from command_start onwards)
  local cmd=("${@:$command_start}")
  
  if [[ ${#cmd[@]} -eq 0 ]]; then
    echo "Error: with_retry requires a command to execute" >&2
    return 1
  fi
  
  local attempt=1
  local last_exit_code=0
  local last_error=""
  
  while [[ $attempt -le $max_retries ]]; do
    # Execute the command, capturing both stdout and stderr
    local output
    output=$("${cmd[@]}" 2>&1)
    last_exit_code=$?
    
    # If command succeeded, return success
    if [[ $last_exit_code -eq 0 ]]; then
      echo "$output"
      return 0
    fi
    
    # Command failed - capture error message
    last_error="$output"
    
    # Check if this is the last attempt
    if [[ $attempt -eq $max_retries ]]; then
      echo "Error: Command failed after $max_retries attempts" >&2
      echo "Last error: $last_error" >&2
      echo "$last_error" >&2
      return $last_exit_code
    fi
    
    # Check if error is retryable
    if ! is_retryable_error "$last_error"; then
      echo "Error: Non-retryable error detected, aborting retries" >&2
      echo "Error: $last_error" >&2
      echo "$last_error" >&2
      return $last_exit_code
    fi
    
    # Calculate backoff delay
    local delay_ms
    delay_ms=$(calculate_backoff_delay "$attempt" "$base_delay" "$max_delay" "$use_jitter")
    local delay_seconds=$((delay_ms / 1000))
    
    # Log retry attempt
    echo "⚠️  Attempt $attempt/$max_retries failed (exit code: $last_exit_code)" >&2
    echo "   Retrying in ${delay_seconds}s (with exponential backoff)..." >&2
    if [[ -n "$last_error" ]]; then
      echo "   Error: ${last_error:0:200}${last_error:200:+...}" >&2
    fi
    
    # Sleep before retry
    sleep_ms "$delay_ms"
    
    attempt=$((attempt + 1))
  done
  
  # Should never reach here, but handle it anyway
  echo "$last_error" >&2
  return $last_exit_code
}

# =============================================================================
# CONVENIENCE WRAPPERS
# =============================================================================

# Simplified retry wrapper with defaults
# Args: command...
# Uses: DEFAULT_MAX_RETRIES, DEFAULT_BASE_DELAY, DEFAULT_MAX_DELAY, DEFAULT_USE_JITTER
retry() {
  with_retry "$DEFAULT_MAX_RETRIES" "$DEFAULT_BASE_DELAY" "$DEFAULT_MAX_DELAY" "$DEFAULT_USE_JITTER" "$@"
}
