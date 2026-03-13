#!/bin/bash
# Ralph Wiggum: Task Parser with YAML Backend
#
# Parses RALPH_TASK.md and extracts tasks, with optional YAML caching.
# Uses line numbers as task IDs for stable references.
#
# Usage:
#   source task-parser.sh
#   parse_tasks "$workspace"
#   get_next_task "$workspace"
#   mark_task_complete "$workspace" "$task_id"
#
# Cache files:
#   .ralph/tasks.yaml     - Parsed task data (YAML format)
#   .ralph/tasks.mtime    - Mtime of RALPH_TASK.md when cached

# =============================================================================
# CONFIGURATION
# =============================================================================

# Cache directory suffix
TASK_CACHE_FILE="tasks.yaml"
TASK_MTIME_FILE="tasks.mtime"

# Default group for unannotated tasks (999999 = runs last in parallel mode)
# Override with DEFAULT_GROUP=0 to make unannotated tasks run first
DEFAULT_GROUP="${DEFAULT_GROUP:-999999}"

# =============================================================================
# INTERNAL: CACHE MANAGEMENT
# =============================================================================

# Get modification time of a file (cross-platform)
_get_file_mtime() {
  local file="$1"
  if [[ ! -f "$file" ]]; then
    echo "0"
    return
  fi
  
  if [[ "$OSTYPE" == "darwin"* ]]; then
    stat -f '%m' "$file" 2>/dev/null || echo "0"
  else
    stat -c '%Y' "$file" 2>/dev/null || echo "0"
  fi
}

# Check if cache is valid (mtime matches)
_is_cache_valid() {
  local workspace="${1:-.}"
  local task_file="$workspace/RALPH_TASK.md"
  local ralph_dir="$workspace/.ralph"
  local cache_file="$ralph_dir/$TASK_CACHE_FILE"
  local mtime_file="$ralph_dir/$TASK_MTIME_FILE"
  
  # No cache files = invalid
  [[ ! -f "$cache_file" ]] && return 1
  [[ ! -f "$mtime_file" ]] && return 1
  
  # Compare mtimes
  local current_mtime=$(_get_file_mtime "$task_file")
  local cached_mtime
  cached_mtime=$(cat "$mtime_file" 2>/dev/null) || cached_mtime="0"
  
  [[ "$current_mtime" == "$cached_mtime" ]]
}

# Normalize line endings (CRLF -> LF)
_normalize_line_endings() {
  tr -d '\r'
}

# =============================================================================
# INTERNAL: YAML HELPERS
# =============================================================================

# Escape a string for YAML (basic escaping)
_yaml_escape() {
  local str="$1"
  # Replace backslashes first, then quotes
  str="${str//\\/\\\\}"
  str="${str//\"/\\\"}"
  # Wrap in quotes if contains special chars
  if [[ "$str" =~ [:\[\]\{\}\,\#\&\*\!\|\>\'\"\%\@\`] ]] || [[ "$str" =~ ^[[:space:]] ]] || [[ "$str" =~ [[:space:]]$ ]]; then
    echo "\"$str\""
  else
    echo "$str"
  fi
}

# Write task cache to YAML format
_write_cache() {
  local workspace="${1:-.}"
  local ralph_dir="$workspace/.ralph"
  local task_file="$workspace/RALPH_TASK.md"
  local cache_file="$ralph_dir/$TASK_CACHE_FILE"
  local mtime_file="$ralph_dir/$TASK_MTIME_FILE"
  
  mkdir -p "$ralph_dir"
  
  # Get current mtime
  local current_mtime=$(_get_file_mtime "$task_file")
  
  # Parse tasks and write YAML
  {
    echo "# Ralph Task Cache"
    echo "# Auto-generated from RALPH_TASK.md"
    echo "# DO NOT EDIT - regenerated on task file change"
    echo ""
    echo "source_file: RALPH_TASK.md"
    echo "source_mtime: $current_mtime"
    echo "generated_at: $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    echo ""
    echo "tasks:"
    
    # Parse task file, extract checkbox items with line numbers
    local line_num=0
    while IFS= read -r line || [[ -n "$line" ]]; do
      line_num=$((line_num + 1))
      line=$(_normalize_line_endings <<< "$line")
      
      # Match checkbox list items: "- [ ]", "* [x]", "1. [ ]", etc.
      if [[ "$line" =~ ^[[:space:]]*([-*]|[0-9]+\.)[[:space:]]+\[(x|X|\ )\][[:space:]]+(.*) ]]; then
        local marker="${BASH_REMATCH[1]}"
        local status_char="${BASH_REMATCH[2]}"
        local description="${BASH_REMATCH[3]}"
        
        # Extract parallel_group from line (first match wins)
        # Format: <!-- group: N --> where N is a number
        local parallel_group="$DEFAULT_GROUP"
        if [[ "$line" =~ \<!--[[:space:]]*group:[[:space:]]*([0-9]+)[[:space:]]*--\> ]]; then
          parallel_group="${BASH_REMATCH[1]}"
        fi
        
        # Strip ALL group comments from description for cleanliness
        description="$(sed -E 's/[[:space:]]*<!--[[:space:]]*group:[[:space:]]*[0-9]+[[:space:]]*-->[[:space:]]*//g' <<<"$description")"
        
        # Determine status
        local status="pending"
        if [[ "$status_char" == "x" ]] || [[ "$status_char" == "X" ]]; then
          status="completed"
        fi
        
        # Calculate indentation level (each 2 spaces = 1 level)
        local indent="${line%%[![:space:]]*}"
        local indent_level=$(( ${#indent} / 2 ))
        
        # Write YAML entry
        echo "  - id: \"line_$line_num\""
        echo "    line_number: $line_num"
        echo "    status: $status"
        echo "    parallel_group: $parallel_group"
        echo "    description: $(_yaml_escape "$description")"
        echo "    indent_level: $indent_level"
        echo "    raw_marker: $(_yaml_escape "$marker")"
      fi
    done < "$task_file"
    
  } > "$cache_file"
  
  # Save mtime
  echo "$current_mtime" > "$mtime_file"
}

# Read a value from YAML cache (simple key extraction)
# Usage: _read_yaml_value "tasks[0].status" < cache.yaml
_read_yaml_simple() {
  local key="$1"
  local file="$2"
  grep "^${key}:" "$file" 2>/dev/null | sed 's/^[^:]*:[[:space:]]*//' | sed 's/^"//' | sed 's/"$//'
}

# =============================================================================
# PUBLIC: TASK PARSING
# =============================================================================

# Parse tasks from RALPH_TASK.md, update cache if needed
# Returns: 0 on success, 1 on failure
parse_tasks() {
  local workspace="${1:-.}"
  local task_file="$workspace/RALPH_TASK.md"
  
  # Check task file exists
  if [[ ! -f "$task_file" ]]; then
    echo "ERROR: No RALPH_TASK.md found in $workspace" >&2
    return 1
  fi
  
  # Check if cache is still valid
  if _is_cache_valid "$workspace"; then
    return 0
  fi
  
  # Regenerate cache
  _write_cache "$workspace"
  return 0
}

# =============================================================================
# PUBLIC: TASK QUERIES
# =============================================================================

# Get all tasks as line-by-line output
# Format: id|status|description
get_all_tasks() {
  local workspace="${1:-.}"
  local ralph_dir="$workspace/.ralph"
  local cache_file="$ralph_dir/$TASK_CACHE_FILE"
  local task_file="$workspace/RALPH_TASK.md"
  
  # Ensure cache is valid
  parse_tasks "$workspace" || return 1
  
  # If cache exists, use it; otherwise parse directly
  if [[ -f "$cache_file" ]]; then
    # Parse YAML cache (simple line-by-line extraction)
    local current_id="" current_status="" current_desc=""
    while IFS= read -r line; do
      line="${line#"${line%%[![:space:]]*}"}"  # trim leading whitespace
      
      if [[ "$line" =~ ^-\ id:\ \"?([^\"]+)\"?$ ]]; then
        # New task entry - output previous if exists
        if [[ -n "$current_id" ]]; then
          echo "$current_id|$current_status|$current_desc"
        fi
        current_id="${BASH_REMATCH[1]}"
        current_status=""
        current_desc=""
      elif [[ "$line" =~ ^status:\ (.+)$ ]]; then
        current_status="${BASH_REMATCH[1]}"
      elif [[ "$line" =~ ^description:\ \"?(.*)\"?$ ]]; then
        current_desc="${BASH_REMATCH[1]}"
        # Remove trailing quote if present
        current_desc="${current_desc%\"}"
      fi
    done < "$cache_file"
    
    # Output last task
    if [[ -n "$current_id" ]]; then
      echo "$current_id|$current_status|$current_desc"
    fi
  else
    # Fallback: parse directly from markdown
    _parse_tasks_direct "$task_file"
  fi
}

# Parse tasks directly from markdown (fallback)
_parse_tasks_direct() {
  local task_file="$1"
  local line_num=0
  
  while IFS= read -r line || [[ -n "$line" ]]; do
    line_num=$((line_num + 1))
    line=$(_normalize_line_endings <<< "$line")
    
    if [[ "$line" =~ ^[[:space:]]*([-*]|[0-9]+\.)[[:space:]]+\[(x|X|\ )\][[:space:]]+(.*) ]]; then
      local status_char="${BASH_REMATCH[2]}"
      local description="${BASH_REMATCH[3]}"
      
      local status="pending"
      if [[ "$status_char" == "x" ]] || [[ "$status_char" == "X" ]]; then
        status="completed"
      fi
      
      echo "line_$line_num|$status|$description"
    fi
  done < "$task_file"
}

# Get all tasks WITH group info (extended format for parallel mode)
# Format: id|status|group|description
# Note: get_all_tasks() is kept stable for backward compatibility
get_all_tasks_with_group() {
  local workspace="${1:-.}"
  local ralph_dir="$workspace/.ralph"
  local cache_file="$ralph_dir/$TASK_CACHE_FILE"
  
  # Ensure cache is valid
  parse_tasks "$workspace" || return 1
  
  if [[ -f "$cache_file" ]]; then
    local current_id="" current_status="" current_group="" current_desc=""
    while IFS= read -r line; do
      line="${line#"${line%%[![:space:]]*}"}"  # trim leading whitespace
      
      if [[ "$line" =~ ^-\ id:\ \"?([^\"]+)\"?$ ]]; then
        # New task entry - output previous if exists
        if [[ -n "$current_id" ]]; then
          echo "$current_id|$current_status|$current_group|$current_desc"
        fi
        current_id="${BASH_REMATCH[1]}"
        current_status=""
        current_group="$DEFAULT_GROUP"
        current_desc=""
      elif [[ "$line" =~ ^status:\ (.+)$ ]]; then
        current_status="${BASH_REMATCH[1]}"
      elif [[ "$line" =~ ^parallel_group:\ (.+)$ ]]; then
        current_group="${BASH_REMATCH[1]}"
      elif [[ "$line" =~ ^description:\ \"?(.*)\"?$ ]]; then
        current_desc="${BASH_REMATCH[1]}"
        current_desc="${current_desc%\"}"
      fi
    done < "$cache_file"
    
    # Output last task
    if [[ -n "$current_id" ]]; then
      echo "$current_id|$current_status|$current_group|$current_desc"
    fi
  else
    # Fallback: parse directly (returns default group for all)
    _parse_tasks_direct_with_group "$workspace/RALPH_TASK.md"
  fi
}

# Parse tasks with group directly from markdown (fallback)
_parse_tasks_direct_with_group() {
  local task_file="$1"
  local line_num=0
  
  while IFS= read -r line || [[ -n "$line" ]]; do
    line_num=$((line_num + 1))
    line=$(_normalize_line_endings <<< "$line")
    
    if [[ "$line" =~ ^[[:space:]]*([-*]|[0-9]+\.)[[:space:]]+\[(x|X|\ )\][[:space:]]+(.*) ]]; then
      local status_char="${BASH_REMATCH[2]}"
      local description="${BASH_REMATCH[3]}"
      
      # Extract group
      local group="$DEFAULT_GROUP"
      if [[ "$line" =~ \<!--[[:space:]]*group:[[:space:]]*([0-9]+)[[:space:]]*--\> ]]; then
        group="${BASH_REMATCH[1]}"
      fi
      
      # Strip group comment from description
      description="$(sed -E 's/[[:space:]]*<!--[[:space:]]*group:[[:space:]]*[0-9]+[[:space:]]*-->[[:space:]]*//g' <<<"$description")"
      
      local status="pending"
      if [[ "$status_char" == "x" ]] || [[ "$status_char" == "X" ]]; then
        status="completed"
      fi
      
      echo "line_$line_num|$status|$group|$description"
    fi
  done < "$task_file"
}

# Get all pending tasks for a specific group
# Format: id|task_status|group|description
get_tasks_by_group() {
  local workspace="${1:-.}"
  local target_group="$2"
  
  get_all_tasks_with_group "$workspace" | while IFS='|' read -r task_id task_status task_group task_desc; do
    if [[ "$task_status" == "pending" ]] && [[ "$task_group" == "$target_group" ]]; then
      echo "$task_id|$task_status|$task_group|$task_desc"
    fi
  done
}

# Get sorted list of unique groups that have pending tasks
# Returns one group number per line, sorted numerically
get_pending_groups() {
  local workspace="${1:-.}"
  
  get_all_tasks_with_group "$workspace" | while IFS='|' read -r task_id task_status task_group task_desc; do
    if [[ "$task_status" == "pending" ]]; then
      echo "$task_group"
    fi
  done | sort -n | uniq
}

# Get the next incomplete task
# Returns: id|status|description or empty if all complete
get_next_task() {
  local workspace="${1:-.}"
  
  get_all_tasks "$workspace" | while IFS='|' read -r id status desc; do
    if [[ "$status" == "pending" ]]; then
      echo "$id|$status|$desc"
      break
    fi
  done
}

# Get task by ID
# Returns: id|status|description or empty if not found
get_task_by_id() {
  local workspace="${1:-.}"
  local target_id="$2"
  
  get_all_tasks "$workspace" | while IFS='|' read -r id status desc; do
    if [[ "$id" == "$target_id" ]]; then
      echo "$id|$status|$desc"
      break
    fi
  done
}

# Count remaining (pending) tasks
count_remaining() {
  local workspace="${1:-.}"
  local count=0
  
  while IFS='|' read -r id status desc; do
    if [[ "$status" == "pending" ]]; then
      count=$((count + 1))
    fi
  done < <(get_all_tasks "$workspace")
  
  echo "$count"
}

# Count completed tasks
count_completed() {
  local workspace="${1:-.}"
  local count=0
  
  while IFS='|' read -r id status desc; do
    if [[ "$status" == "completed" ]]; then
      count=$((count + 1))
    fi
  done < <(get_all_tasks "$workspace")
  
  echo "$count"
}

# Count total tasks
count_total() {
  local workspace="${1:-.}"
  get_all_tasks "$workspace" | wc -l | tr -d ' '
}

# =============================================================================
# PUBLIC: TASK MODIFICATION
# =============================================================================

# Mark a task as complete by ID (line_N format)
# Modifies RALPH_TASK.md directly
mark_task_complete() {
  local workspace="${1:-.}"
  local task_id="$2"
  local task_file="$workspace/RALPH_TASK.md"
  
  # Extract line number from ID
  if [[ ! "$task_id" =~ ^line_([0-9]+)$ ]]; then
    echo "ERROR: Invalid task ID format: $task_id (expected line_N)" >&2
    return 1
  fi
  local line_num="${BASH_REMATCH[1]}"
  
  # Read the file
  if [[ ! -f "$task_file" ]]; then
    echo "ERROR: Task file not found: $task_file" >&2
    return 1
  fi
  
  # Use sed to replace [ ] with [x] on the specific line
  if [[ "$OSTYPE" == "darwin"* ]]; then
    sed -i '' "${line_num}s/\[ \]/[x]/" "$task_file"
  else
    sed -i "${line_num}s/\[ \]/[x]/" "$task_file"
  fi
  
  # Invalidate cache by removing mtime file
  rm -f "$workspace/.ralph/$TASK_MTIME_FILE"
  
  return 0
}

# Mark a task as incomplete by ID
mark_task_incomplete() {
  local workspace="${1:-.}"
  local task_id="$2"
  local task_file="$workspace/RALPH_TASK.md"
  
  # Extract line number from ID
  if [[ ! "$task_id" =~ ^line_([0-9]+)$ ]]; then
    echo "ERROR: Invalid task ID format: $task_id (expected line_N)" >&2
    return 1
  fi
  local line_num="${BASH_REMATCH[1]}"
  
  if [[ ! -f "$task_file" ]]; then
    echo "ERROR: Task file not found: $task_file" >&2
    return 1
  fi
  
  # Use sed to replace [x] or [X] with [ ] on the specific line
  if [[ "$OSTYPE" == "darwin"* ]]; then
    sed -i '' "${line_num}s/\[[xX]\]/[ ]/" "$task_file"
  else
    sed -i "${line_num}s/\[[xX]\]/[ ]/" "$task_file"
  fi
  
  # Invalidate cache
  rm -f "$workspace/.ralph/$TASK_MTIME_FILE"
  
  return 0
}

# =============================================================================
# PUBLIC: YAML IMPORT/EXPORT
# =============================================================================

# Check if YAML task file exists
has_yaml_tasks() {
  local workspace="${1:-.}"
  [[ -f "$workspace/.ralph/$TASK_CACHE_FILE" ]]
}

# Export current task state to YAML (for external tooling)
export_tasks_yaml() {
  local workspace="${1:-.}"
  
  # Force cache refresh
  rm -f "$workspace/.ralph/$TASK_MTIME_FILE"
  parse_tasks "$workspace" || return 1
  
  # Output the cache file
  cat "$workspace/.ralph/$TASK_CACHE_FILE"
}

# Import tasks from external YAML (advanced usage)
# This creates/updates RALPH_TASK.md from YAML
import_tasks_yaml() {
  local workspace="${1:-.}"
  local yaml_file="$2"
  local task_file="$workspace/RALPH_TASK.md"
  
  if [[ ! -f "$yaml_file" ]]; then
    echo "ERROR: YAML file not found: $yaml_file" >&2
    return 1
  fi
  
  # For now, just copy to cache location
  # Full YAML->Markdown conversion would require more logic
  mkdir -p "$workspace/.ralph"
  cp "$yaml_file" "$workspace/.ralph/$TASK_CACHE_FILE"
  
  echo "Imported YAML to cache. Note: RALPH_TASK.md not modified." >&2
  echo "Use mark_task_complete/incomplete to sync changes." >&2
}

# =============================================================================
# PUBLIC: UTILITY FUNCTIONS
# =============================================================================

# Get progress as "done:total" format (compatible with existing count_criteria)
get_progress() {
  local workspace="${1:-.}"
  local done=$(count_completed "$workspace")
  local total=$(count_total "$workspace")
  echo "$done:$total"
}

# Check if all tasks are complete
is_all_complete() {
  local workspace="${1:-.}"
  local remaining=$(count_remaining "$workspace")
  [[ "$remaining" -eq 0 ]]
}

# Pretty print task status
print_task_status() {
  local workspace="${1:-.}"
  local done=$(count_completed "$workspace")
  local total=$(count_total "$workspace")
  local remaining=$((total - done))
  
  echo "Task Progress: $done / $total complete ($remaining remaining)"
  echo ""
  echo "Tasks:"
  
  get_all_tasks "$workspace" | while IFS='|' read -r id status desc; do
    local checkbox="[ ]"
    if [[ "$status" == "completed" ]]; then
      checkbox="[x]"
    fi
    echo "  $checkbox $desc (${id})"
  done
}

# =============================================================================
# CLI INTERFACE (when run directly)
# =============================================================================

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  # Script is being run directly, not sourced
  
  usage() {
    echo "Usage: $0 <command> [workspace] [args...]"
    echo ""
    echo "Commands:"
    echo "  parse           Parse tasks and update cache"
    echo "  list            List all tasks"
    echo "  next            Get next pending task"
    echo "  complete <id>   Mark task as complete"
    echo "  incomplete <id> Mark task as incomplete"
    echo "  count           Show task counts"
    echo "  status          Show pretty task status"
    echo "  export          Export tasks as YAML"
    echo ""
    echo "Examples:"
    echo "  $0 list ."
    echo "  $0 next /path/to/project"
    echo "  $0 complete . line_15"
  }
  
  cmd="${1:-}"
  workspace="${2:-.}"
  
  case "$cmd" in
    parse)
      parse_tasks "$workspace"
      echo "Cache updated."
      ;;
    list)
      get_all_tasks "$workspace"
      ;;
    next)
      result=$(get_next_task "$workspace")
      if [[ -n "$result" ]]; then
        echo "$result"
      else
        echo "No pending tasks."
      fi
      ;;
    complete)
      task_id="${3:-}"
      if [[ -z "$task_id" ]]; then
        echo "ERROR: Task ID required" >&2
        exit 1
      fi
      mark_task_complete "$workspace" "$task_id"
      echo "Marked $task_id as complete."
      ;;
    incomplete)
      task_id="${3:-}"
      if [[ -z "$task_id" ]]; then
        echo "ERROR: Task ID required" >&2
        exit 1
      fi
      mark_task_incomplete "$workspace" "$task_id"
      echo "Marked $task_id as incomplete."
      ;;
    count)
      echo "Completed: $(count_completed "$workspace")"
      echo "Remaining: $(count_remaining "$workspace")"
      echo "Total:     $(count_total "$workspace")"
      ;;
    status)
      print_task_status "$workspace"
      ;;
    export)
      export_tasks_yaml "$workspace"
      ;;
    -h|--help|help)
      usage
      ;;
    *)
      usage >&2
      exit 1
      ;;
  esac
fi
