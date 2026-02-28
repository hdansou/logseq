# Bash completion for the Logseq CLI (src/main/logseq/cli)
#
# Installation:
#   Option A — per-user (recommended):
#     mkdir -p ~/.local/share/bash-completion/completions
#     cp completions/logseq.bash ~/.local/share/bash-completion/completions/logseq
#
#   Option B — source directly from ~/.bashrc:
#     source /path/to/logseq/completions/logseq.bash
#
#   Option C — system-wide:
#     cp completions/logseq.bash /etc/bash_completion.d/logseq

# ---------------------------------------------------------------------------
# Dynamic helpers
# ---------------------------------------------------------------------------

_logseq_json_names_bash() {
  python3 -c "
import sys, json
field = sys.argv[1]
try:
    data = json.load(sys.stdin)
    if isinstance(data, list):
        for item in data:
            v = item.get(field)
            if isinstance(v, str) and v:
                print(v)
except Exception:
    pass
" "$1" 2>/dev/null
}

_logseq_current_repo_bash() {
  local i
  for (( i = 1; i < ${#COMP_WORDS[@]}; i++ )); do
    if [[ "${COMP_WORDS[i]}" == '--repo' && -n "${COMP_WORDS[i+1]}" ]]; then
      printf '%s' "${COMP_WORDS[i+1]}"
      return
    fi
  done
}

_logseq_graphs_bash() {
  logseq graph list --output json 2>/dev/null | _logseq_json_names_bash name
}

_logseq_pages_bash() {
  logseq list page --repo "$1" --output json 2>/dev/null | _logseq_json_names_bash title
}

_logseq_queries_bash() {
  logseq query list --repo "$1" --output json 2>/dev/null | _logseq_json_names_bash name
}

# Append completions from a newline-delimited list that may contain spaces.
_logseq_compadd_lines() {
  local cur="$1" source_fn="$2"; shift 2
  while IFS= read -r item; do
    [[ "$item" == "$cur"* ]] && COMPREPLY+=( "$item" )
  done < <("$source_fn" "$@")
}

# ---------------------------------------------------------------------------
# Options per command
# ---------------------------------------------------------------------------

_logseq_global_opts='--help -h --version --config --repo --data-dir --timeout-ms --output --verbose'

_logseq_opts_for() {
  local cmd="$1" subcmd="$2"
  local opts="$_logseq_global_opts"

  case "$cmd" in
    list)
      case "$subcmd" in
        page)
          opts+=' --created-after --updated-after --limit --offset --fields
                  --sort --order --journal-only --include-hidden --include-journal --expand'
          ;;
        tag)
          opts+=' --limit --offset --fields --sort --order
                  --include-built-in --with-extends --with-properties --expand'
          ;;
        property)
          opts+=' --limit --offset --fields --sort --order
                  --include-built-in --with-type --with-classes --expand'
          ;;
      esac
      ;;
    add)
      case "$subcmd" in
        block)
          opts+=' --content --blocks --blocks-file --tags --properties
                  --target-page-name --target-uuid --target-id --pos --status'
          ;;
        page)  opts+=' --page --tags --properties' ;;
        tag)   opts+=' --name' ;;
      esac
      ;;
    remove) opts+=' --page --uuid --id' ;;
    update) opts+=' --id --uuid --target-id --target-uuid --target-page
                    --pos --update-tags --remove-tags --update-properties --remove-properties' ;;
    query)
      [[ "$subcmd" != 'list' ]] && opts+=' --name --query --inputs'
      ;;
    show)   opts+=' --page --uuid --id --level --linked-references' ;;
    graph)
      case "$subcmd" in
        export) opts+=' --type --output' ;;
        import) opts+=' --type --input' ;;
      esac
      ;;
    doctor) opts+=' --dev-script' ;;
  esac

  printf '%s' "$opts"
}

# ---------------------------------------------------------------------------
# Detect command / subcommand already typed (skip option values)
# ---------------------------------------------------------------------------

# Options that consume the next word as their value.
_logseq_is_value_opt() {
  case "$1" in
    --config|--repo|--data-dir|--timeout-ms|--output| \
    --sort|--order|--pos|--status|--type|--input| \
    --created-after|--updated-after|--limit|--offset|--fields| \
    --blocks-file|--content|--blocks|--tags|--properties| \
    --target-page-name|--target-uuid|--target-id| \
    --target-page|--id|--uuid|--page|--name| \
    --query|--inputs|--update-tags|--remove-tags| \
    --update-properties|--remove-properties|--level)
      return 0 ;;
    *) return 1 ;;
  esac
}

_logseq_cmd_and_subcmd() {
  # Sets caller's variables: __cmd and __subcmd
  local i skip=0
  __cmd='' __subcmd=''
  for (( i = 1; i < COMP_CWORD; i++ )); do
    local w="${COMP_WORDS[i]}"
    if (( skip )); then skip=0; continue; fi
    if [[ "$w" == -* ]]; then
      _logseq_is_value_opt "$w" && skip=1
      continue
    fi
    if [[ -z "$__cmd" ]]; then
      __cmd="$w"
    elif [[ -z "$__subcmd" ]]; then
      __subcmd="$w"
    fi
  done
}

# ---------------------------------------------------------------------------
# Main completion function
# ---------------------------------------------------------------------------

_logseq() {
  local cur prev
  cur="${COMP_WORDS[COMP_CWORD]}"
  prev="${COMP_WORDS[COMP_CWORD-1]}"
  COMPREPLY=()

  local __cmd __subcmd
  _logseq_cmd_and_subcmd

  # --- Option value completion -------------------------------------------
  case "$prev" in
    --repo)
      _logseq_compadd_lines "$cur" _logseq_graphs_bash
      return ;;

    --config)
      COMPREPLY=( $(compgen -f -- "$cur") )
      return ;;

    --data-dir)
      COMPREPLY=( $(compgen -d -- "$cur") )
      return ;;

    --output)
      # graph export --output is a file path; everywhere else it's a format
      if [[ "$__cmd" == 'graph' && "$__subcmd" == 'export' ]]; then
        COMPREPLY=( $(compgen -f -- "$cur") )
      else
        COMPREPLY=( $(compgen -W 'human json edn' -- "$cur") )
      fi
      return ;;

    --sort)
      if [[ "$__cmd" == 'list' && "$__subcmd" == 'page' ]]; then
        COMPREPLY=( $(compgen -W 'title created-at updated-at' -- "$cur") )
      else
        COMPREPLY=( $(compgen -W 'name title' -- "$cur") )
      fi
      return ;;

    --order)
      COMPREPLY=( $(compgen -W 'asc desc' -- "$cur") )
      return ;;

    --pos)
      COMPREPLY=( $(compgen -W 'first-child last-child sibling' -- "$cur") )
      return ;;

    --status)
      COMPREPLY=( $(compgen -W \
        'todo doing done now later wait waiting backlog canceled cancelled in-review in-progress' \
        -- "$cur") )
      return ;;

    --type)
      COMPREPLY=( $(compgen -W 'edn sqlite' -- "$cur") )
      return ;;

    --blocks-file|--input)
      COMPREPLY=( $(compgen -f -- "$cur") )
      return ;;

    --page|--target-page-name|--target-page)
      local repo
      repo="$(_logseq_current_repo_bash)"
      [[ -n "$repo" ]] && _logseq_compadd_lines "$cur" _logseq_pages_bash "$repo"
      return ;;

    --name)
      # Only complete query names for the 'query' command; add tag --name is free text
      if [[ "$__cmd" == 'query' ]]; then
        local repo
        repo="$(_logseq_current_repo_bash)"
        [[ -n "$repo" ]] && _logseq_compadd_lines "$cur" _logseq_queries_bash "$repo"
      fi
      return ;;
  esac

  # --- Flag / positional completion --------------------------------------

  # Completing an option flag
  if [[ "$cur" == -* ]]; then
    # shellcheck disable=SC2046
    COMPREPLY=( $(compgen -W "$(_logseq_opts_for "$__cmd" "$__subcmd")" -- "$cur") )
    return
  fi

  # Completing top-level command
  if [[ -z "$__cmd" ]]; then
    COMPREPLY=( $(compgen -W \
      'list add remove update query show graph server doctor' \
      -- "$cur") )
    return
  fi

  # Completing subcommand
  if [[ -z "$__subcmd" ]]; then
    case "$__cmd" in
      list)   COMPREPLY=( $(compgen -W 'page tag property'                                       -- "$cur") ) ;;
      add)    COMPREPLY=( $(compgen -W 'block page tag'                                          -- "$cur") ) ;;
      query)  COMPREPLY=( $(compgen -W 'list'                                                    -- "$cur") ) ;;
      graph)  COMPREPLY=( $(compgen -W 'list create switch remove validate info export import'   -- "$cur") ) ;;
      server) COMPREPLY=( $(compgen -W 'list status start stop restart'                          -- "$cur") ) ;;
    esac
    return
  fi
}

complete -F _logseq logseq
