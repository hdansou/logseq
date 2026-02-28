#compdef logseq

# Zsh completion for the Logseq CLI (src/main/logseq/cli)
#
# Installation:
#   1. Copy this file to a directory on your $fpath, renaming it to _logseq:
#        mkdir -p ~/.zsh/completions
#        cp completions/_logseq.zsh ~/.zsh/completions/_logseq
#   2. Add that directory to fpath in ~/.zshrc BEFORE the compinit call:
#        fpath=(~/.zsh/completions $fpath)
#        autoload -Uz compinit && compinit
#   3. Start a new shell session (or run `compinit` again).
#
# Caching: dynamic completions (graphs, pages, queries) are cached via zsh's
# completion cache. To invalidate, run: rm -f ~/.zcompcache/logseq_*

# ---------------------------------------------------------------------------
# Global option specs — included in every command
# ---------------------------------------------------------------------------

_logseq_global_opts=(
  '(-h --help)'{-h,--help}'[show help]'
  '--version[show version]'
  '--config=[path to cli.edn]:config file:_files'
  '--repo=[graph name]:graph:_logseq_graphs'
  '--data-dir=[path to db-worker data dir]:data dir:_files -/'
  '--timeout-ms=[request timeout in ms]:timeout (ms):'
  '--output=[output format]:format:(human json edn)'
  '--verbose[enable verbose debug logging to stderr]'
)

# ---------------------------------------------------------------------------
# Dynamic completion helpers
# ---------------------------------------------------------------------------

# Extract string values of a named field from a JSON array on stdin.
# Usage: <cmd> | _logseq_json_names FIELD
_logseq_json_names() {
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

# Complete graph names, cached.
_logseq_graphs() {
  local cache_id='logseq_graphs'
  local -a graphs
  if _cache_invalid "$cache_id" || ! _retrieve_cache "$cache_id"; then
    graphs=( ${(f)"$(logseq graph list --output json 2>/dev/null | _logseq_json_names name)"} )
    _store_cache "$cache_id" graphs
  fi
  compadd -a graphs
}

# Resolve the --repo value from the current command line words.
_logseq_current_repo() {
  local i
  for (( i = 1; i < ${#words[@]}; i++ )); do
    if [[ "${words[i]}" == '--repo' && -n "${words[i+1]}" ]]; then
      print -r -- "${words[i+1]}"
      return
    fi
  done
}

# Complete page titles for the current repo, cached per repo.
_logseq_pages() {
  local repo
  repo=$(_logseq_current_repo)
  local cache_id="logseq_pages_${repo:-__default__}"
  local -a pages
  if _cache_invalid "$cache_id" || ! _retrieve_cache "$cache_id"; then
    if [[ -n "$repo" ]]; then
      pages=( ${(f)"$(logseq list page --repo "$repo" --output json 2>/dev/null | _logseq_json_names title)"} )
    fi
    _store_cache "$cache_id" pages
  fi
  compadd -a pages
}

# Complete query names for the current repo, cached per repo.
_logseq_queries() {
  local repo
  repo=$(_logseq_current_repo)
  local cache_id="logseq_queries_${repo:-__default__}"
  local -a queries
  if _cache_invalid "$cache_id" || ! _retrieve_cache "$cache_id"; then
    if [[ -n "$repo" ]]; then
      queries=( ${(f)"$(logseq query list --repo "$repo" --output json 2>/dev/null | _logseq_json_names name)"} )
    fi
    _store_cache "$cache_id" queries
  fi
  compadd -a queries
}

# ---------------------------------------------------------------------------
# list page / list tag / list property
# ---------------------------------------------------------------------------

_logseq_list_page() {
  _arguments -s \
    "${_logseq_global_opts[@]}" \
    '--created-after=[filter by created-at (ISO8601)]:date:' \
    '--updated-after=[filter by updated-at (ISO8601)]:date:' \
    '--limit=[limit results]:count:' \
    '--offset=[offset results]:count:' \
    '--fields=[select output fields (comma-separated)]:fields:' \
    '--sort=[sort field]:field:(title created-at updated-at)' \
    '--order=[sort order]:order:(asc desc)' \
    '--journal-only[only journal pages]' \
    '--include-hidden[include hidden pages]' \
    '--include-journal[include journal pages]' \
    '--expand[include expanded metadata]'
}

_logseq_list_tag() {
  _arguments -s \
    "${_logseq_global_opts[@]}" \
    '--limit=[limit results]:count:' \
    '--offset=[offset results]:count:' \
    '--fields=[select output fields (comma-separated)]:fields:' \
    '--sort=[sort field]:field:(name title)' \
    '--order=[sort order]:order:(asc desc)' \
    '--include-built-in[include built-in tags]' \
    '--with-extends[include tag extends]' \
    '--with-properties[include tag properties]' \
    '--expand[include expanded metadata]'
}

_logseq_list_property() {
  _arguments -s \
    "${_logseq_global_opts[@]}" \
    '--limit=[limit results]:count:' \
    '--offset=[offset results]:count:' \
    '--fields=[select output fields (comma-separated)]:fields:' \
    '--sort=[sort field]:field:(name title)' \
    '--order=[sort order]:order:(asc desc)' \
    '--include-built-in[include built-in properties]' \
    '--with-type[include property type]' \
    '--with-classes[include property classes]' \
    '--expand[include expanded metadata]'
}

# ---------------------------------------------------------------------------
# add block / add page / add tag
# ---------------------------------------------------------------------------

_logseq_add_block() {
  _arguments -s \
    "${_logseq_global_opts[@]}" \
    '--content=[block content]:content:' \
    '--blocks=[EDN vector of blocks]:edn:' \
    '--blocks-file=[EDN file of blocks]:file:_files' \
    '--tags=[EDN vector of tags]:edn:' \
    '--properties=[EDN map of built-in properties]:edn:' \
    '--target-page-name=[target page name]:page:_logseq_pages' \
    '--target-uuid=[target block UUID]:uuid:' \
    '--target-id=[target block db/id]:id:' \
    '--pos=[insert position]:position:(first-child last-child sibling)' \
    '--status=[task status]:status:(todo doing done now later wait waiting backlog canceled cancelled in-review in-progress)'
}

_logseq_add_page() {
  _arguments -s \
    "${_logseq_global_opts[@]}" \
    '--page=[page name]:name:' \
    '--tags=[EDN vector of tags]:edn:' \
    '--properties=[EDN map of built-in properties]:edn:'
}

_logseq_add_tag() {
  _arguments -s \
    "${_logseq_global_opts[@]}" \
    '--name=[tag name]:name:'
}

# ---------------------------------------------------------------------------
# remove / update / show
# ---------------------------------------------------------------------------

_logseq_remove() {
  _arguments -s \
    "${_logseq_global_opts[@]}" \
    '--page=[page name]:page:_logseq_pages' \
    '--uuid=[block UUID]:uuid:' \
    '--id=[block db/id or EDN vector of ids]:id:'
}

_logseq_update() {
  _arguments -s \
    "${_logseq_global_opts[@]}" \
    '--id=[source block db/id]:id:' \
    '--uuid=[source block UUID]:uuid:' \
    '--target-id=[target block db/id]:id:' \
    '--target-uuid=[target block UUID]:uuid:' \
    '--target-page=[target page name]:page:_logseq_pages' \
    '--pos=[insert position]:position:(first-child last-child sibling)' \
    '--update-tags=[tags to add/update (EDN vector)]:edn:' \
    '--remove-tags=[tags to remove (EDN vector)]:edn:' \
    '--update-properties=[properties to add/update (EDN map)]:edn:' \
    '--remove-properties=[properties to remove (EDN vector)]:edn:'
}

_logseq_show() {
  _arguments -s \
    "${_logseq_global_opts[@]}" \
    '--page=[page name]:page:_logseq_pages' \
    '--uuid=[block UUID]:uuid:' \
    '--id=[block db/id or EDN vector of ids]:id:' \
    '--level=[limit tree depth (default 10)]:depth:' \
    '--linked-references[include linked references]'
}

# ---------------------------------------------------------------------------
# query / query list
#
# 'query' is both a standalone command (--name / --query / --inputs) and a
# group with a single 'list' subcommand.  Both are handled here.
# ---------------------------------------------------------------------------

_logseq_query() {
  local curcontext="$curcontext" state line
  typeset -A opt_args

  _arguments -C -s \
    "${_logseq_global_opts[@]}" \
    '--name=[query name from cli.edn or built-ins]:query:_logseq_queries' \
    '--query=[ad-hoc Datascript query (EDN)]:edn:' \
    '--inputs=[EDN vector of query inputs]:edn:' \
    '1:subcommand:->subcmd' \
    '*::args:->args'

  case $state in
    subcmd)
      local -a subcmds
      subcmds=('list:list available queries')
      _describe 'subcommand' subcmds
      ;;
    args)
      if [[ $line[1] == 'list' ]]; then
        _arguments -s "${_logseq_global_opts[@]}"
      fi
      ;;
  esac
}

# ---------------------------------------------------------------------------
# graph subcommands
# ---------------------------------------------------------------------------

_logseq_graph() {
  local curcontext="$curcontext" state line
  typeset -A opt_args

  _arguments -C -s \
    "${_logseq_global_opts[@]}" \
    '1:subcommand:->subcmd' \
    '*::args:->args'

  case $state in
    subcmd)
      local -a subcmds
      subcmds=(
        'list:list graphs'
        'create:create graph'
        'switch:switch current graph'
        'remove:remove graph'
        'validate:validate graph'
        'info:graph metadata'
        'export:export graph'
        'import:import graph'
      )
      _describe 'subcommand' subcmds
      ;;
    args)
      case $line[1] in
        list|create|switch|remove|validate|info)
          _arguments -s "${_logseq_global_opts[@]}"
          ;;
        export)
          _arguments -s \
            "${_logseq_global_opts[@]}" \
            '--type=[export type]:type:(edn sqlite)' \
            '--output=[output file path]:file:_files'
          ;;
        import)
          _arguments -s \
            "${_logseq_global_opts[@]}" \
            '--type=[import type]:type:(edn sqlite)' \
            '--input=[input file path]:file:_files'
          ;;
      esac
      ;;
  esac
}

# ---------------------------------------------------------------------------
# server subcommands
# ---------------------------------------------------------------------------

_logseq_server() {
  local curcontext="$curcontext" state line
  typeset -A opt_args

  _arguments -C -s \
    "${_logseq_global_opts[@]}" \
    '1:subcommand:->subcmd' \
    '*::args:->args'

  case $state in
    subcmd)
      local -a subcmds
      subcmds=(
        'list:list db-worker-node servers'
        'status:show server status for a graph'
        'start:start db-worker-node for a graph'
        'stop:stop db-worker-node for a graph'
        'restart:restart db-worker-node for a graph'
      )
      _describe 'subcommand' subcmds
      ;;
    args)
      # All server subcommands share only global options
      _arguments -s "${_logseq_global_opts[@]}"
      ;;
  esac
}

# ---------------------------------------------------------------------------
# list group dispatcher
# ---------------------------------------------------------------------------

_logseq_list() {
  local curcontext="$curcontext" state line
  typeset -A opt_args

  _arguments -C -s \
    "${_logseq_global_opts[@]}" \
    '1:subcommand:->subcmd' \
    '*::args:->args'

  case $state in
    subcmd)
      local -a subcmds
      subcmds=(
        'page:list pages'
        'tag:list tags'
        'property:list properties'
      )
      _describe 'subcommand' subcmds
      ;;
    args)
      case $line[1] in
        page)     _logseq_list_page ;;
        tag)      _logseq_list_tag ;;
        property) _logseq_list_property ;;
      esac
      ;;
  esac
}

# ---------------------------------------------------------------------------
# add group dispatcher
# ---------------------------------------------------------------------------

_logseq_add() {
  local curcontext="$curcontext" state line
  typeset -A opt_args

  _arguments -C -s \
    "${_logseq_global_opts[@]}" \
    '1:subcommand:->subcmd' \
    '*::args:->args'

  case $state in
    subcmd)
      local -a subcmds
      subcmds=(
        'block:add blocks'
        'page:create page'
        'tag:create tag'
      )
      _describe 'subcommand' subcmds
      ;;
    args)
      case $line[1] in
        block) _logseq_add_block ;;
        page)  _logseq_add_page ;;
        tag)   _logseq_add_tag ;;
      esac
      ;;
  esac
}

# ---------------------------------------------------------------------------
# doctor
# ---------------------------------------------------------------------------

_logseq_doctor() {
  _arguments -s \
    "${_logseq_global_opts[@]}" \
    '--dev-script[check static/db-worker-node.js instead of bundled dist runtime]'
}

# ---------------------------------------------------------------------------
# Top-level dispatcher
# ---------------------------------------------------------------------------

_logseq() {
  local curcontext="$curcontext" state line
  typeset -A opt_args

  _arguments -C -s \
    "${_logseq_global_opts[@]}" \
    '1:command:->cmds' \
    '*::args:->args'

  case $state in
    cmds)
      local -a cmds
      cmds=(
        'list:list pages, tags, or properties'
        'add:add blocks, pages, or tags'
        'remove:remove blocks or pages'
        'update:update block'
        'query:run a Datascript query'
        'show:show tree'
        'graph:graph management (list, create, switch, ...)'
        'server:server management (list, status, start, ...)'
        'doctor:run runtime diagnostics'
      )
      _describe 'command' cmds
      ;;
    args)
      case $line[1] in
        list)   _logseq_list ;;
        add)    _logseq_add ;;
        remove) _logseq_remove ;;
        update) _logseq_update ;;
        query)  _logseq_query ;;
        show)   _logseq_show ;;
        graph)  _logseq_graph ;;
        server) _logseq_server ;;
        doctor) _logseq_doctor ;;
      esac
      ;;
  esac
}

_logseq "$@"
