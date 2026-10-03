#compdef syncpoc
# Zsh completion for syncpoc.
#
# Install:
#   mkdir -p ~/.zsh/completions
#   cp completions/syncpoc.zsh ~/.zsh/completions/_syncpoc
#   # make sure ~/.zsh/completions is in $fpath BEFORE `compinit` in ~/.zshrc
#
# If you run the binary under a different name (e.g. ./main), the simplest
# fix is giving it the expected name: `cp main syncpoc` (or build with
# `go build -o syncpoc ./cmd/syncpoc`).
_syncpoc() {
    local cur prev sub
    local -a pos
    cur="${words[CURRENT]}"
    prev="${words[CURRENT-1]}"

    # Value for --port: suggest common ports.
    if [[ "$prev" == "--port" ]]; then
        compadd 8080 4433 9000
        return
    fi

    # Collect positionals, skipping flags and their values.
    # (zsh arrays are 1-indexed: words[1] is the command itself.)
    sub=""
    pos=()
    local i w
    for (( i = 2; i < CURRENT; i++ )); do
        w="${words[i]}"
        if [[ "$w" == "--port" ]]; then
            (( i++ ))
            continue
        fi
        [[ "$w" == -* ]] && continue
        if [[ -z "$sub" ]]; then
            sub="$w"
        else
            pos+=("$w")
        fi
    done

    # No subcommand yet: `syncpoc FILE|host|join ...`
    if [[ -z "$sub" ]]; then
        if [[ "$cur" == -* ]]; then
            compadd -- --port
        else
            compadd host join
            _files
        fi
        return
    fi

    # Complete the FILE argument where the grammar expects one.
    case "$sub" in
        host)
            # host FILE [--port N]
            (( ${#pos} == 0 )) && _files
            ;;
        join)
            # join <addr> FILE [--port N]: the address is free text.
            (( ${#pos} == 1 )) && _files
            ;;
        *)
            # Bare FILE form: the file is already given.
            ;;
    esac

    # --port is a valid trailer once required positionals are present.
    if [[ "$cur" == -* ]]; then
        compadd -- --port
    elif { [[ "$sub" == host && ${#pos} -ge 1 ]]; } || \
         { [[ "$sub" == join && ${#pos} -ge 2 ]]; } || \
         { [[ "$sub" != host && "$sub" != join ]]; }; then
        compadd -- --port
    fi
}

_syncpoc "$@"
