# Bash completion for syncpoc.
#
# Install (pick one):
#   source /path/to/completions/syncpoc.bash         # this shell only
#   echo 'source /path/to/completions/syncpoc.bash' >> ~/.bashrc
#
# This completes the `syncpoc` command name. If you run the binary under a
# different name (e.g. ./main), register that name too:
#   complete -F _syncpoc ./main
_syncpoc() {
    local cur prev
    COMPREPLY=()
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"

    # Value for --port: suggest common ports.
    if [[ "$prev" == "--port" ]]; then
        COMPREPLY=( $(compgen -W "8080 4433 9000" -- "$cur") )
        return 0
    fi

    # Collect positionals, skipping flags and their values.
    local sub="" pos i w
    pos=()
    for (( i = 1; i < COMP_CWORD; i++ )); do
        w="${COMP_WORDS[i]}"
        if [[ "$w" == "--port" ]]; then
            (( i++ ))
            continue
        fi
        if [[ "$w" == -* ]]; then
            continue
        fi
        if [[ -z "$sub" ]]; then
            sub="$w"
        else
            pos+=("$w")
        fi
    done

    # No subcommand yet: `syncpoc FILE|host|join ...`
    if [[ -z "$sub" ]]; then
        if [[ "$cur" == -* ]]; then
            COMPREPLY=( $(compgen -W "--port" -- "$cur") )
        else
            COMPREPLY=( $(compgen -W "host join" -- "$cur") )
            COMPREPLY+=( $(compgen -f -- "$cur") )
        fi
        return 0
    fi

    # Complete the FILE argument where the grammar expects one.
    case "$sub" in
        host)
            # host FILE [--port N]
            if (( ${#pos[@]} == 0 )); then
                COMPREPLY=( $(compgen -f -- "$cur") )
            fi
            ;;
        join)
            # join <addr> FILE [--port N]: the address is free text.
            if (( ${#pos[@]} == 1 )); then
                COMPREPLY=( $(compgen -f -- "$cur") )
            fi
            ;;
        *)
            # Bare FILE form: the file is already given.
            ;;
    esac

    # --port is a valid trailer once required positionals are present.
    if [[ "$cur" == -* ]]; then
        COMPREPLY+=( $(compgen -W "--port" -- "$cur") )
    elif { [[ "$sub" == host && ${#pos[@]} -ge 1 ]]; } || \
         { [[ "$sub" == join && ${#pos[@]} -ge 2 ]]; } || \
         { [[ "$sub" != host && "$sub" != join ]]; }; then
        COMPREPLY+=( $(compgen -W "--port" -- "$cur") )
    fi
}
complete -F _syncpoc syncpoc
