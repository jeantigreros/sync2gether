# Fish completion for syncpoc (basic: subcommands, --port, files).
#
# Install:
#   cp completions/syncpoc.fish ~/.config/fish/completions/syncpoc.fish
#
# If you run the binary under a different name (e.g. ./main), copy it under
# the expected name instead: `cp main syncpoc`.
# Subcommands (only when none given yet).
complete -c syncpoc -n __fish_use_subcommand -f -a host -d 'play and wait for a peer'
complete -c syncpoc -n __fish_use_subcommand -f -a join -d 'join a host'
# Bare FILE form: complete files when no subcommand yet.
complete -c syncpoc -n __fish_use_subcommand -F
# --port flag with suggested values.
complete -c syncpoc -l port -d 'tailcat TCP port (default 8080)' -x -a '8080 4433 9000'
