#!/usr/bin/env bash
source /opt/cgr/demos/lib.sh

banner "SSH Demo 0: Verify connectivity"

section "Two-container setup"
narrate "Control node: this container — has CommandGraph installed"
narrate "Target node:  'target' — runs sshd, accepts key-based auth"
narrate "Connection:   ssh deploy@target (key pre-shared)"
echo ""

section "Step 1: Test SSH connectivity"
wait_for_target || exit 1

section "Step 2: Show what's on each side"
narrate "Control node (this container):"
run_cmd "hostname && whoami && which cgr"

narrate "Target node (via SSH):"
run_on_target "hostname && whoami && id"

section "Step 3: Verify sudo on target"
narrate "The deploy user has passwordless sudo on the target."
narrate "CommandGraph uses this via 'as root' in step headers."
run_on_target "sudo whoami"

section "Step 4: Validate an SSH graph"
narrate "This graph targets 'ssh deploy@target' — the engine will"
narrate "execute every command on the remote host over SSH."
run_cmd "cgr validate $EXAMPLES/ssh-basic.cgr --repo $REPO"

success "SSH connectivity verified. Ready to run remote graphs."
